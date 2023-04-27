// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//3.1 - 3.7 ✔

#include "db/version_set.h"

#include <algorithm>
#include <cstdio>
#include <unordered_map>

#include "db/run_manager.h"
#include "db/filename.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/table_cache.h"
#include "trees/vanilla_b_plus_tree.h"
#include "leveldb/env.h"
#include "leveldb/table_builder.h"
#include "table/merger.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"
#include "util/logging.h"

namespace leveldb {

//关于DB中文件的一些配置和统计
static size_t TargetFileSize(const Options* options) {
  return options->max_file_size;
}

// Maximum bytes of overlaps in grandparent (i.e., level+2) before we
// stop building a single file in a level->level+1 compaction.
static int64_t MaxGrandParentOverlapBytes(const Options* options) {
  return 10 * TargetFileSize(options);
}

// Maximum number of bytes in all compacted files.  We avoid expanding
// the lower level file set of a compaction if it would make the
// total compaction cover more than this many bytes.
static int64_t ExpandedCompactionByteSizeLimit(const Options* options) {
  return 25 * TargetFileSize(options);
}

static double MaxBytesForLevel(const Options* options, int level) {
  // Note: the result for level zero is not really used since we set
  // the level-0 compaction threshold based on number of files.

  // Result for both level-0 and level-1
  double result = 10. * 1048576.0;
  while (level > 1) {
    result *= 10;
    level--;
  }
  return result;
}

static uint64_t MaxFileSizeForLevel(const Options* options, int level) {
  // We could vary per level to reduce number of files?
  return TargetFileSize(options);
}

static int64_t TotalFileSize(const std::vector<FileMetaData*>& files) {
  int64_t sum = 0;
  for (size_t i = 0; i < files.size(); i++) {
    sum += files[i]->file_size;
  }
  return sum;
}

Version::~Version() {
  //移除一个Version
  //对这个Version的引用为0时才能释放
  assert(refs_ == 0);

  // Remove from linked list
  prev_->next_ = next_;
  next_->prev_ = prev_;

  // Drop references to files
  //对每个级别
  for (int level = 0; level < config::kNumLevels; level++){
      for(size_t i = 0; i < runs_[level].size(); i++){
        SortedRun* r = runs_[level][i];
        r->ref_--;
        if(r->ref_ <= 0){
          delete r;
        }
      }
  }
  /*for (int level = 0; level < config::kNumLevels; level++) {
    //对某个级别的所有文件
    //files_二维数组[级别][该级别的第i个文件的meta]
    for (size_t i = 0; i < files_[level].size(); i++) {
      FileMetaData* f = files_[level][i];
      assert(f->refs > 0);
      f->refs--;//由于Version被释放，该文件的引用数-1
      if (f->refs <= 0) {
        //说明这个文件已经没有被使用，可以删除
        delete f;
      }
    }
  }*/
}

//files中的文件都是完全排序的（无重叠）
//返回最大key大于key的最小file
//如 1，3，5，7 key=4 则返回文件3（largestkey=5）
int FindFile(const InternalKeyComparator& icmp,
             const std::vector<FileMetaData*>& files, const Slice& key) {
  uint32_t left = 0;
  uint32_t right = files.size();
  while (left < right) {//二分查找
    uint32_t mid = (left + right) / 2;
    const FileMetaData* f = files[mid];
    if (icmp.InternalKeyComparator::Compare(f->largest.Encode(), key) < 0) {
      // Key at "mid.largest" is < "target".  Therefore all
      // files at or before "mid" are uninteresting.
      left = mid + 1;
    } else {
      // Key at "mid.largest" is >= "target".  Therefore all files
      // after "mid" are uninteresting.
      right = mid;
    }
  }
  return right;
}

//user_key是否在f的后面
//即user_key>f->largest.user_key
static bool AfterFile(const Comparator* ucmp, const Slice* user_key,
                      const FileMetaData* f) {
  // null user_key occurs before all keys and is therefore never after *f
  return (user_key != nullptr &&
          ucmp->Compare(*user_key, f->largest.user_key()) > 0);
}

//user_key是否在f的前面
static bool BeforeFile(const Comparator* ucmp, const Slice* user_key,
                       const FileMetaData* f) {
  // null user_key occurs after all keys and is therefore never before *f
  return (user_key != nullptr &&
          ucmp->Compare(*user_key, f->smallest.user_key()) < 0);
}


/*bool SomeFileOverlapsRange(const InternalKeyComparator& icmp,
                           bool disjoint_sorted_files,
                           const std::vector<SortedRun*>& runs,
                           const Slice* smallest_user_key,
                           const Slice* largest_user_key) {*/
  //const Comparator* ucmp = icmp.user_comparator();
  //disjoint：不相交
  /*if (!disjoint_sorted_files) {//file有重叠的key_range（如L0层）
    // Need to check against all files
    //逐个检查
    for (size_t i = 0; i < files.size(); i++) {
      const FileMetaData* f = files[i];
      //smallest key在f之后
      //或者largest key在f之前
      //说明f不包含[smallest,largest]
      if (AfterFile(ucmp, smallest_user_key, f) ||
          BeforeFile(ucmp, largest_user_key, f)) {
        // No overlap
      } else {//f和[smallest,largest]有重叠
        return true;  // Overlap
      }
    }
    return false;
  }*/

  // Binary search over file list
  //file都不相互重叠的情况
  //bool flag;
  /*for(size_t i = 0; i < runs.size(); i++){
    std::vector<FileMetaData*> files = *(runs[i]->GetContainFile());
    uint32_t index = 0;
    if (smallest_user_key != nullptr) {
      // Find the earliest possible internal key for smallest_user_key
      InternalKey small_key(*smallest_user_key, kMaxSequenceNumber,
                            kValueTypeForSeek);
      //files中的文件都是完全排序的（无重叠）
      //返回最大key大于key的最小file
      //如 1，3，5，7 key=4 则返回文件3（largestkey=5）
      index = FindFile(icmp, files, small_key.Encode());
    }
    if (index >= files.size()) {
    // beginning of range is after all files, so no overlap.
      flag = false;
    }else{
      flag = !BeforeFile(ucmp, largest_user_key, files[index]);
    }

    if(flag) {
      return true;
    }
  
  }
  return false;
  /*uint32_t index = 0;
  if (smallest_user_key != nullptr) {
    // Find the earliest possible internal key for smallest_user_key
    InternalKey small_key(*smallest_user_key, kMaxSequenceNumber,
                          kValueTypeForSeek);
    //files中的文件都是完全排序的（无重叠）
    //返回最大key大于key的最小file
    //如 1，3，5，7 key=4 则返回文件3（largestkey=5）
    index = FindFile(icmp, files, small_key.Encode());
  }*/
  //说明smallest key大于所有的文件，不重叠
  /*if (index >= files.size()) {
    // beginning of range is after all files, so no overlap.
    return false;
  }*/
  //[index],smallest,[index+1]
  //largest肯定不能在files[index]之前
  //return !BeforeFile(ucmp, largest_user_key, files[index]);
//}*/

// An internal iterator.  For a given version/level pair, yields
// information about the files in the level.  For a given entry, key()
// is the largest key that occurs in the file, and value() is an
// 16-byte value containing the file number and file size, both
// encoded using EncodeFixed64.
//Version上的迭代器
//迭代器指向的内容是flist中的FileMetaData
//key()：迭代器所指向meta中保存的largest key
//value()：迭代器所指向meta中保存的file number+size
class Version::LevelFileNumIterator : public Iterator {
 public:
  LevelFileNumIterator(const InternalKeyComparator& icmp,
                       const std::vector<FileMetaData*>* flist)
      : icmp_(icmp), flist_(flist), index_(flist->size()) {  // Marks as invalid
  }
  bool Valid() const override { return index_ < flist_->size(); }
  void Seek(const Slice& target) override {
    index_ = FindFile(icmp_, *flist_, target);
  }
  void SeekToFirst() override { index_ = 0; }
  void SeekToLast() override {
    index_ = flist_->empty() ? 0 : flist_->size() - 1;
  }
  void Next() override {
    assert(Valid());
    index_++;
  }
  void Prev() override {
    assert(Valid());
    if (index_ == 0) {
      index_ = flist_->size();  // Marks as invalid
    } else {
      index_--;
    }
  }
  Slice key() const override {
    assert(Valid());
    return (*flist_)[index_]->largest.Encode();
  }
  Slice value() const override {
    assert(Valid());
    EncodeFixed64(value_buf_, (*flist_)[index_]->number);
    EncodeFixed64(value_buf_ + 8, (*flist_)[index_]->file_size);
    return Slice(value_buf_, sizeof(value_buf_));
  }
  Status status() const override { return Status::OK(); }

 private:
  const InternalKeyComparator icmp_;
  const std::vector<FileMetaData*>* const flist_;
  uint32_t index_;

  // Backing store for value().  Holds the file number and size.
  mutable char value_buf_[16];//在value()函数中用到
};

//建立file（table）上的迭代器，
//（其实具体是构造在table的index block上）
static Iterator* GetFileIterator(void* arg, const ReadOptions& options,
                                 const Slice& file_value) {
  TableCache* cache = reinterpret_cast<TableCache*>(arg);
  if (file_value.size() != 16) {
    return NewErrorIterator(
        Status::Corruption("FileReader invoked with unexpected value"));
  } else {
    return cache->NewIterator(options, DecodeFixed64(file_value.data()),
                              DecodeFixed64(file_value.data() + 8));
  }
}

//创建级联的迭代器
Iterator* Version::NewConcatenatingIterator(const ReadOptions& options,
                                            const std::vector<FileMetaData*>* flist) const {
  return NewTwoLevelIterator(
    //new LevelFileNumIterator指向的是第level层的meta list
    //GetFileIterator(参数：block function)用来构造index block的迭代器
    //第一级：file list 每个条目是一个meta
    //第二级：对每个meta对应的文件，指向一个index block，每个条目是一个data block句柄
      new LevelFileNumIterator(vset_->icmp_, flist), &GetFileIterator,
      vset_->table_cache_, options);
}

//构建一个整体的迭代器组
void Version::AddIterators(const ReadOptions& options,
                           std::vector<Iterator*>* iters, std::unordered_map<uint64_t, int>* index_map) {
  // Merge all level zero files together since they may overlap
  //对于L0层的所有文件，每个文件上创建一个迭代器
  //迭代的是index block上的内容，每个条目对于一个data block
  int index = iters->size();
  for(size_t i = 0; i < config::kNumLevels; i++){//对每层
    for(size_t j = 0; j < runs_[i].size(); j++){//对第i层的第j个run
      std::vector<FileMetaData*>* files = runs_[i][j]->GetContainFile();
      std::vector<uint64_t>* L0 = runs_[i][j]->GetRunToL0();
      for(int k = 0; k < L0->size(); k++){
        //std::cout<<L0->at(k)<<" "<<index<<std::endl;
        index_map->insert(std::make_pair(L0->at(k), index));
      }
      if(i == 0){
        assert(files->size() == 1);
        //std::cout<<"L0 conatain:"<<files->at(0)->number<<" index:"<<index<<std::endl;
        iters->push_back(vset_->table_cache_->NewIterator(options, files->at(0)->number, files->at(0)->file_size));
        //std::cout<<"itersize:"<<iters->size()<<" "<<index<<std::endl;
        index++;
        /*for(size_t fno = 0; fno < files->size(); fno++){//对每个run的每个file
          FileMetaData* file = files->at(fno);
          iters->push_back(vset_->table_cache_->NewIterator(options, file->number, file->file_size));
        }*/
      }else{
        if(!files->empty()){
          iters->push_back(NewConcatenatingIterator(options, files));
          index++;
        }
      }
    }
  }
  /*for (size_t i = 0; i < files_[0].size(); i++) {
    iters->push_back(vset_->table_cache_->NewIterator(
        options, files_[0][i]->number, files_[0][i]->file_size));
  }*/

  // For levels > 0, we can use a concatenating iterator that sequentially
  // walks through the non-overlapping files in the level, opening them
  // lazily.
  //对于非L0层的文件，每级构建一个级联的迭代器（两级迭代器）
  /*for (int level = 1; level < config::kNumLevels; level++) {
    if (!files_[level].empty()) {
      iters->push_back(NewConcatenatingIterator(options, level));
    }
  }*/
}

// Callback from TableCache::Get()
namespace {
enum SaverState {
  kNotFound,
  kFound,
  kDeleted,
  kCorrupt,
};

struct Saver {
  SaverState state;
  const Comparator* ucmp;
  Slice user_key;//用户端传入
  std::string* value;//传回给用户端
};
}  // namespace
static void SaveValue(void* arg, const Slice& ikey, const Slice& v) {
  //
  Saver* s = reinterpret_cast<Saver*>(arg);
  //ParsedInternalKey包含的是InternalKey的内部信息
  //user_key
  //sequence
  //type
  ParsedInternalKey parsed_key;
  //把整体的InternalKey的各部分解析出来记录在ParsedInternalKey中
  if (!ParseInternalKey(ikey, &parsed_key)) {
    s->state = kCorrupt;
  } else {
    //说明ikey就是用户要找的key
    if (s->ucmp->Compare(parsed_key.user_key, s->user_key) == 0) {
      s->state = (parsed_key.type == kTypeValue) ? kFound : kDeleted;
      //把值记录在Saver中，等待传回
      if (s->state == kFound) {
        s->value->assign(v.data(), v.size());
      }
    }
  }
}

//a的编号比b大则返回true
static bool NewestFirst(FileMetaData* a, FileMetaData* b) {
  return a->number > b->number;
}


Status Version::RebuildTree(VanillaBPlusTree<std::string, uint64_t>* btree){
  for(int i = config::kNumLevels - 1; i >= 0; i--){
    for(int j = 0; j < runs_[i].size(); j++){
      const SortedRun* run = runs_[i][j];
      std::vector<uint64_t>* run_to_L0 = runs_[i][j]->GetRunToL0();
      std::vector<FileMetaData*>* contain_files = runs_[i][j]->GetContainFile();
      //只保留一个run 到 L0的映射
      uint64_t L0 = run_to_L0->at(0);
      run_to_L0->clear();
      run_to_L0->push_back(L0);
      //std::cout<<"the L0:"<<L0<<std::endl;
      Iterator* iter = NewConcatenatingIterator(ReadOptions(), contain_files);
      for(iter->SeekToLast(); iter->Valid(); iter->Prev()){
        ParsedInternalKey ikey;
        Slice k = iter->key();
        if (!ParseInternalKey(k, &ikey)) {
          Status status = Status::Corruption("corrupted internal key in DBIter");
          return status;
        }
        switch (ikey.type) {
          case kTypeDeletion:
            // Arrange to skip all upcoming entries for this key since
            // they are hidden by this deletion.
            //是否要改成0？
            btree->insert(ikey.user_key.ToString(), L0);
            break;
          case kTypeValue:
            btree->insert(ikey.user_key.ToString(), L0);
            break;
        }        
      }
    }
  }
}

void Version::PrintMap(VanillaBPlusTree<std::string, uint64_t>* btree){
  //std::unordered_map<uint64_t, SortedRun*> L0_file_to_run_; 
  /*for(const auto& L0 : L0_file_to_run_){
    std::cout<<"map contain L0:"<<L0.first<<std::endl;
  }
  BTree<std::string, uint64_t>::Iterator* btree_iter = btree->NewTreeIterator();
  for(btree_iter->SeekToFirst(); btree_iter->Valid(); btree_iter->Next()){
    std::cout<<btree_iter->Key()<<" "<<btree_iter->Value()<<std::endl;
  }*/
}

//对每个包含use key的文件调用func函数
void Version::ForEachOverlapping(SortedRun* search_run, Slice user_key, Slice internal_key, void* arg,
                                 bool (*func)(void*, int, FileMetaData*)) {
  //const Comparator* ucmp = vset_->icmp_.user_comparator();
  // Search level-0 in order from newest to oldest.
  //std::vector<FileMetaData*> tmp;
  //tmp.reserve(files->size());
  //对于L0层的每个文件
  /*for (uint32_t i = 0; i < files_[0].size(); i++) {
    //取得每个文件的meta信息
    FileMetaData* f = files_[0][i];
    //如果user_key在这个文件范围内，则加入tmp
    if (ucmp->Compare(user_key, f->smallest.user_key()) >= 0 &&
        ucmp->Compare(user_key, f->largest.user_key()) <= 0) {
      tmp.push_back(f);
    }
  }*/

  /*if (!tmp.empty()) {
    //以file number排序
    std::sort(tmp.begin(), tmp.end(), NewestFirst);
    for (uint32_t i = 0; i < tmp.size(); i++) {
      //如果返回false的话，返回（找到）
      if (!(*func)(arg, 0, tmp[i])) {
        return;
      }
    }
  }*/

  const Comparator* ucmp = vset_->icmp_.user_comparator();
  std::vector<FileMetaData*>* files = search_run->GetContainFile();
  uint32_t index = FindFile(vset_->icmp_, *files, internal_key);
  assert(index < files->size());
  FileMetaData* f = files->at(index);
  assert(ucmp->Compare(user_key, f->smallest.user_key()) >= 0);
  if (!(*func)(arg, search_run->GetLevel(), f)) {
    return;
  }

  // Search other levels.
  //对于非L0层
  /*for (int level = 1; level < config::kNumLevels; level++) {
    size_t num_files = files_[level].size();
    if (num_files == 0) continue;

    // Binary search to find earliest index whose largest key >= internal_key.
    //找到key可能在的file
    uint32_t index = FindFile(vset_->icmp_, files_[level], internal_key);
    if (index < num_files) {//找到
      FileMetaData* f = files_[level][index];
      //落在两个文件之间的空隙：
      //[index-1的largestkey] userkey [index的smallest key]
      if (ucmp->Compare(user_key, f->smallest.user_key()) < 0) {
        // All of "f" is past any data for user_key
      } else {
        if (!(*func)(arg, level, f)) {
          return;
        }
      }
    }
  }*/
}

//为k寻找value
Status Version::Get(const ReadOptions& options, const LookupKey& k,
                    std::string* value, GetStats* stats, uint64_t L0_id) {
  stats->seek_file = nullptr;//filemeta
  stats->seek_file_level = -1;

  struct State {
    Saver saver;
    GetStats* stats;
    const ReadOptions* options;
    Slice ikey;
    FileMetaData* last_file_read;
    int last_file_read_level;

    VersionSet* vset;
    Status s;
    bool found;

    //对f执行的match判断
    static bool Match(void* arg, int level, FileMetaData* f) {
      State* state = reinterpret_cast<State*>(arg);
      if (state->stats->seek_file == nullptr &&
          state->last_file_read != nullptr) {
        // We have had more than one seek for this read.  Charge the 1st file.
        //seek记录上次的
        state->stats->seek_file = state->last_file_read;
        state->stats->seek_file_level = state->last_file_read_level;
      }

      //last记录当前的f（本次
      state->last_file_read = f;
      state->last_file_read_level = level;

      state->s = state->vset->table_cache_->Get(*state->options, f->number,
                                                f->file_size, state->ikey,
                                                &state->saver, SaveValue);
      if (!state->s.ok()) {
        state->found = true;
        return false;
      }
      switch (state->saver.state) {
        case kNotFound:
          return true;  // Keep searching in other files
        case kFound:
          state->found = true;
          return false;
        case kDeleted:
          return false;
        case kCorrupt:
          state->s =
              Status::Corruption("corrupted key for ", state->saver.user_key);
          state->found = true;
          return false;
      }

      // Not reached. Added to avoid false compilation warnings of
      // "control reaches end of non-void function".
      return false;
    }
  };

  State state;
  state.found = false;
  state.stats = stats;
  state.last_file_read = nullptr;
  state.last_file_read_level = -1;

  state.options = &options;
  state.ikey = k.internal_key();
  state.vset = vset_;

  state.saver.state = kNotFound;
  state.saver.ucmp = vset_->icmp_.user_comparator();
  state.saver.user_key = k.user_key();
  state.saver.value = value;

  //对每个文件待查文件都调用Match函数，直到match到所查key，就停止查找过程
  if(L0_id != 0){
    if(L0_file_to_run_.find(L0_id) == L0_file_to_run_.end()){
      L0_id = 0;
    }else{
      SortedRun* search_run = GetMapRun(L0_id);
      //修改这个函数：
      //state.found=true;
      ForEachOverlapping(search_run, state.saver.user_key, state.ikey, &state, &State::Match);          
    }  
  }

  //SortedRun* search_run = GetMapRun(L0_id);
  //修改这个函数：
  //ForEachOverlapping(search_run, state.saver.user_key, state.ikey, &state, &State::Match);

  return state.found ? state.s : Status::NotFound(Slice());
}

SortedRun* Version::GetMapRun(uint64_t id){
  std::unordered_map<uint64_t, SortedRun*>::iterator iter = L0_file_to_run_.find(id);
  return iter->second;
}

//？
bool Version::UpdateStats(const GetStats& stats) {
  FileMetaData* f = stats.seek_file;
  if (f != nullptr) {
    f->allowed_seeks--;
    if (f->allowed_seeks <= 0 && file_to_compact_ == nullptr) {
      file_to_compact_ = f;
      file_to_compact_level_ = stats.seek_file_level;
      return true;
    }
  }
  return false;
}

//？
bool Version::RecordReadSample(Slice internal_key) {
  /*ParsedInternalKey ikey;
  if (!ParseInternalKey(internal_key, &ikey)) {
    return false;
  }

  struct State {
    GetStats stats;  // Holds first matching file
    int matches;

    static bool Match(void* arg, int level, FileMetaData* f) {
      State* state = reinterpret_cast<State*>(arg);
      state->matches++;
      if (state->matches == 1) {
        // Remember first match.
        state->stats.seek_file = f;
        state->stats.seek_file_level = level;
      }
      // We can stop iterating once we have a second match.
      return state->matches < 2;
    }
  };

  State state;
  state.matches = 0;
  ForEachOverlapping(ikey.user_key, internal_key, &state, &State::Match);

  // Must have at least two matches since we want to merge across
  // files. But what if we have a single file that contains many
  // overwrites and deletions?  Should we have another mechanism for
  // finding such files?
  if (state.matches >= 2) {
    // 1MB cost is about 1 seek (see comment in Builder::Apply).
    return UpdateStats(state.stats);
  }*/
  return false;
}

//增加对Version的引用
void Version::Ref() { ++refs_; }

//减少对Version的引用
void Version::Unref() {
  assert(this != &vset_->dummy_versions_);//链表头
  //dummy：表头不是一个真正的version？
  assert(refs_ >= 1);
  --refs_;
  if (refs_ == 0) {
    delete this;
  }
}


/*bool Version::OverlapInLevel(int level, const Slice* smallest_user_key,
                             const Slice* largest_user_key) {
  //对level中的所有文件，判断其中是否有与[smallest,largest]重叠的文件                            
  return SomeFileOverlapsRange(vset_->icmp_, false, runs_[level],
                               smallest_user_key, largest_user_key);
}*/

//要把memtable flush到哪一层？
/*int Version::PickLevelForMemTableOutput(const Slice& smallest_user_key,
                                        const Slice& largest_user_key) {
  int level = 0;
  //level0没有与[smallest,largest]重叠的文件
  if (!OverlapInLevel(0, &smallest_user_key, &largest_user_key)) {
    // Push to next level if there is no overlap in next level,
    // and the #bytes overlapping in the level after that are limited.
    //构造一个最小的Internalkey
    InternalKey start(smallest_user_key, kMaxSequenceNumber, kValueTypeForSeek);
    //构造一个最大的internalkey
    InternalKey limit(largest_user_key, 0, static_cast<ValueType>(0));
    std::vector<FileMetaData*> overlaps;
    //博客：https://bean-li.github.io/leveldb-compaction/
    //如果和level+2层重叠太多的话，假如放到Level+1，
    //则从Level+1->Level+2涉及的文件会很多
    while (level < config::kMaxMemCompactLevel) {
      if (OverlapInLevel(level + 1, &smallest_user_key, &largest_user_key)) {
        break;
      }
      if (level + 2 < config::kNumLevels) {
        // Check that file does not overlap too many grandparent bytes.
        GetOverlappingInputs(level + 2, &start, &limit, &overlaps);
        const int64_t sum = TotalFileSize(overlaps);
        if (sum > MaxGrandParentOverlapBytes(vset_->options_)) {
          break;
        }
      }
      level++;
    }
  }
  return level;
}*/

// Store in "*inputs" all files in "level" that overlap [begin,end]
//简单修改：去掉if(level == 0)
void Version::GetOverlappingInputs(int level, const InternalKey* begin,
                                   const InternalKey* end,
                                   std::vector<FileMetaData*>* inputs) {
  assert(level >= 0);
  assert(level < config::kNumLevels);
  inputs->clear();
  Slice user_begin, user_end;
  if (begin != nullptr) {
    user_begin = begin->user_key();
  }
  if (end != nullptr) {
    user_end = end->user_key();
  }
  const Comparator* user_cmp = vset_->icmp_.user_comparator();
  //对Level层的所有文件
  for (size_t i = 0; i < files_[level].size();) {
    FileMetaData* f = files_[level][i++];
    const Slice file_start = f->smallest.user_key();
    const Slice file_limit = f->largest.user_key();
    //range在这个文件之后
    if (begin != nullptr && user_cmp->Compare(file_limit, user_begin) < 0) {
      // "f" is completely before specified range; skip it
      //这个range在这个文件之前
    } else if (end != nullptr && user_cmp->Compare(file_start, user_end) > 0) {
      // "f" is completely after specified range; skip it
      //重叠
    } else {
      inputs->push_back(f);//放入inputs
      //if (level == 0) {
        //L0可能会拓展range
        //把L0中以[begin,end]开始扩展的所有file都加入input
        // Level-0 files may overlap each other.  So check if the newly
        // added file has expanded the range.  If so, restart search.
        if (begin != nullptr && user_cmp->Compare(file_start, user_begin) < 0) {
          user_begin = file_start;
          inputs->clear();
          i = 0;
        } else if (end != nullptr &&
                   user_cmp->Compare(file_limit, user_end) > 0) {
          user_end = file_limit;
          inputs->clear();
          i = 0;
        }
      //}
    }
  }
}

std::string Version::DebugString() const {
  std::string r;
  for (int level = 0; level < config::kNumLevels; level++) {
    // E.g.,
    //   --- level 1 ---
    //   17:123['a' .. 'd']
    //   20:43['e' .. 'g']
    r.append("--- level ");
    AppendNumberTo(&r, level);
    r.append(" ---\n");
    const std::vector<FileMetaData*>& files = files_[level];
    for (size_t i = 0; i < files.size(); i++) {
      r.push_back(' ');
      AppendNumberTo(&r, files[i]->number);
      r.push_back(':');
      AppendNumberTo(&r, files[i]->file_size);
      r.append("[");
      r.append(files[i]->smallest.DebugString());
      r.append(" .. ");
      r.append(files[i]->largest.DebugString());
      r.append("]\n");
    }
  }
  return r;
}

// A helper class so we can efficiently apply a whole sequence
// of edits to a particular state without creating intermediate
// Versions that contain full copies of the intermediate state.
//帮助直接引用一系列的edits，而不用生成中间状态的Version包含所有中间state
class VersionSet::Builder {
 private:
  // Helper to sort by v->files_[file_number].smallest
  /*struct BySmallestKey {
    const InternalKeyComparator* internal_comparator;

    bool operator()(FileMetaData* f1, FileMetaData* f2) const {
      int r = internal_comparator->Compare(f1->smallest, f2->smallest);
      if (r != 0) {
        return (r < 0);//true说明f1的smallkey<f2的smallkey
      } else {
        // Break ties by file number
        return (f1->number < f2->number);//number小的file在前
      }
    }
  };*/

  //typedef std::set<FileMetaData*, BySmallestKey> FileSet;
  typedef std::set<SortedRun*> RunSet;
  struct LevelState {
    //LevelState():added_run(nullptr){};
    //std::set<uint64_t> deleted_files;
    std::set<uint64_t> deleted_runs;
    //FileSet* added_files;
    RunSet* added_run;
  };

  VersionSet* vset_;
  Version* base_;
  LevelState levels_[config::kNumLevels];
  std::unordered_map<uint64_t, SortedRun*> L0_file_to_run_tmp_;

 public:
  // Initialize a builder with the files from *base and other info from *vset
  Builder(VersionSet* vset, Version* base) : vset_(vset), base_(base) {
    base_->Ref();//对Version的一个引用
    //BySmallestKey cmp;
    //cmp.internal_comparator = &vset_->icmp_;
    /*for (int level = 0; level < config::kNumLevels; level++) {
      //typedef std::set<FileMetaData*, BySmallestKey> FileSet;
      levels_[level].added_files = new FileSet(cmp);
    }*/
    for (int level = 0; level < config::kNumLevels; level++){
      levels_[level].added_run = new RunSet();
    }
  }

  ~Builder() {
    for (int level = 0; level < config::kNumLevels; level++) {
      //const FileSet* added = levels_[level].added_files;
      const RunSet* added = levels_[level].added_run;
      //std::vector<FileMetaData*> to_unref;
      std::vector<SortedRun*> to_unref;
      to_unref.reserve(added->size());
      /*for (FileSet::const_iterator it = added->begin(); it != added->end();
           ++it) {
        to_unref.push_back(*it);
      }*/
      for (RunSet::const_iterator it = added->begin(); it != added->end();
           ++it) {
        to_unref.push_back(*it);
      }      
      delete added;
      for (uint32_t i = 0; i < to_unref.size(); i++) {
        //FileMetaData* f = to_unref[i];
        SortedRun* r = to_unref[i];
        //f->refs--;
        r->ref_--;
        /*if (f->refs <= 0) {
          delete f;
        }*/
        if(r->ref_ <= 0){
          delete r;
        }
      }
    }
    base_->Unref();
  }

  // Apply all of the edits in *edit to the current state.
  //将edit中的所有编辑应用于当前状态。
  void Apply(const VersionEdit* edit) {
    //一次edit只有一个新增run
    if(!edit->snapshot_runs_.empty()){
      for(int i = 0; i < edit->snapshot_runs_.size(); i++){
        uint64_t output_level = edit->snapshot_runs_[i].GetLevel();
        SortedRun* r = new SortedRun(edit->snapshot_runs_[i]);
        r->ref_ = 1;
        levels_[output_level].added_run->insert(r);
        std::vector<uint64_t>* Run_To_L0_File = r->GetRunToL0();
        std::vector<uint64_t>::iterator iter = Run_To_L0_File->begin();
        for(; iter != Run_To_L0_File->end(); iter++){
          L0_file_to_run_tmp_.insert(std::make_pair(*iter, r));
        }
      }
      return ;
    }
    const int input_level = edit->GetInputLevel();
    const int output_level = edit->GetOutputLevel();
    if(output_level < 0){
      return ;
    }
    SortedRun* r = new SortedRun(edit->new_run_);//ref初始化为0
    r->ref_ = 1;//version对run的引用
    //对新增run，更新contains_file_

    levels_[output_level].added_run->insert(r);

    //需要删除的run：把映射到该run的L0映射改到新run：写在L0_file_to_run_temp_;
    //对于新run，加入映射到该run的L0
    if(output_level > 0){
      for(const auto& deleted_run_set_kvp : edit->deleted_map_){
        levels_[input_level].deleted_runs.insert(deleted_run_set_kvp.first);
        const std::vector<uint64_t>& Run_To_L0_File = deleted_run_set_kvp.second;
        //std::vector<uint64_t>::iterator iter = Run_To_L0_File.begin();
        for(const auto& iter : Run_To_L0_File){
          std::unordered_map<uint64_t, SortedRun*>::iterator L0_iter = L0_file_to_run_tmp_.find(iter);
          if(L0_iter != L0_file_to_run_tmp_.end()){
            L0_iter->second = r;
          }else{
            L0_file_to_run_tmp_.insert(std::make_pair(iter, r));
          }
          r->InsertL0File(iter);
        }
      }
    }else{
      //对于L0层的run，其包含的file，就是会映射到它的L0 file
      uint64_t L0 = (*(r->GetContainFile()->begin()))->number;
      r->InsertL0File(L0);
      L0_file_to_run_tmp_.insert(std::make_pair(L0, r));
    }
  }

  // Save the current state in *v.
  void SaveTo(Version* v) {
    //BySmallestKey cmp;
    //cmp.internal_comparator = &vset_->icmp_;

    for (int level = 0; level < config::kNumLevels; level++) {
      const std::vector<SortedRun*>& base_runs = base_->runs_[level];
      std::vector<SortedRun*>::const_iterator base_run_iter = base_runs.begin();
      std::vector<SortedRun*>::const_iterator base_run_end = base_runs.end();
      RunSet* added_run = levels_[level].added_run;
      v->runs_[level].reserve(base_runs.size() + added_run->size());
      for(; base_run_iter != base_run_end; ++base_run_iter){
        MaybeAddRun(v, level, *base_run_iter);
      }
      const RunSet* added_runs = levels_[level].added_run;
      for (const auto& run : *added_runs) {
        MaybeAddRun(v, level, run);
      }

      const std::vector<SortedRun*>& runs = v->runs_[level];
      for(size_t i = 0; i < runs.size(); i++){
        std::vector<FileMetaData*>* files = runs[i]->GetContainFile();
        for(size_t j = 0; j < files->size(); j++){
          v->files_[level].push_back(files->at(j));
        }
      }
    }

    const std::unordered_map<uint64_t, SortedRun*>& base_map = base_->L0_file_to_run_;
    v->L0_file_to_run_ = base_map;
    for(const auto& map : L0_file_to_run_tmp_){
      const std::unordered_map<uint64_t, SortedRun*>::iterator iter = v->L0_file_to_run_.find(map.first);
      if(iter != v->L0_file_to_run_.end()){
        iter->second = map.second;
      }else{
        v->L0_file_to_run_.insert(map);
      }
    }

  }

  /*void MaybeAddFile(Version* v, int level, FileMetaData* f) {
    if (levels_[level].deleted_files.count(f->number) > 0) {
      // File is deleted: do nothing
      //f是本来存在在base中，但后来要被删除的文件
      //因此不加入新的version
    } else {
      std::vector<FileMetaData*>* files = &v->files_[level];
      if (level > 0 && !files->empty()) {
        // Must not overlap
        assert(vset_->icmp_.Compare((*files)[files->size() - 1]->largest,
                                    f->smallest) < 0);
      }
      f->refs++;
      //加入Version（记录每层都有哪些文件）
      files->push_back(f);
    }
  }*/

  void MaybeAddRun(Version* v, int level, SortedRun* run){
    std::vector<SortedRun*>* runs = &v->runs_[level];
    if (levels_[level].deleted_runs.count(run->GetID()) > 0) {
      //本来存在在旧文件中的run，此时要删除

    } else {
      run->ref_++;
      runs->push_back(run);
    }
  }
};

//一个数据库只有一个VersionSet
//记录的都是全局信息
VersionSet::VersionSet(const std::string& dbname, const Options* options,
                       TableCache* table_cache,
                       const InternalKeyComparator* cmp)
    : env_(options->env),
      dbname_(dbname),
      options_(options),
      table_cache_(table_cache),
      icmp_(*cmp),
      next_file_number_(2),
      manifest_file_number_(0),  // Filled by Recover()
      last_sequence_(0),
      log_number_(0),
      prev_log_number_(0),
      next_run_number_(1),
      descriptor_file_(nullptr),
      descriptor_log_(nullptr),
      dummy_versions_(this),
      current_(nullptr) {
  AppendVersion(new Version(this));
}

VersionSet::~VersionSet() {
  current_->Unref();
  assert(dummy_versions_.next_ == &dummy_versions_);  // List must be empty
  delete descriptor_log_;
  delete descriptor_file_;
}

//把Version加入VersionSet
void VersionSet::AppendVersion(Version* v) {
  // Make "v" current
  //新加入的Version就是current_
  assert(v->refs_ == 0);
  assert(v != current_);
  if (current_ != nullptr) {
    current_->Unref();
  }
  current_ = v;
  v->Ref();

  // Append to linked list
  //链表的修改
  v->prev_ = dummy_versions_.prev_;
  v->next_ = &dummy_versions_;
  v->prev_->next_ = v;
  v->next_->prev_ = v;
}

//open的时候会调用
Status VersionSet::LogAndApply(VersionEdit* edit, port::Mutex* mu) {
  if (edit->has_log_number_) {
    assert(edit->log_number_ >= log_number_);
    assert(edit->log_number_ < next_file_number_);
  } else {
    edit->SetLogNumber(log_number_);
  }

  if (!edit->has_prev_log_number_) {
    edit->SetPrevLogNumber(prev_log_number_);
  }

  edit->SetNextFile(next_file_number_);
  edit->SetLastSequence(last_sequence_);

  Version* v = new Version(this);
  {
    //Builder(VersionSet* vset, Version* base)
    Builder builder(this, current_);
    builder.Apply(edit);
    builder.SaveTo(v);//生成新Version
  }
  Finalize(v);

  // Initialize new descriptor log file if necessary by creating
  // a temporary file that contains a snapshot of the current version.
  //VersionEdit会保存在MANIFEST文件中。
  //VersionEdit就相当于MANIFEST文件中的一条记录。
  //VersionEdit是version对象的变更记录，用于写入MANIFEST文件。
  //这样通过原始的version加上一系列的versionedit的记录，就可以恢复到最新状态。
  //博客：https://blog.csdn.net/weixin_36145588/article/details/77978433
  std::string new_manifest_file;
  Status s;
  if (descriptor_log_ == nullptr) {
    // No reason to unlock *mu here since we only hit this path in the
    // first call to LogAndApply (when opening the database).
    //只有打开数据库时会运行这段
    assert(descriptor_file_ == nullptr);
    //返回dbname/MANIFEST-number （文件名）
    new_manifest_file = DescriptorFileName(dbname_, manifest_file_number_);
    s = env_->NewWritableFile(new_manifest_file, &descriptor_file_);
    if (s.ok()) {
      //descriptor_log_用来写file
      descriptor_log_ = new log::Writer(descriptor_file_);
      //保存一次完整快照（整个数据库中那些层保存哪些文件..）
      //作为Manifest的新起点，在此基准上叠加edit
      //这次快照保存的Version是LogAndAppend调用之前的Version
      //因为前半段LogAndAppend并没有修改VersionSet的current_指针
      //“保存Version”=将这个Version的所有内容以edit形式记录
      s = WriteSnapshot(descriptor_log_);
    }
  }

  // Unlock during expensive MANIFEST log write
  {
    mu->Unlock();

    // Write new record to MANIFEST log
    if (s.ok()) {
      std::string record;
      edit->EncodeTo(&record);
      //把本次应用的edit加入manifest
      s = descriptor_log_->AddRecord(record);
      if (s.ok()) {
        s = descriptor_file_->Sync();
      }
      if (!s.ok()) {
        Log(options_->info_log, "MANIFEST write: %s\n", s.ToString().c_str());
      }
    }

    // If we just created a new descriptor file, install it by writing a
    // new CURRENT file that points to it.
    //new_manifest_file是empty的话说明没有新建manifest文件
    if (s.ok() && !new_manifest_file.empty()) {
      //修改current file，指向最新的manifest
      s = SetCurrentFile(env_, dbname_, manifest_file_number_);
    }

    mu->Lock();
  }

  // Install the new version
  if (s.ok()) {
    //新version加入VersionSet，修改current指针
    AppendVersion(v);
    log_number_ = edit->log_number_;
    prev_log_number_ = edit->prev_log_number_;
  } else {//出错
    delete v;
    if (!new_manifest_file.empty()) {
      delete descriptor_log_;
      delete descriptor_file_;
      descriptor_log_ = nullptr;
      descriptor_file_ = nullptr;
      env_->RemoveFile(new_manifest_file);
    }
  }

  return s;
}

//
Status VersionSet::Recover(bool* save_manifest) {
  struct LogReporter : public log::Reader::Reporter {
    Status* status;
    void Corruption(size_t bytes, const Status& s) override {
      if (this->status->ok()) *this->status = s;
    }
  };

  // Read "CURRENT" file, which contains a pointer to the current manifest file
  std::string current;
  //把current文件内容读出放在变量current中
  //此时current的内容就是最新manifest的文件名
  Status s = ReadFileToString(env_, CurrentFileName(dbname_), &current);
  if (!s.ok()) {
    return s;
  }
  if (current.empty() || current[current.size() - 1] != '\n') {
    return Status::Corruption("CURRENT file does not end with newline");
  }
  current.resize(current.size() - 1);

  std::string dscname = dbname_ + "/" + current;
  SequentialFile* file;
  //读出最新manifest -> file
  s = env_->NewSequentialFile(dscname, &file);
  if (!s.ok()) {
    if (s.IsNotFound()) {
      return Status::Corruption("CURRENT points to a non-existent file",
                                s.ToString());
    }
    return s;
  }

  bool have_log_number = false;
  bool have_prev_log_number = false;
  bool have_next_file = false;
  bool have_last_sequence = false;
  uint64_t next_file = 0;
  uint64_t last_sequence = 0;
  uint64_t log_number = 0;
  uint64_t prev_log_number = 0;
  //以current_为base开始恢复
  //此时的current为空
  Builder builder(this, current_);
  int read_records = 0;

  {
    LogReporter reporter;
    reporter.status = &s;
    log::Reader reader(file, &reporter, true /*checksum*/,
                       0 /*initial_offset*/);
    Slice record;
    std::string scratch;
    //读出所有的edit
    while (reader.ReadRecord(&record, &scratch) && s.ok()) {
      ++read_records;
      VersionEdit edit;
      s = edit.DecodeFrom(record);
      if (s.ok()) {
        if (edit.has_comparator_ &&
            edit.comparator_ != icmp_.user_comparator()->Name()) {
          s = Status::InvalidArgument(
              edit.comparator_ + " does not match existing comparator ",
              icmp_.user_comparator()->Name());
        }
      }
      if (s.ok()) {
        //应用到builder上
        builder.Apply(&edit);
      }

      if (edit.has_log_number_) {
        log_number = edit.log_number_;
        have_log_number = true;
      }

      if (edit.has_prev_log_number_) {
        prev_log_number = edit.prev_log_number_;
        have_prev_log_number = true;
      }

      if (edit.has_next_file_number_) {
        next_file = edit.next_file_number_;
        have_next_file = true;
      }

      if (edit.has_last_sequence_) {
        last_sequence = edit.last_sequence_;
        have_last_sequence = true;
      }
    }
  }
  delete file;
  file = nullptr;

  if (s.ok()) {
    if (!have_next_file) {
      s = Status::Corruption("no meta-nextfile entry in descriptor");
    } else if (!have_log_number) {
      s = Status::Corruption("no meta-lognumber entry in descriptor");
    } else if (!have_last_sequence) {
      s = Status::Corruption("no last-sequence-number entry in descriptor");
    }

    if (!have_prev_log_number) {
      prev_log_number = 0;
    }

    MarkFileNumberUsed(prev_log_number);
    MarkFileNumberUsed(log_number);
  }

  if (s.ok()) {
    Version* v = new Version(this);
    //所有edit应用的中间结果存放在builder中
    //最后得到的结果才保存成version
    builder.SaveTo(v);
    // Install recovered version
    Finalize(v);
    //加入VersionSet
    AppendVersion(v);
    manifest_file_number_ = next_file;
    next_file_number_ = next_file + 1;
    last_sequence_ = last_sequence;
    log_number_ = log_number;
    prev_log_number_ = prev_log_number;

    // See if we can reuse the existing MANIFEST file.
    //dscname=dbname+current
    //dscbase=current
    //重用意味着，后续的record继续记录在当前的manifest中
    //不重用则生成一个空白的manifest文件
    if (ReuseManifest(dscname, current)) {
      // No need to save new manifest
    } else {
      //需要生成一个新的manifest
      *save_manifest = true;
    }
  } else {
    std::string error = s.ToString();
    Log(options_->info_log, "Error recovering version set with %d records: %s",
        read_records, error.c_str());
  }

  return s;
}

Status VersionSet::RebuildTree(VanillaBPlusTree<std::string, uint64_t>* btree){
  Status s = current_->RebuildTree(btree);
  //current_->PrintMap(btree);
  //std::cout<<"print map end"<<std::endl;
  return s;
}

//是否延用旧的manifest
bool VersionSet::ReuseManifest(const std::string& dscname,
                               const std::string& dscbase) {
  if (!options_->reuse_logs) {
    return false;
  }
  FileType manifest_type;
  uint64_t manifest_number;
  uint64_t manifest_size;
  //太大了则开启新的
  if (!ParseFileName(dscbase, &manifest_number, &manifest_type) ||
      manifest_type != kDescriptorFile ||
      !env_->GetFileSize(dscname, &manifest_size).ok() ||
      // Make new compacted MANIFEST if old one is too big
      manifest_size >= TargetFileSize(options_)) {
    return false;
  }

  assert(descriptor_file_ == nullptr);
  assert(descriptor_log_ == nullptr);
  //复用旧的
  //如果没有到这一步的话，
  //descriptor_log_=null，那么open数据库的时候就会新生成一个manifest
  //可见LogAndApply函数
  //记录descriptor_file_=当前被复用的manifest
  //manifest只有在open的时候才会新生成
  Status r = env_->NewAppendableFile(dscname, &descriptor_file_);
  if (!r.ok()) {
    Log(options_->info_log, "Reuse MANIFEST: %s\n", r.ToString().c_str());
    assert(descriptor_file_ == nullptr);
    return false;
  }

  Log(options_->info_log, "Reusing MANIFEST %s\n", dscname.c_str());
  descriptor_log_ = new log::Writer(descriptor_file_, manifest_size);
  manifest_file_number_ = manifest_number;//记录当前正在写入的manifestfile num
  return true;
}

void VersionSet::MarkFileNumberUsed(uint64_t number) {
  if (next_file_number_ <= number) {
    next_file_number_ = number + 1;
  }
}

int VersionSet::GetTieredTriggerNum(int level){
  return config::kTieredTrigger ;
}

void VersionSet::Finalize(Version* v) {
  // Precomputed best level for next compaction
  //挑选下次进行compaction的层
  int best_level = -1;//最适合compaction的层
  double best_score = -1;//该层的分数

  //逐层计算分数
  //不算最后一层。最后一层只能被倒数第二层选择，一起compacion
  /*for (int level = 0; level < config::kNumLevels - 1; level++) {
    double score;
    if (level == 0) {
      //对于Level0，根据文件数量出发
      // We treat level-0 specially by bounding the number of files
      // instead of number of bytes for two reasons:
      //
      //如果buffer很大，而且根据
      // (1) With larger write-buffer sizes, it is nice not to do too
      // many level-0 compactions.
      //
      // (2) The files in level-0 are merged on every read and
      // therefore we wish to avoid too many files when the individual
      // file size is small (perhaps because of a small write-buffer
      // setting, or very high compression ratios, or lots of
      // overwrites/deletions).
      //level0层的文件数量/配置的L0层的trigger参数，得到score
      score = v->files_[level].size() /
              static_cast<double>(config::kL0_CompactionTrigger);
    } else {
      // Compute the ratio of current size to size limit.
      //计算出该层所有文件的总字节数
      const uint64_t level_bytes = TotalFileSize(v->files_[level]);
      //score=该层当前字节数/最大字节数
      score =
          static_cast<double>(level_bytes) / MaxBytesForLevel(options_, level);
    }*/
  
  for (int level = 0; level < config::kNumLevels; level++){
    const uint64_t level_bytes = TotalFileSize(v->files_[level]);
    double score;
    score = static_cast<double>(level_bytes) / MaxBytesForLevel(options_, level);

    //记录最高分和对应的层
    if (score > best_score) {
      best_level = level;
      best_score = score;
    }
  }

  //记录即将进行compaction的层
  v->compaction_level_ = best_level;
  v->compaction_score_ = best_score;
}

Status VersionSet::WriteSnapshot(log::Writer* log) {
  // TODO: Break up into multiple records to reduce memory usage on recovery?

  // Save metadata
  //保存一次完整记录（数据库的完整信息）
  VersionEdit edit;
  edit.SetComparatorName(icmp_.user_comparator()->Name());

  // Save compaction pointers
  /*for (int level = 0; level < config::kNumLevels; level++) {
    if (!compact_pointer_[level].empty()) {
      InternalKey key;
      key.DecodeFrom(compact_pointer_[level]);
      //把VersionSet中的信息保存在Edit中
      edit.SetCompactPointer(level, key);
    }
  }*/

  // Save files
  for (int level = 0; level < config::kNumLevels; level++) {
    //只记录current这个Version的信息
    /*const std::vector<FileMetaData*>& files = current_->files_[level];
    for (size_t i = 0; i < files.size(); i++) {
      const FileMetaData* f = files[i];
      //edit.AddFile(level, f->number, f->file_size, f->smallest, f->largest);
      //需要改为run
    }*/
    const std::vector<SortedRun*>& runs = current_->runs_[level];
    for(size_t i = 0; i < runs.size(); i++){
      SortedRun* r = runs[i];
      edit.AddSnapshotRun(*r);

    }
  }

  std::string record;
  edit.EncodeTo(&record);
  //edit的信息写入log(manifest)
  return log->AddRecord(record);
}

//返回level层有多少个文件
int VersionSet::NumLevelFiles(int level) const {
  assert(level >= 0);
  assert(level < config::kNumLevels);
  return current_->files_[level].size();
}

//  struct LevelSummaryStorage {
//    char buffer[100];
//  };
const char* VersionSet::LevelSummary(LevelSummaryStorage* scratch) const {
  // Update code if kNumLevels changes
  static_assert(config::kNumLevels == 7, "");
  std::snprintf(
      scratch->buffer, sizeof(scratch->buffer), "files[ %d %d %d %d %d %d %d ]",
      int(current_->files_[0].size()), int(current_->files_[1].size()),
      int(current_->files_[2].size()), int(current_->files_[3].size()),
      int(current_->files_[4].size()), int(current_->files_[5].size()),
      int(current_->files_[6].size()));
  return scratch->buffer;
}

//返回ikey在数据库中大概的偏移量
uint64_t VersionSet::ApproximateOffsetOf(Version* v, const InternalKey& ikey) {
  uint64_t result = 0;
  //对每一层
  for (int level = 0; level < config::kNumLevels; level++) {
    const std::vector<FileMetaData*>& files = v->files_[level];
    //对每一层的每一个文件
    for (size_t i = 0; i < files.size(); i++) {
      //ikey超出了当前这个文件的范围
      if (icmp_.Compare(files[i]->largest, ikey) <= 0) {
        // Entire file is before "ikey", so just add the file size
        result += files[i]->file_size;//加上当前的文件
      } else if (icmp_.Compare(files[i]->smallest, ikey) > 0) {
        //ikey在这个文件的开头之前
        // Entire file is after "ikey", so ignore
        if (level > 0) {
          //非L0层，后面的文件不可能包含ikey
          // Files other than level 0 are sorted by meta->smallest, so
          // no further files in this level will contain data for
          // "ikey".
          break;
        }
      } else {
        //落在文件里
        // "ikey" falls in the range for this table.  Add the
        // approximate offset of "ikey" within the table.
        Table* tableptr;
        //跳过table cache获得读table的迭代器
        Iterator* iter = table_cache_->NewIterator(
            ReadOptions(), files[i]->number, files[i]->file_size, &tableptr);
        if (tableptr != nullptr) {//获得ikey在table中的大致位置
          result += tableptr->ApproximateOffsetOf(ikey.Encode());
        }
        delete iter;
      }
    }
  }
  return result;
}

//保存仍被引用的文件（暂时还不能删除）到live中
void VersionSet::AddLiveFiles(std::set<uint64_t>* live) {
  //对于每个Version
  for (Version* v = dummy_versions_.next_; v != &dummy_versions_;
       v = v->next_) {
    for (int level = 0; level < config::kNumLevels; level++) {
      /*const std::vector<SortedRun*>& runs = v->runs_[level];
      for(size_t i = 0; i < runs.size(); i++){
        std::vector<FileMetaData*>* files = runs[i]->GetContainFile();
        for(size_t j = 0; j < files->size(); j++){
          live->insert(files->at(j)->number);
        }
      }*/
      const std::vector<FileMetaData*>& files = v->files_[level];
      for (size_t i = 0; i < files.size(); i++) {
        //把现有Version中的每个文件都加入live
        live->insert(files[i]->number);
      }
    }
  }
}

//返回level层的总字节数
int64_t VersionSet::NumLevelBytes(int level) const {
  assert(level >= 0);
  assert(level < config::kNumLevels);
  return TotalFileSize(current_->files_[level]);
}

int64_t VersionSet::MaxNextLevelOverlappingBytes() {
  int64_t result = 0;
  std::vector<FileMetaData*> overlaps;
  for (int level = 1; level < config::kNumLevels - 1; level++) {
    for (size_t i = 0; i < current_->files_[level].size(); i++) {
      const FileMetaData* f = current_->files_[level][i];
      //把level+1层中和文件f有重叠的文件加入overlaps
      current_->GetOverlappingInputs(level + 1, &f->smallest, &f->largest,
                                     &overlaps);
      const int64_t sum = TotalFileSize(overlaps);
      if (sum > result) {
        result = sum;
      }
    }
  }
  return result;
}

// Stores the minimal range that covers all entries in inputs in
// *smallest, *largest.
// REQUIRES: inputs is not empty
//保存整个inputs中的最小key和最大key
/*void VersionSet::GetRange(const std::vector<FileMetaData*>& inputs,
                          InternalKey* smallest, InternalKey* largest) {
  assert(!inputs.empty());
  smallest->Clear();
  largest->Clear();
  for (size_t i = 0; i < inputs.size(); i++) {
    FileMetaData* f = inputs[i];
    if (i == 0) {
      *smallest = f->smallest;
      *largest = f->largest;
    } else {
      if (icmp_.Compare(f->smallest, *smallest) < 0) {
        *smallest = f->smallest;
      }
      if (icmp_.Compare(f->largest, *largest) > 0) {
        *largest = f->largest;
      }
    }
  }
}*/

// Stores the minimal range that covers all entries in inputs1 and inputs2
// in *smallest, *largest.
// REQUIRES: inputs is not empty
//保存inputs1和inputs2中的最小key和最大key
/*void VersionSet::GetRange2(const std::vector<FileMetaData*>& inputs1,
                           const std::vector<FileMetaData*>& inputs2,
                           InternalKey* smallest, InternalKey* largest) {
  std::vector<FileMetaData*> all = inputs1;
  all.insert(all.end(), inputs2.begin(), inputs2.end());
  GetRange(all, smallest, largest);
}*/

//compaction操作相关的函数
//InputIterator即compaction需要读出的数据
Iterator* VersionSet::MakeInputIterator(Compaction* c) {
  ReadOptions options;
  options.verify_checksums = options_->paranoid_checks;
  //这些迭代器读出的数据不缓存在内存
  options.fill_cache = false;

  // Level-0 files have to be merged together.  For other levels,
  // we will make a concatenating iterator per level.
  // TODO(opt): use concatenating iterator for level-0 if there is no overlap
  const int space = (c->level() == 0)? c->inputs_.size(): c->inputs_runs_.size();//有几个run就有几个级联的迭代器
  //如果是Level0的话，则要构建的iterator数量是每个文件一个
  //如果不是L0的话，则是每个run构建一个级联的
  Iterator** list = new Iterator*[space];
  int num = 0;
  
  if(c->level() == 0){
    const std::vector<FileMetaData*>& files = c->inputs_;
    for(size_t i = 0; i < files.size(); i++){
      list[num++] = table_cache_->NewIterator(options, files[i]->number, files[i]->file_size);
    }
  }else{
    const std::vector<SortedRun*>& runs = c->inputs_runs_;
    for(size_t i = 0; i < runs.size(); i++){
      const std::vector<FileMetaData*>* contain_files = runs[i]->GetContainFile();
      list[num++] = NewTwoLevelIterator(
          new Version::LevelFileNumIterator(icmp_, contain_files),
          &GetFileIterator, table_cache_, options);      
    }
  }

  assert(num <= space);
  //构建一个迭代器堆
  Iterator* result = NewMergingIterator(&icmp_, list, num);
  delete[] list;
  return result;
}

//每次调用Finalize都会计算分数
//Finalize的调用发生在新Version的生成
Compaction* VersionSet::PickCompaction() {
  Compaction* c;
  int level;

  // We prefer compactions triggered by too much data in a level over
  // the compactions triggered by seeks.
  const bool size_compaction = (current_->compaction_score_ >= 1);//当前分数大于1时才触发compaction
  //std::cout<<"score:"<<current_->compaction_score_<<std::endl;
  //std::cout<<"level:"<<current_->compaction_level_<<std::endl;

  if (size_compaction) {
    level = current_->compaction_level_;
    assert(level >= 0);
    c = new Compaction(options_, level);

    for(size_t i = 0; i < current_->runs_[level].size() && i < GetTieredTriggerNum(level); i++){
      //if(level == 0){
      //  c->inputs_.push_back(current_->files_[0][i]);
      //}else{
        SortedRun* run = current_->runs_[level][i];
        c->inputs_runs_.push_back(run);
        std::vector<FileMetaData*>* files = run->GetContainFile();
        for(size_t j = 0; j < files->size(); j++){
          c->inputs_.push_back((*files)[j]);
        }
      //}
    }
    
  } else {
    return nullptr;
  }

  c->input_version_ = current_;//记录使用的是哪个Version
  c->input_version_->Ref();//对该Version的引用+1

  return c;
}

// Finds the largest key in a vector of files. Returns true if files is not
// empty.
bool FindLargestKey(const InternalKeyComparator& icmp,
                    const std::vector<FileMetaData*>& files,
                    InternalKey* largest_key) {
  if (files.empty()) {
    return false;
  }
  *largest_key = files[0]->largest;
  for (size_t i = 1; i < files.size(); ++i) {
    FileMetaData* f = files[i];
    if (icmp.Compare(f->largest, *largest_key) > 0) {
      *largest_key = f->largest;
    }
  }
  return true;
}

// Finds minimum file b2=(l2, u2) in level file for which l2 > u1 and
// user_key(l2) = user_key(u1)
FileMetaData* FindSmallestBoundaryFile(
    const InternalKeyComparator& icmp,
    const std::vector<FileMetaData*>& level_files,
    const InternalKey& largest_key) {
  const Comparator* user_cmp = icmp.user_comparator();
  FileMetaData* smallest_boundary_file = nullptr;
  for (size_t i = 0; i < level_files.size(); ++i) {
    FileMetaData* f = level_files[i];
    if (icmp.Compare(f->smallest, largest_key) > 0 &&
        user_cmp->Compare(f->smallest.user_key(), largest_key.user_key()) ==
            0) {
      if (smallest_boundary_file == nullptr ||
          icmp.Compare(f->smallest, smallest_boundary_file->smallest) < 0) {
        smallest_boundary_file = f;
      }
    }
  }
  return smallest_boundary_file;
}

//相关博客：https://zhuanlan.zhihu.com/p/60188395
// Extracts the largest file b1 from |compaction_files| and then searches for a
// b2 in |level_files| for which user_key(u1) = user_key(l2). If it finds such a
// file b2 (known as a boundary file) it adds it to |compaction_files| and then
// searches again using this new upper bound.
//
// If there are two blocks, b1=(l1, u1) and b2=(l2, u2) and
// user_key(u1) = user_key(l2), and if we compact b1 but not b2 then a
// subsequent get operation will yield an incorrect result because it will
// return the record from b2 in level i rather than from b1 because it searches
// level by level for records matching the supplied user key.
//
// parameters:
//   in     level_files:      List of files to search for boundary files.
//   in/out compaction_files: List of files to extend by adding boundary files.
// 假如有两个文件[u1 l1][u2 l2]
//u2比l1大，但是userkey相同（即l1的版本比u2新，所以internalkey l1在u2前面
//如果[u1 l1]被包括进compaction的输入文件
//则此时要把[u2 l2]也包括进compaction的输入文件
/*void AddBoundaryInputs(const InternalKeyComparator& icmp,
                       const std::vector<FileMetaData*>& level_files,
                       std::vector<FileMetaData*>* compaction_files) {
  InternalKey largest_key;

  // Quick return if compaction_files is empty.
  if (!FindLargestKey(icmp, *compaction_files, &largest_key)) {
    return;
  }

  bool continue_searching = true;
  while (continue_searching) {
    FileMetaData* smallest_boundary_file =
        FindSmallestBoundaryFile(icmp, level_files, largest_key);

    // If a boundary file was found advance largest_key, otherwise we're done.
    if (smallest_boundary_file != NULL) {
      compaction_files->push_back(smallest_boundary_file);
      largest_key = smallest_boundary_file->largest;
    } else {
      continue_searching = false;
    }
  }
}*/

/*void VersionSet::SetupOtherInputs(Compaction* c) {
  const int level = c->level();
  InternalKey smallest, largest;

  //c->inputs_[0]是已经选择的level层的待compaction文件
  //从current_->files_[level]中选择边界是同一个userkey的加入inputs_中
  AddBoundaryInputs(icmp_, current_->files_[level], &c->inputs_[0]);
  GetRange(c->inputs_[0], &smallest, &largest);

  //把level+1层中与level层参与compaction的文件有重叠的加入inputs_
  current_->GetOverlappingInputs(level + 1, &smallest, &largest,
                                 &c->inputs_[1]);
  AddBoundaryInputs(icmp_, current_->files_[level + 1], &c->inputs_[1]);

  // Get entire range covered by compaction
  //compaction输入文件整体的最小key和最大key
  InternalKey all_start, all_limit;
  GetRange2(c->inputs_[0], c->inputs_[1], &all_start, &all_limit);

  // See if we can grow the number of inputs in "level" without
  // changing the number of "level+1" files we pick up.
  //是否能在不改变level+1层compaction文件的前提下，扩展level层
  //下一层不为空
  if (!c->inputs_[1].empty()) {
    std::vector<FileMetaData*> expanded0;
    current_->GetOverlappingInputs(level, &all_start, &all_limit, &expanded0);
    AddBoundaryInputs(icmp_, current_->files_[level], &expanded0);
    const int64_t inputs0_size = TotalFileSize(c->inputs_[0]);
    const int64_t inputs1_size = TotalFileSize(c->inputs_[1]);
    const int64_t expanded0_size = TotalFileSize(expanded0);
    if (expanded0.size() > c->inputs_[0].size() &&
        inputs1_size + expanded0_size <
            ExpandedCompactionByteSizeLimit(options_)) {
      InternalKey new_start, new_limit;
      GetRange(expanded0, &new_start, &new_limit);
      std::vector<FileMetaData*> expanded1;
      current_->GetOverlappingInputs(level + 1, &new_start, &new_limit,
                                     &expanded1);
      AddBoundaryInputs(icmp_, current_->files_[level + 1], &expanded1);
      //level+1层没有扩展出新文件，符合扩展要求
      if (expanded1.size() == c->inputs_[1].size()) {
        Log(options_->info_log,
            "Expanding@%d %d+%d (%ld+%ld bytes) to %d+%d (%ld+%ld bytes)\n",
            level, int(c->inputs_[0].size()), int(c->inputs_[1].size()),
            long(inputs0_size), long(inputs1_size), int(expanded0.size()),
            int(expanded1.size()), long(expanded0_size), long(inputs1_size));
        smallest = new_start;
        largest = new_limit;
        c->inputs_[0] = expanded0;
        c->inputs_[1] = expanded1;
        GetRange2(c->inputs_[0], c->inputs_[1], &all_start, &all_limit);
      }
    }
  }

  // Compute the set of grandparent files that overlap this compaction
  // (parent == level+1; grandparent == level+2)
  if (level + 2 < config::kNumLevels) {
    current_->GetOverlappingInputs(level + 2, &all_start, &all_limit,
                                   &c->grandparents_);
  }

  // Update the place where we will do the next compaction for this level.
  // We update this immediately instead of waiting for the VersionEdit
  // to be applied so that if the compaction fails, we will try a different
  // key range next time.
  //这一层下一次会开展compaction的文件
  //compaction_pointer_在这里会被设置
  compact_pointer_[level] = largest.Encode().ToString();
  c->edit_.SetCompactPointer(level, largest);
}*/

/*Compaction* VersionSet::CompactRange(int level, const InternalKey* begin,
                                     const InternalKey* end) {
  std::vector<FileMetaData*> inputs;
  current_->GetOverlappingInputs(level, begin, end, &inputs);
  if (inputs.empty()) {
    return nullptr;
  }

  // Avoid compacting too much in one shot in case the range is large.
  // But we cannot do this for level-0 since level-0 files can overlap
  // and we must not pick one file and drop another older file if the
  // two files overlap.
  if (level > 0) {
    //获得参数信息
    const uint64_t limit = MaxFileSizeForLevel(options_, level);
    uint64_t total = 0;
    for (size_t i = 0; i < inputs.size(); i++) {
      uint64_t s = inputs[i]->file_size;
      total += s;
      if (total >= limit) {
        inputs.resize(i + 1);
        break;
      }
    }
  }

  Compaction* c = new Compaction(options_, level);
  c->input_version_ = current_;
  c->input_version_->Ref();
  c->inputs_[0] = inputs;
  SetupOtherInputs(c);
  return c;
}*/

Compaction::Compaction(const Options* options, int level)
    : level_(level),
      max_output_file_size_(MaxFileSizeForLevel(options, level)),
      input_version_(nullptr),
      grandparent_index_(0),
      seen_key_(false),
      overlapped_bytes_(0) {
  for (int i = 0; i < config::kNumLevels; i++) {
    level_ptrs_[i] = 0;
  }
}

Compaction::~Compaction() {
  if (input_version_ != nullptr) {
    input_version_->Unref();
  }
}

//所有input文件都当作要被删除的文件写入edit
/*void Compaction::AddInputDeletions(VersionEdit* edit) {
  for (int which = 0; which < 2; which++) {
    for (size_t i = 0; i < inputs_[which].size(); i++) {
      edit->RemoveFile(level_ + which, inputs_[which][i]->number);
    }
  }
}*/

//所有参与compaction的Run都要加入待删除
void Compaction::AddRunDeletions(VersionEdit* edit){
  for(size_t i = 0; i < inputs_runs_.size(); i++){
    edit->RemoveRun(inputs_runs_[i]);
  }
}
//当key的type是delete的时候
//如果level+1以上都没有该key
//则直接丢弃该key
bool Compaction::IsBaseLevelForKey(const Slice& user_key) {
  // Maybe use binary search to find right entry instead of linear search?
  const Comparator* user_cmp = input_version_->vset_->icmp_.user_comparator();
  for (int lvl = level_ + 2; lvl < config::kNumLevels; lvl++) {
    //对level+1后的每一层的文件
    const std::vector<FileMetaData*>& files = input_version_->files_[lvl];
    //level_ptrs_[lvl]用来保存已经检查到lvl的哪一个sstable
    while (level_ptrs_[lvl] < files.size()) {
      FileMetaData* f = files[level_ptrs_[lvl]];
      //userkey在f访问内，退出
      if (user_cmp->Compare(user_key, f->largest.user_key()) <= 0) {
        // We've advanced far enough
        if (user_cmp->Compare(user_key, f->smallest.user_key()) >= 0) {
          // Key falls in this file's range, so definitely not base level
          return false;//不可直接丢弃
        }
        break;
      }
      level_ptrs_[lvl]++;
    }
  }
  return true;//后续level都不存在该key
}

//compaction到这个key时与level+2层重叠是否超出阈值
/*bool Compaction::ShouldStopBefore(const Slice& internal_key) {
  const VersionSet* vset = input_version_->vset_;
  // Scan to find earliest grandparent file that contains key.
  const InternalKeyComparator* icmp = &vset->icmp_;
  while (grandparent_index_ < grandparents_.size() &&
         icmp->Compare(internal_key,
                       grandparents_[grandparent_index_]->largest.Encode()) >
             0) {//直到internal_key超过某个grandparents文件的最大key
    if (seen_key_) {
      overlapped_bytes_ += grandparents_[grandparent_index_]->file_size;
    }
    grandparent_index_++;
  }
  seen_key_ = true;

  if (overlapped_bytes_ > MaxGrandParentOverlapBytes(vset->options_)) {
    // Too much overlap for current output; start new output
    overlapped_bytes_ = 0;
    return true;//需要新建文件
  } else {
    return false;
  }
}*/

//对输入文件的处理结束
void Compaction::ReleaseInputs() {
  if (input_version_ != nullptr) {
    input_version_->Unref();
    input_version_ = nullptr;
  }
}

}  // namespace leveldb
