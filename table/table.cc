// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//2.28 ✔
//从file读各部分的内容
//file->table

#include "leveldb/table.h"

#include "leveldb/cache.h"
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "leveldb/options.h"
#include "table/block.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"
#include "db/dbformat.h"

namespace leveldb {

struct Table::Rep {
  ~Rep() {
    delete filter;
    delete[] filter_data;
    delete index_block;
  }

  Options options;
  Status status;
  RandomAccessFile* file;
  uint64_t cache_id;
  FilterBlockReader* filter;
  Block* data_info_block;
  Block* model_info_block;
  const char* filter_data;
  const char* data_info_data;
  const char* model_info_data;

  double common_prefix_len_;
  uint32_t error_bound_;  

  BlockHandle metaindex_handle;  // Handle to metaindex_block: saved from footer
  Block* index_block;
  bool is_model = false;
};

//打开一个sst file
//生成index block
//调用ReadMeta -> 生成meta block
Status Table::Open(const Options& options, RandomAccessFile* file,
                   uint64_t size, Table** table) {
  *table = nullptr;
  if (size < Footer::kEncodedLength) {
    return Status::Corruption("file is too short to be an sstable");
  }

  //把footer的内容读到footer_input中
  char footer_space[Footer::kEncodedLength];
  Slice footer_input;
  Status s = file->Read(size - Footer::kEncodedLength, Footer::kEncodedLength,
                        &footer_input, footer_space);
  if (!s.ok()) return s;

  //解析到footer中
  Footer footer;
  s = footer.DecodeFrom(&footer_input);
  if (!s.ok()) return s;

  // Read the index block
  //
  BlockContents index_block_contents;
  ReadOptions opt;
  if (options.paranoid_checks) {
    opt.verify_checksums = true;
  }
  //ReadBlock() format.cc
  //根据index_handle从file中读index block的内容放入contents
  s = ReadBlock(file, opt, footer.index_handle(), &index_block_contents);

  //生成index_block
  
  if (s.ok()) {
    // We've successfully read the footer and the index block: we're
    // ready to serve requests.
    Block* index_block = new PrefixBlock(index_block_contents);
    Rep* rep = new Table::Rep;
    rep->options = options;
    rep->file = file;
    rep->metaindex_handle = footer.metaindex_handle();
    rep->index_block = index_block;
    rep->cache_id = (options.block_cache ? options.block_cache->NewId() : 0);
    rep->filter_data = nullptr;
    rep->filter = nullptr;
    rep->data_info_block = nullptr;
    rep->data_info_data = nullptr;
    rep->model_info_block = nullptr;
    rep->model_info_data = nullptr;
    *table = new Table(rep);
    //*table的类型是Table
    //**table的类型是Table*
    (*table)->ReadMeta(footer);
  }

  return s;
}

void Table::ReadMeta(const Footer& footer) {
  // if (rep_->options.filter_policy == nullptr) {
  //   return;  // Do not need any metadata
  // }

  // TODO(sanjay): Skip this if footer.metaindex_handle() size indicates
  // it is an empty block.
  ReadOptions opt;
  if (rep_->options.paranoid_checks) {
    opt.verify_checksums = true;
  }
  BlockContents contents;
  //把metaindex block的内容读到contents中
  //metaindex block的内容是key：meta data的名字、value：handle
  if (!ReadBlock(rep_->file, opt, footer.metaindex_handle(), &contents).ok()) {
    // Do not propagate errors since meta info is not needed for operation
    return;
  }
  //重组为一个meta block
  Block* meta = new PrefixBlock(contents);

  //在meta block上构建迭代器
  Iterator* iter = meta->NewIterator(BytewiseComparator(), false);
  //构造filter block的名字
  std::string key;

  if (rep_->options.filter_policy != nullptr) {
    key = "filter.";
    key.append(rep_->options.filter_policy->Name());
    //在metadata中找到filter block对应的条目的位置
    iter->Seek(key);
    //得到的是filter block对应的handle
    if (iter->Valid() && iter->key() == Slice(key)) {
      ReadFilter(iter->value());
    }
  }

  key = "data_info";
  iter->Seek(key);
  if (iter->Valid() && iter->key() == Slice(key)) {
    ReadIndexInfo(iter->value());
  }

  key = "model_info";
  iter->Seek(key);
  if (iter->Valid() && iter->key() == Slice(key)) {
    ReadModelInfo(iter->value());
    rep_->is_model = true;
  }

  key = "common_prefix_len";
  iter->Seek(key);
  if (iter->Valid() && iter->key() == Slice(key)) {
    Slice v = iter->value();
    rep_->common_prefix_len_ = DecodeFixed64(v.data());
  }

  key = "error_bound";
  iter->Seek(key);
  if (iter->Valid() && iter->key() == Slice(key)) {
    Slice v = iter->value();
    rep_->error_bound_ = DecodeFixed32(v.data());
  }

  delete iter;
  delete meta;
}

//v：Filter的handle
void Table::ReadIndexInfo(const Slice& data_info_handle_value) {
  Slice v = data_info_handle_value;
  BlockHandle data_info_handle;
  if (!data_info_handle.DecodeFrom(&v).ok()) {
    return;
  }

  // We might want to unify with ReadBlock() if we start
  // requiring checksum verification in Table::Open.
  ReadOptions opt;
  if (rep_->options.paranoid_checks) {
    opt.verify_checksums = true;
  }
  BlockContents block;
  //根据handle从file中读block
  //block存放filter block
  if (!ReadBlock(rep_->file, opt, data_info_handle, &block).ok()) {
    return;
  }
  //heap_allocated?
  if (block.heap_allocated) {
    //第一个data是block的实际data，第二个data是sclice的data
    rep_->data_info_data = block.data.data();  // Will need to delete later
  }
  //filter块的信息
  rep_->data_info_block = new FlatBlock(block);
}

void Table::ReadModelInfo(const Slice& model_info_handle_value) {
  Slice v = model_info_handle_value;
  BlockHandle model_info_handle;
  if (!model_info_handle.DecodeFrom(&v).ok()) {
    return;
  }

  // We might want to unify with ReadBlock() if we start
  // requiring checksum verification in Table::Open.
  ReadOptions opt;
  if (rep_->options.paranoid_checks) {
    opt.verify_checksums = true;
  }
  BlockContents block;
  //根据handle从file中读block
  //block存放filter block
  if (!ReadBlock(rep_->file, opt, model_info_handle, &block).ok()) {
    return;
  }
  //heap_allocated?
  if (block.heap_allocated) {
    //第一个data是block的实际data，第二个data是sclice的data
    rep_->model_info_data = block.data.data();  // Will need to delete later
  }
  //filter块的信息
  rep_->model_info_block = new FlatBlock(block);
}

void Table::ReadFilter(const Slice& filter_handle_value) {
  Slice v = filter_handle_value;
  BlockHandle filter_handle;
  if (!filter_handle.DecodeFrom(&v).ok()) {
    return;
  }

  // We might want to unify with ReadBlock() if we start
  // requiring checksum verification in Table::Open.
  ReadOptions opt;
  if (rep_->options.paranoid_checks) {
    opt.verify_checksums = true;
  }
  BlockContents block;
  //根据handle从file中读block
  //block存放filter block
  if (!ReadBlock(rep_->file, opt, filter_handle, &block).ok()) {
    return;
  }
  //heap_allocated?
  if (block.heap_allocated) {
    //第一个data是block的实际data，第二个data是sclice的data
    rep_->filter_data = block.data.data();  // Will need to delete later
  }
  //filter块的信息
  rep_->filter = new FilterBlockReader(rep_->options.filter_policy, block.data);
}

Table::~Table() { delete rep_; }

static void DeleteBlock(void* arg, void* ignored) {
  delete reinterpret_cast<Block*>(arg);
}

static void DeleteCachedBlock(const Slice& key, void* value) {
  Block* block = reinterpret_cast<Block*>(value);
  delete block;
}

static void ReleaseBlock(void* arg, void* h) {
  Cache* cache = reinterpret_cast<Cache*>(arg);
  Cache::Handle* handle = reinterpret_cast<Cache::Handle*>(h);
  cache->Release(handle);
}

// Convert an index iterator value (i.e., an encoded BlockHandle)
// into an iterator over the contents of the corresponding block.
//传入index block的一个条目，构造一个对应data block的迭代器
Iterator* Table::BlockReader(void* arg, const ReadOptions& options,
                             const Slice& index_value, const bool& is_model,
                            const bool& from_data_info) {
  Table* table = reinterpret_cast<Table*>(arg);
  Cache* block_cache = table->rep_->options.block_cache;
  Block* block = nullptr;
  Cache::Handle* cache_handle = nullptr;

  BlockHandle handle;
  IndexHandle index_handle;
  //index_value就是handle的平铺值
  Status s;
  if(from_data_info){
    Slice input = index_value;
    s = index_handle.DecodeFrom(&input);
    index_handle.ToBlockHandle(handle);
  }else{
    Slice input = index_value;
    s = handle.DecodeFrom(&input);
  }

  // We intentionally allow extra stuff in index_value so that we
  // can add more features in the future.

  if (s.ok()) {
    BlockContents contents;
    //读block前先在block cache中查找是否保存该block
    if (block_cache != nullptr) {
      char cache_key_buffer[16];
      //cache_key_buffer的内容：[cache_id][该block在文件中的偏移量]
      //用于构造block在block cache中的key
      EncodeFixed64(cache_key_buffer, table->rep_->cache_id);
      EncodeFixed64(cache_key_buffer + 8, handle.offset());
      Slice key(cache_key_buffer, sizeof(cache_key_buffer));
      //在cache中查找是否存在该block。返回的handle就是指向所map的值对象
      cache_handle = block_cache->Lookup(key);
      //存在
      if (cache_handle != nullptr) { 
        block = reinterpret_cast<Block*>(block_cache->Value(cache_handle));
      } else {
        //不存在则还是从file中读
        s = ReadBlock(table->rep_->file, options, handle, &contents);
        assert(s.ok());
        if (s.ok()) {
          if(is_model){
            block = new FlatBlock(contents);
          } else{
            block = new PrefixBlock(contents);
          }
          //如果允许缓存，放入block cache
          if (contents.cachable && options.fill_cache) {
            cache_handle = block_cache->Insert(key, block, block->size(),
                                               &DeleteCachedBlock);
          }
        }
      }
    } else {//不允许缓存，直接从file读
      s = ReadBlock(table->rep_->file, options, handle, &contents);
      if (s.ok()) {
        if(is_model){
          block = new FlatBlock(contents);
        } else{
          block = new PrefixBlock(contents);
        }
      }
    }
  }
  //构造一个迭代器，（？用于释放block的空间？）
  //cleanup函数在迭代器销毁时会逐个调用
  Iterator* iter;
  if (block != nullptr) {
    //这里的NewIterator是Block类的方法，在函数内部会用Block对象内保存的信息构造迭代器
    //..return new Iter(comparator, data_, restart_offset_, num_restarts);
    iter = block->NewIterator(table->rep_->options.comparator, true);
    if (cache_handle == nullptr) {
      iter->RegisterCleanup(&DeleteBlock, block, nullptr);
    } else {
      iter->RegisterCleanup(&ReleaseBlock, block_cache, cache_handle);
    }
  } else {
    iter = NewErrorIterator(s);
  }
  return iter;
}


Iterator* Table::NewIterator(const ReadOptions& options) const {
  return NewTwoLevelIterator(
      rep_->index_block->NewIterator(rep_->options.comparator, false),//构造index_block上的迭代器
      &Table::BlockReader, const_cast<Table*>(this), options, rep_->is_model, false);//BlockReader函数，index value->data block iter
      //this 把这个对象传给NewTwoLevelIterator函数（在这个table对象上构造的）
}

Status Table::GetByModel(const ReadOptions& options, const Slice& k, void* arg,
                          void (*handle_result)(void*, const Slice&,
                                                const Slice&)){
  Status s;
  Iterator* miter = rep_->model_info_block->NewIterator(BytewiseComparator(), false);
  //定位条目
  std::string target = Slice(k.data(), k.size()-8).ToString();     
  PutFixed32(&target, 1);  // start index
  PutFixed32(&target, 0);  // end index     
  //std::cout<<"seek model key:"<<target<<std::endl;                        
  miter->Seek(target);
  if(miter->Valid()){
    Slice handle_value = miter->value();
    ModelParam param;
    //std::cout<<"target key:"<<Slice(k.data(), k.size()-8).ToString()<<" model start key:"<<miter->key().ToString()<<std::endl;
    if (param.DecodeFrom(&handle_value).ok()) {
      std::pair<size_t, size_t> range = sindex::GreedyPLR::GetSearchRange(Slice(k.data(), k.size()-8).ToString(), rep_->common_prefix_len_, param.slope(), param.intercept(), rep_->error_bound_);
      //std::cout<<"model predicted range: ["<<range.first<<","<<range.second<<"]"<<std::endl;
      size_t start_index = range.first;
      size_t end_index = range.second;

      size_t start_data_block = start_index / rep_->options.block_contain_keys;
      size_t end_data_block = end_index / rep_->options.block_contain_keys;

      Iterator* iiter = rep_->data_info_block->NewIterator(rep_->options.comparator, false);
      std::string index_key = k.ToString();
      PutFixed32(&index_key, start_data_block);
      PutFixed32(&index_key, end_data_block);
      iiter->Seek(index_key);

      //std::cout<<"data info block key:"<<iiter->key().ToString()<<" "<<k.ToString()<<std::endl;

      if(iiter->Valid()){
        //[todo]r->data_info_block->Add(r->data_block->SmallestKey(), r->); 
        Slice handle_value = iiter->value();
        IndexHandle handle;
        if(handle.DecodeFrom(&handle_value).ok()){
          uint32_t block_index = handle.index();

          Iterator* block_iter = BlockReader(this, options, iiter->value(), true, true);
          //在block层面上的对key的查找
          std::string target = k.ToString();

          int left_bound = start_index - block_index *  rep_->options.block_contain_keys;
          int right_bound = end_index - block_index *  rep_->options.block_contain_keys;

          PutFixed32(&target, left_bound < 0 ? 0 : left_bound);  // start index
          PutFixed32(&target, right_bound > rep_->options.block_contain_keys -1 ? rep_->options.block_contain_keys -1 : right_bound);  // end index
          
          // std::cout<<"data block bound:"<<left_bound + block_index *  rep_->options.block_contain_keys
          // <<" "<<right_bound + block_index *  rep_->options.block_contain_keys<<std::endl;

          block_iter->Seek(target);
          if (block_iter->Valid()) {
            //？对找到的kv对的某种处理？
            (*handle_result)(arg, block_iter->key(), block_iter->value());
          }
          s = block_iter->status();
          delete block_iter;          
        }
      }
      if (s.ok()) {
        s = iiter->status();
      }
      delete iiter;      
    }
  }
  delete miter;
  return s;
}

//在table层面上的对key的查找
Status Table::InternalGet(const ReadOptions& options, const Slice& k, void* arg,
                          void (*handle_result)(void*, const Slice&,
                                                const Slice&)) {
  Status s;
  //iiter在index_block上的迭代器
  if(rep_->model_info_block != nullptr){
    return GetByModel(options, k, arg, handle_result);
  }
  Iterator* iiter = rep_->index_block->NewIterator(rep_->options.comparator, false);
  //定位条目
  iiter->Seek(k);
  if (iiter->Valid()) {
    // Slice target_key = ExtractUserKey(k);
    // Slice index_key = ExtractUserKey(iiter->key());
    // std::cout<<"get target:"<<target_key.ToString()<<" index block key:"<<index_key.ToString()<<std::endl;
    //条目的值就是目标data block的handle
    Slice handle_value = iiter->value();
    FilterBlockReader* filter = rep_->filter;
    BlockHandle handle;
    //bf判断这个data block中是否包含key
    if (filter != nullptr && handle.DecodeFrom(&handle_value).ok() &&
        !filter->KeyMayMatch(handle.offset(), k)) {
      // Not found
    } else {//可能存在key
      //构造这个data block上的迭代器
      Iterator* block_iter = BlockReader(this, options, iiter->value(), false, false);
      //在block层面上的对key的查找
      block_iter->Seek(k);
      if (block_iter->Valid()) {
        //？对找到的kv对的某种处理？
        (*handle_result)(arg, block_iter->key(), block_iter->value());
      }
      s = block_iter->status();
      delete block_iter;
    }
  }
  if (s.ok()) {
    s = iiter->status();
  }
  delete iiter;
  return s;
}

//key在table中的大概的偏移量
uint64_t Table::ApproximateOffsetOf(const Slice& key) const {
  Iterator* index_iter =
      rep_->index_block->NewIterator(rep_->options.comparator, false);
  //先定位block
  index_iter->Seek(key);
  uint64_t result;
  if (index_iter->Valid()) {
    BlockHandle handle;//把block handle放在handle中
    Slice input = index_iter->value();
    Status s = handle.DecodeFrom(&input);
    if (s.ok()) {
      result = handle.offset();
    } else {//handle解析失败的情况
      // Strange: we can't decode the block handle in the index block.
      // We'll just return the offset of the metaindex block, which is
      // close to the whole file size for this case.
      result = rep_->metaindex_handle.offset();
    }
  } else {//超出了index记录的所有最大key（说明key不在这个文件中）
    // key is past the last key in the file.  Approximate the offset
    // by returning the offset of the metaindex block (which is
    // right near the end of the file).
    result = rep_->metaindex_handle.offset();
  }
  delete index_iter;
  return result;
}

}  // namespace leveldb
