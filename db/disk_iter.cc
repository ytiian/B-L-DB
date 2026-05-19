#include "table/merger.h"

#include "leveldb/comparator.h"
#include "leveldb/iterator.h"
#include "table/iterator_wrapper.h"
#include "table/two_level_iterator.h"
#include "sindex/sindex_wrapper.h"
#include "db/dbformat.h"
#include "db/dbformat.h"
#include <unordered_map>
#include <chrono>
#include <iostream>

namespace leveldb {
namespace{
class DiskIterator : public Iterator {
 public:
  DiskIterator(const Comparator* comparator, Iterator** children, int n, SIndexWrapper* sindex, 
                const std::unordered_map<uint64_t, int>* index_map)
    :comparator_(comparator),
    children_(new IteratorWrapper[n]),
    n_(n),
    current_(nullptr),
    direction_(kForward),
    sindex_(sindex),
    index_map_(index_map){
    for (int i = 0; i < n; i++) {
      children_[i].Set(children[i]);
    }
    index_iter_ = sindex->NewIterator(2);//[todo]
  }

  ~DiskIterator() override { 
    delete[] children_; 
    delete index_iter_;
    delete index_map_;
  }

  bool Valid() const override {
    return index_iter_->Valid(); 
  }

  void SeekToFirst() override{
    for (int i = 0; i < n_; i++) {
      children_[i].SeekToFirst();
    }      
    uint64_t run_no;
    int index;
    index_iter_->SeekToFirst();
    run_no = index_iter_->Value();
    //index_map_记录run的id为run_no的run此时在children_中的索引
    auto iter = index_map_->find(run_no);
    assert(iter != index_map_->end());
    index = iter->second;
    current_ = &children_[index];
    //direction_ = kForward;        
  }

  void SeekToLast() override{

  }        

  void Seek(const Slice& target) override { 
    for (int i = 0; i < n_; i++) { 
       //auto t_seek_begin = std::chrono::high_resolution_clock::now();
      children_[i].Seek(target); 
      //  auto t_seek_end = std::chrono::high_resolution_clock::now();
      //   auto seek_time_us =
      //     std::chrono::duration_cast<std::chrono::microseconds>(
      //     t_seek_end - t_seek_begin)
      //     .count();
      //     std::cout<<"DiskIterator Seek child["<<i<<"] time us:"<<seek_time_us<<std::endl;
    } 
    //std::cout<<"children number:"<<n_<<std::endl;
    std::string sindex_target_key = Slice(target.data(), target.size()-8).ToString();
    // auto t_index_begin = std::chrono::high_resolution_clock::now();
    index_iter_->Seek(index_key_t(sindex_target_key)); 
    // auto t_index_end = std::chrono::high_resolution_clock::now();
    // auto index_seek_time_us =
    //   std::chrono::duration_cast<std::chrono::microseconds>(
    //       t_index_end - t_index_begin)
    //       .count();
    // std::cout<<"DiskIterator Seek index_iter_ time us:"<<index_seek_time_us<<std::endl;

    SeekFindKV(); 
    if(current_ == nullptr){
       //std::cout<<"DiskIterator Seek key not found:"<<target.ToString()<<std::endl;
      return ; 
    } 
  }

  void SeekFindKV(){
    if(!index_iter_->Valid()){
      current_ = nullptr;
      return;
    }    
    uint64_t run_no;
    int index;
    run_no = index_iter_->Value();
    auto iter = index_map_->find(run_no);
    while (iter == index_map_->end())
    {
      index_iter_->Next();
      if(!index_iter_->Valid()){
        current_ = nullptr;
        return;
      }
      run_no = index_iter_->Value();
      iter = index_map_->find(run_no);
    }
    assert(iter != index_map_->end());
    index = iter->second;
    current_ = &children_[index];
  }

  void Next() override{
    index_iter_->Next();
    FindKV();
  }        

  void Prev() override{

  }        

  Slice key() const override{
    assert(Valid());
    //assert(current_ != nullptr);
    return current_->key();
  }

  Slice value() const override {
    assert(Valid());
    return current_->value();
  }

  Status status() const override {
    Status status;
    for (int i = 0; i < n_; i++) {
        status = children_[i].status();
        if (!status.ok()) {
            break;
        }
    }
    return status;
  }    

  void FindKV() {
    if(!index_iter_->Valid()){
      current_ = nullptr;
      return;
    }
    uint64_t run_no;
    int index;
    run_no = index_iter_->Value();
    auto iter = index_map_->find(run_no);
    while (iter == index_map_->end())
    {
      index_iter_->Next();
      if(!index_iter_->Valid()){
        current_ = nullptr;
        return;
      }
      run_no = index_iter_->Value();
      iter = index_map_->find(run_no);
    }
    assert(iter != index_map_->end());
    index = iter->second;
    //std::cout<<"search key:"<<index_iter_->Key().ToString()<<" run_no:"<<run_no<<" index:"<<index<<std::endl;
    
    std::string sindex_key_str = index_iter_->Key().ToString();
    const Slice& sindex_target_key = Slice(sindex_key_str);    

    bool init = false;
    while(comparator_->Compare(ExtractUserKey(children_[index].indexKey()), sindex_target_key) < 0){
      //std::cout<<"index key:"<<children_[index].indexKey().ToString()<<" sindex_key:"<<sindex_target_key.ToString()<<"compare: "<<comparator_->Compare(ExtractUserKey(children_[index].indexKey()), sindex_target_key)<<std::endl;
      children_[index].NextIndex();
      init = true;
    }

    if(init){
      children_[index].ResetDataBlock();
    }
   
    
    Slice internal_key_ = children_[index].key();
    Slice user_key = ExtractUserKey(internal_key_);

    assert(user_key.size() == sindex_target_key.size());
    while(comparator_->Compare(user_key, sindex_target_key) != 0){
      assert(user_key.size() == sindex_target_key.size());
      if(comparator_->Compare(user_key, sindex_target_key) > 0){
        std::cout<<"error: user_key:"<<user_key.ToString()<<" sindex_key:"<<sindex_target_key.ToString()<<"compare: "<<comparator_->Compare(user_key, sindex_target_key)<<std::endl;
        return ;
      }
      children_[index].Next();
      internal_key_ = children_[index].key();
      user_key = ExtractUserKey(internal_key_);
    }

    current_ = &children_[index];
  }

    void NextIndex() override {}
    Slice indexKey() const override {
      return Slice();
    }
    bool IndexValid() const override {
      return false;
    }
    void ResetDataBlock() override {}
    bool IsIndex() override {}

 private:
  enum Direction { kForward, kReverse }; 
  const Comparator* comparator_;
  IteratorWrapper* children_;
  int n_;
  typename SIndexWrapper::Iterator* index_iter_;//todo
  SIndexWrapper* sindex_;
  const std::unordered_map<uint64_t, int>* index_map_; //x号run对应的迭代器在child[y]中
  IteratorWrapper* current_;
  Direction direction_;    
};

}

Iterator* NewDiskIterator(const Comparator* comparator, Iterator** children,
                             int n, SIndexWrapper* sindex, const std::unordered_map<uint64_t, int>* index_map){
  assert(n >= 0);
  if (n == 0) {
    return NewEmptyIterator();
  } else if (n == 1) {
    return children[0];
  } else {
    return new DiskIterator(comparator, children, n, sindex, index_map);
  }
}
}