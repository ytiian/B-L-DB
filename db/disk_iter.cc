#include "table/merger.h"

#include "leveldb/comparator.h"
#include "leveldb/iterator.h"
#include "table/iterator_wrapper.h"
#include "skiplist/skip_list.h"
#include "db/dbformat.h"
#include "db/dbformat.h"
#include <unordered_map>

namespace leveldb {
namespace{
class DiskIterator : public Iterator {
 public:
  DiskIterator(const Comparator* comparator, Iterator** children, int n, SkipListBase* skip_index, 
                const std::unordered_map<uint64_t, int>* index_map)
    :comparator_(comparator),
    children_(new IteratorWrapper[n]),
    n_(n),
    current_(nullptr),
    direction_(kForward),
    skip_index_(skip_index),
    index_map_(index_map){
    for (int i = 0; i < n; i++) {
      children_[i].Set(children[i]);
    }
    index_iter_ = skip_index->NewIterator();//[todo]
  }

  ~DiskIterator() override { 
    delete[] children_; 
    delete index_iter_;
    delete index_map_;
  }

  bool Valid() const override {
    return (index_iter_->Valid()); 
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

  void Seek(const Slice& target) override{
    for (int i = 0; i < n_; i++) {
      children_[i].Seek(target);
    }
    uint64_t run_no;
    int index;
    index_iter_->Seek(target);
    SeekFindKV();
    //direction_ = kForward;          
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

    Slice internal_key_ = children_[index].key();
    Slice user_key = Slice(internal_key_.data(), internal_key_.size() - 8);
    const Slice& target_key = index_iter_->Key();
    while(comparator_->Compare(user_key, target_key) != 0){
      children_[index].Next();
      internal_key_ = children_[index].key();
      user_key = Slice(internal_key_.data(), internal_key_.size() - 8);
    }

    current_ = &children_[index];
  }
 private:
  enum Direction { kForward, kReverse }; 
  const Comparator* comparator_;
  IteratorWrapper* children_;
  int n_;
  SkipListBase::Iterator* index_iter_;//todo
  SkipListBase* skip_index_;
  const std::unordered_map<uint64_t, int>* index_map_; //x号run对应的迭代器在child[y]中
  IteratorWrapper* current_;
  Direction direction_;    
};

}

Iterator* NewDiskIterator(const Comparator* comparator, Iterator** children,
                             int n, SkipListBase* skip_index, const std::unordered_map<uint64_t, int>* index_map){
  assert(n >= 0);
  if (n == 0) {
    return NewEmptyIterator();
  } else if (n == 1) {
    return children[0];
  } else {
    return new DiskIterator(comparator, children, n, skip_index, index_map);
  }
}
}