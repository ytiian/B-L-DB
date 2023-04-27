#include "table/merger.h"

#include "leveldb/comparator.h"
#include "leveldb/iterator.h"
#include "table/iterator_wrapper.h"
#include "trees/vanilla_b_plus_tree.h"
#include "db/dbformat.h"
#include "db/dbformat.h"
#include <unordered_map>

namespace leveldb {
namespace{
class DiskIterator : public Iterator {
 public:
  DiskIterator(const Comparator* comparator, Iterator** children, int n, VanillaBPlusTree<std::string, uint64_t>* btree, 
                const std::unordered_map<uint64_t, int>* index_map)
    :comparator_(comparator),
    children_(new IteratorWrapper[n]),
    n_(n),
    current_(nullptr),
    direction_(kForward),
    btree_(btree),
    index_map_(index_map){
    for (int i = 0; i < n; i++) {
      children_[i].Set(children[i]);
    }
    btree_iter_ = btree->NewTreeIterator();
  }

  ~DiskIterator() override { 
    delete[] children_; 
    delete btree_iter_;
    delete index_map_;
  }

  bool Valid() const override { return (current_ != nullptr); }

  void SeekToFirst() override{
    for (int i = 0; i < n_; i++) {
      children_[i].SeekToFirst();
    }      
    uint64_t run_no;
    int index;
    btree_iter_->SeekToFirst();
    run_no = btree_iter_->Value();
    //index_map_记录run的id为run_no的run此时在children_中的索引
    auto iter = index_map_->find(run_no);
    while(iter == index_map_->end()){
      btree_iter_->Next();
      run_no = btree_iter_->Value();
      iter = index_map_->find(run_no);
    }
    index = iter->second;
    current_ = &children_[index];
    //direction_ = kForward;        
  }

  void SeekToLast() override{
    for (int i = 0; i < n_; i++) {
      children_[i].SeekToLast();
    }      
    uint64_t run_no;
    int index;
    btree_iter_->SeekToLast();
    run_no = btree_iter_->Value();
    auto iter = index_map_->find(run_no);
    while(iter == index_map_->end()){
      btree_iter_->Prev();
      run_no = btree_iter_->Value();
      iter = index_map_->find(run_no);
    }
    index = iter->second;
    current_ = &children_[index];
  }        

  void Seek(const Slice& target) override{
    for (int i = 0; i < n_; i++) {
      children_[i].Seek(target);
    }
    uint64_t run_no;
    int index;
    btree_iter_->Seek(target.ToString());
    run_no = btree_iter_->Value();
    const auto iter = index_map_->find(run_no);
    if(iter != index_map_->end()){
      index = iter->second;
      current_ = &children_[index];
    }
    //direction_ = kForward;          
  }

  void Next() override{
  //children_的初始化问题？
    if(!btree_iter_->GetForward()){
      btree_iter_->SetForward(true);
    }
    uint64_t run_no;
    int index;
    btree_iter_->Next();
    run_no = btree_iter_->Value();
    auto iter = index_map_->find(run_no);
    while(iter == index_map_->end()){
    //std::cout<<"yes"<<std::endl;
      btree_iter_->Next();
      run_no = btree_iter_->Value();
      iter = index_map_->find(run_no);
    }
    index = iter->second;
    ParsedInternalKey ikey;
    ParseInternalKey(children_[index].key(), &ikey);
    /*for(const auto index : *index_map_){
    std::cout<<index.first<<" "<<index.second<<std::endl;
    }*/
    /*btree_iter_->SeekToFirst();
    int read = 100;
    while(read>=0){
      std::cout<<"btree:"<<btree_iter_->Key()<<" "<<btree_iter_->Value()<<std::endl;
      btree_iter_->Next();
      read--;
    }
    std::cout<<btree_iter_->Key()<<std::endl;*/
    while(btree_iter_->Key() != ikey.user_key.ToString()){
      //std::cout<<"1"<<std::endl;
      //std::cout<<btree_iter_->Key()<<" "<<ikey.user_key.ToString()<<std::endl;
      children_[index].Next();
      ParseInternalKey(children_[index].key(), &ikey);
    }
    current_ = &children_[index];
    if(!btree_iter_->Valid()){
      current_ = nullptr;
    }
  }        

  void Prev() override{
  //children_的初始化问题？
    if(btree_iter_->GetForward()){
      btree_iter_->SetForward(false);
    }
    uint64_t run_no;
    int index;
    btree_iter_->Prev();
    run_no = btree_iter_->Value();
    auto iter = index_map_->find(run_no);
    while(iter == index_map_->end()){
    //std::cout<<"yes"<<std::endl;
      btree_iter_->Prev();
      run_no = btree_iter_->Value();
      iter = index_map_->find(run_no);
    }
    index = iter->second;
    ParsedInternalKey ikey;
    ParseInternalKey(children_[index].key(), &ikey);
    /*for(const auto index : *index_map_){
    std::cout<<index.first<<" "<<index.second<<std::endl;
    }*/
    /*btree_iter_->SeekToFirst();
    int read = 100;
    while(read>=0){
      std::cout<<"btree:"<<btree_iter_->Key()<<" "<<btree_iter_->Value()<<std::endl;
      btree_iter_->Next();
      read--;
    }
    std::cout<<btree_iter_->Key()<<std::endl;*/
    while(btree_iter_->Key() != ikey.user_key.ToString()){
      //std::cout<<"1"<<std::endl;
      //std::cout<<btree_iter_->Key()<<" "<<ikey.user_key.ToString()<<std::endl;
      children_[index].Prev();
      ParseInternalKey(children_[index].key(), &ikey);
    }
    bool roll_back = true;
    int i = 0;
    while(btree_iter_->Key() == ikey.user_key.ToString()){
      children_[index].Prev();
      if(!children_[index].Valid()){
        //std::cout<<"wait.."<<i<<std::endl;
        i++;
        children_[index].SeekToFirst();
        roll_back = false;
        break;
      }
      ParseInternalKey(children_[index].key(), &ikey);      
    }
    if(roll_back){
      children_[index].Next();
    }
    current_ = &children_[index];
    if(!btree_iter_->Valid()){
      current_ = nullptr;
    }
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

 private:
  enum Direction { kForward, kReverse }; 
  const Comparator* comparator_;
  IteratorWrapper* children_;
  int n_;
  BTree<std::string, uint64_t>::Iterator* btree_iter_;
  VanillaBPlusTree<std::string, uint64_t>* btree_;
  const std::unordered_map<uint64_t, int>* index_map_; //x号run对应的迭代器在child[y]中
  IteratorWrapper* current_;
  Direction direction_;    
};

}

Iterator* NewDiskIterator(const Comparator* comparator, Iterator** children,
                             int n, VanillaBPlusTree<std::string, uint64_t>* btree, const std::unordered_map<uint64_t, int>* index_map){
  assert(n >= 0);
  if (n == 0) {
    return NewEmptyIterator();
  } else if (n == 1) {
    return children[0];
  } else {
    return new DiskIterator(comparator, children, n, btree, index_map);
  }
}
}