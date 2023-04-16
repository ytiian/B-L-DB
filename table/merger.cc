// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//待看

#include "table/merger.h"

#include "leveldb/comparator.h"
#include "leveldb/iterator.h"
#include "table/iterator_wrapper.h"
#include "trees/vanilla_b_plus_tree.h"
#include "db/dbformat.h"
#include <unordered_map>

namespace leveldb {

namespace {
//继承Iterator
class MergingIterator : public Iterator {
 public:
 //构造一个新Iterator，合并了children中的迭代器
 //children是Iterator* 的数组
  MergingIterator(const Comparator* comparator, Iterator** children, int n, VanillaBPlusTree<std::string, uint64_t>* btree = nullptr, 
                  const std::unordered_map<uint64_t, int>* index_map = nullptr, int mem_num = 0)
      : comparator_(comparator),
        children_(new IteratorWrapper[n]),
        n_(n),
        current_(nullptr),
        mem_num_(mem_num),
        direction_(kForward) {      
    for (int i = 0; i < n; i++) {
      children_[i].Set(children[i]);
    }
    btree_ = btree;
    //如果btree_为空，则是在compaction的过程中调用了MergingIterator函数
    //否则则是构建迭代器时调用
    if(btree_ != nullptr){
      btree_iter_ = btree->NewTreeIterator();
      index_map_ = index_map;
    }else{
      btree_iter_ = nullptr;
    }
  }

  ~MergingIterator() override { 
    delete[] children_; 
    if(btree_iter_ != nullptr)
    {
      delete btree_iter_;
    }
  }

  bool Valid() const override { return (current_ != nullptr); }

  void SeekToFirst() override {
    if(btree_ == nullptr)
    {
      for (int i = 0; i < n_; i++) {
        children_[i].SeekToFirst();
      }
      FindSmallest();
      direction_ = kForward;//前向*/      
    }else{
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
  }

  void SeekToLast() override {
    if(btree_ == nullptr){
      for (int i = 0; i < n_; i++) {
        children_[i].SeekToLast();
      }
      FindLargest();
      direction_ = kReverse;//反向*/
    }else{
      for (int i = 0; i < n_; i++) {
        children_[i].SeekToLast();
      }      
      uint64_t run_no;
      int index;
      btree_iter_->SeekToLast();
      run_no = btree_iter_->Value();
      //index_map_记录run的id为run_no的run此时在children_中的索引
      /*const auto iter = index_map_->find(run_no);
      if(iter != index_map_->end()){
        index = iter->second;
        current_ = &children_[index];
      }*/
      //direction_ = kReverse;
    }
  }

  void Seek(const Slice& target) override {
    if(btree_ == nullptr){
      for (int i = 0; i < n_; i++) {
        children_[i].Seek(target);
      }
      FindSmallest();
      direction_ = kForward;      
    }
    else{
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
  }

  void Next() override {
    assert(Valid());

    // Ensure that all children are positioned after key().
    // If we are moving in the forward direction, it is already
    // true for all of the non-current_ children since current_ is
    // the smallest child and key() == current_->key().  Otherwise,
    // we explicitly position the non-current_ children.
    if(btree_ == nullptr){
      if (direction_ != kForward) {
        for (int i = 0; i < n_; i++) {
          IteratorWrapper* child = &children_[i];
          if (child != current_) {
            child->Seek(key());
            if (child->Valid() &&
                comparator_->Compare(key(), child->key()) == 0) {
              child->Next();
            }
          }
        }
        direction_ = kForward;
      }

      current_->Next();
      FindSmallest();
    }else{
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
  }

  void Prev() override {
    assert(Valid());

    // Ensure that all children are positioned before key().
    // If we are moving in the reverse direction, it is already
    // true for all of the non-current_ children since current_ is
    // the largest child and key() == current_->key().  Otherwise,
    // we explicitly position the non-current_ children.
    if(btree_ == nullptr){
      if (direction_ != kReverse) {
        for (int i = 0; i < n_; i++) {
          IteratorWrapper* child = &children_[i];
          if (child != current_) {
            child->Seek(key());
            if (child->Valid()) {
              // Child is at first entry >= key().  Step back one to be < key()
              child->Prev();
            } else {
              // Child has no entries >= key().  Position at last entry.
              child->SeekToLast();
            }
          }
        }
        direction_ = kReverse;
      }
      current_->Prev();
      FindLargest();
    }else{
      if(btree_iter_->GetForward()){
        btree_iter_->SetForward(false);
      }
      uint64_t run_no;
      int index;
        btree_iter_->Prev();
        run_no = btree_iter_->Value();
        const auto iter = index_map_->find(run_no);
        if(iter != index_map_->end()){
          index = iter->second;
          current_ = &children_[index];
        }
        ParsedInternalKey ikey;
        ParseInternalKey(children_[index].key(), &ikey);
        while(children_[index].key() != ikey.user_key){
          children_[index].Prev();
          ParseInternalKey(children_[index].key(), &ikey);
        }
        current_ = &children_[index];
      if(!btree_iter_->Valid()){
        current_ = nullptr;    
      }  
    }
  }

  Slice key() const override {
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
  // Which direction is the iterator moving?
  enum Direction { kForward, kReverse };

  void FindSmallest();
  void FindLargest();

  // We might want to use a heap in case there are lots of children.
  // For now we use a simple array since we expect a very small number
  // of children in leveldb.
  const Comparator* comparator_;
  IteratorWrapper* children_;
  int n_;
  BTree<std::string, uint64_t>::Iterator* btree_iter_;
  VanillaBPlusTree<std::string, uint64_t>* btree_;
  const std::unordered_map<uint64_t, int>* index_map_; //x号run对应的迭代器在child[y]中
  IteratorWrapper* current_;
  Direction direction_;
  int mem_num_;
};

void MergingIterator::FindSmallest() {
  IteratorWrapper* smallest = nullptr;
  for (int i = 0; i < n_; i++) {
    IteratorWrapper* child = &children_[i];
    if (child->Valid()) {
      if (smallest == nullptr) {
        smallest = child;
      } else if (comparator_->Compare(child->key(), smallest->key()) < 0) {
        smallest = child;
      }
    }
  }
  current_ = smallest;
}

void MergingIterator::FindLargest() {
  IteratorWrapper* largest = nullptr;
  for (int i = n_ - 1; i >= 0; i--) {
    IteratorWrapper* child = &children_[i];
    if (child->Valid()) {
      if (largest == nullptr) {
        largest = child;
      } else if (comparator_->Compare(child->key(), largest->key()) > 0) {
        largest = child;
      }
    }
  }
  current_ = largest;
}
}  // namespace

Iterator* NewMergingIterator(const Comparator* comparator, Iterator** children,
                             int n){
  assert(n >= 0);
  if (n == 0) {
    return NewEmptyIterator();
  } else if (n == 1) {
    return children[0];
  } else {
    return new MergingIterator(comparator, children, n);
  }
}


Iterator* NewMergingIterator(const Comparator* comparator, Iterator** children,
                             int n, VanillaBPlusTree<std::string, uint64_t>* btree, const std::unordered_map<uint64_t, int>* index_map, int mem_num){
  assert(n >= 0);
  if (n == 0) {
    return NewEmptyIterator();
  } else if (n == 1) {
    return children[0];
  } else {
    return new MergingIterator(comparator, children, n, btree, index_map, mem_num);
  }
}

}  // namespace leveldb
