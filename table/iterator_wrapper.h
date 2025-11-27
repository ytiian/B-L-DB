// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//2.24✔

#ifndef STORAGE_LEVELDB_TABLE_ITERATOR_WRAPPER_H_
#define STORAGE_LEVELDB_TABLE_ITERATOR_WRAPPER_H_

#include "leveldb/iterator.h"
#include "leveldb/slice.h"

namespace leveldb {

// A internal wrapper class with an interface similar to Iterator that
// caches the valid() and key() results for an underlying iterator.
// This can help avoid virtual function calls and also gives better
// cache locality.
class IteratorWrapper {
 public:
  IteratorWrapper() : iter_(nullptr), valid_(false) {}
  explicit IteratorWrapper(Iterator* iter) : iter_(nullptr) { Set(iter); }
  ~IteratorWrapper() { delete iter_; }
  Iterator* iter() const { return iter_; }

  // Takes ownership of "iter" and will delete it when destroyed, or
  // when Set() is invoked again.
  void Set(Iterator* iter) {
    delete iter_;
    iter_ = iter;
    if (iter_ == nullptr) {
      valid_ = false;
    } else {
      Update();
      UpdateIndex();
    }
  }

  // Iterator interface methods
  bool Valid() const { return valid_; }
  Slice key() const {
    assert(Valid());
    return key_;
  }
  Slice value() const {
    assert(Valid());
    return iter_->value();
  }
  // Methods below require iter() != nullptr
  Status status() const {
    assert(iter_);
    return iter_->status();
  }
  void Next() {
    assert(iter_);
    iter_->Next();
    Update();
    UpdateIndex();
  }
  void Prev() {
    assert(iter_);
    iter_->Prev();
    Update();
  }
  void Seek(const Slice& k) {
    assert(iter_);
    iter_->Seek(k);
    Update();
  }
  void SeekToFirst() {
    //std::cout<<"iter SeekToFirst"<<std::endl;
    assert(iter_);
    iter_->SeekToFirst();
    Update();
    UpdateIndex();
  }
  void SeekToLast() {
    assert(iter_);
    iter_->SeekToLast();
    Update();
  }


  void NextIndex(){
    assert(iter_);
    iter_->NextIndex();
    UpdateIndex();
    Update();
  }

  void ResetDataBlock(){
    assert(iter_);
    iter_->ResetDataBlock();
  }

  Slice indexKey() const {
    assert(IndexValid());
    return index_key_;
  }

  bool IsIndex() {
    return iter_->IsIndex();
  }

  bool IndexValid() const {
    return index_valid_;
  }

 private:
  void Update() {
    valid_ = iter_->Valid();
    if (valid_) {
      key_ = iter_->key();
    }else{
      key_ = Slice();
    }
  }

  void UpdateIndex(){
    //index_valid_ = iter_->IndexValid();
    //if (index_valid_) {
    index_valid_ = iter_->IndexValid();
    if(index_valid_){
      index_key_ = iter_->indexKey();
    }else{
      index_key_ = Slice();
    } 
  }


  Iterator* iter_;
  bool valid_;
  Slice key_;

  bool index_valid_;
  Slice index_key_;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_TABLE_ITERATOR_WRAPPER_H_
