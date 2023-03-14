// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//3.1 ✔
//第一层 指在index block上，迭代的是index block的条目（data block的handle
//index block的每个条目对应一个data block
//第二层 指在data block上，迭代的是data block的条目（KV对


#include "table/two_level_iterator.h"

#include "leveldb/table.h"
#include "table/block.h"
#include "table/format.h"
#include "table/iterator_wrapper.h"

namespace leveldb {

namespace {

//定义函数指针的方式
//定义指针BlockFunction，指向返回值为Iterator* 三个形参的函数
typedef Iterator* (*BlockFunction)(void*, const ReadOptions&, const Slice&);

class TwoLevelIterator : public Iterator {
 public:
  //block_function 传递某个函数
  TwoLevelIterator(Iterator* index_iter, BlockFunction block_function,
                   void* arg, const ReadOptions& options);
  ~TwoLevelIterator() override;

  void Seek(const Slice& target) override;
  void SeekToFirst() override;
  void SeekToLast() override;
  void Next() override;
  void Prev() override;

  //data_iter是IteratorWrapper类型
  bool Valid() const override { return data_iter_.Valid(); }
  Slice key() const override {
    assert(Valid());
    return data_iter_.key();
  }
  Slice value() const override {
    assert(Valid());
    return data_iter_.value();
  }
  Status status() const override {
    // It'd be nice if status() returned a const Status& instead of a Status
    if (!index_iter_.status().ok()) {
      return index_iter_.status();
    } else if (data_iter_.iter() != nullptr && !data_iter_.status().ok()) {
      return data_iter_.status();
    } else {
      return status_;
    }
  }

 private:
  void SaveError(const Status& s) {
    if (status_.ok() && !s.ok()) status_ = s;
  }
  void SkipEmptyDataBlocksForward();
  void SkipEmptyDataBlocksBackward();
  void SetDataIterator(Iterator* data_iter);
  void InitDataBlock();

  BlockFunction block_function_;
  void* arg_;
  const ReadOptions options_;
  Status status_;
  IteratorWrapper index_iter_;
  IteratorWrapper data_iter_;  // May be nullptr
  // If data_iter_ is non-null, then "data_block_handle_" holds the
  // "index_value" passed to block_function_ to create the data_iter_.
  std::string data_block_handle_;
};

TwoLevelIterator::TwoLevelIterator(Iterator* index_iter,
                                   BlockFunction block_function, void* arg,
                                   const ReadOptions& options)
    : block_function_(block_function),
      arg_(arg),
      options_(options),
      index_iter_(index_iter),
      data_iter_(nullptr) {}

TwoLevelIterator::~TwoLevelIterator() = default;

//2，3，5    如果target=4 则停在第三个条目（>=target
//逻辑：先定位是哪个data block，再定位key
void TwoLevelIterator::Seek(const Slice& target) {
  index_iter_.Seek(target);//index_iter现在指向
  InitDataBlock();
  if (data_iter_.iter() != nullptr) data_iter_.Seek(target);
  SkipEmptyDataBlocksForward();
}

//先定位第一个data block，再定位这个block的第一条
void TwoLevelIterator::SeekToFirst() {
  index_iter_.SeekToFirst();
  InitDataBlock();
  if (data_iter_.iter() != nullptr) data_iter_.SeekToFirst();
  SkipEmptyDataBlocksForward();
}

//先定位最后一个data block，再定位这个block的最后一条
void TwoLevelIterator::SeekToLast() {
  index_iter_.SeekToLast();
  InitDataBlock();
  if (data_iter_.iter() != nullptr) data_iter_.SeekToLast();
  SkipEmptyDataBlocksBackward();
}

void TwoLevelIterator::Next() {
  assert(Valid());
  data_iter_.Next();
  //即使data_iter_.Next()失败（也就是读完了全部，没得可读）
  //下面这个函数会继续向前寻找下一个可读的block
  SkipEmptyDataBlocksForward();
}

void TwoLevelIterator::Prev() {   
  assert(Valid());
  data_iter_.Prev();
  SkipEmptyDataBlocksBackward();
}

void TwoLevelIterator::SkipEmptyDataBlocksForward() {
  //data_block_为空的情况或者最后一条已经读完的情况
  while (data_iter_.iter() == nullptr || !data_iter_.Valid()) {
    // Move to next block
    if (!index_iter_.Valid()) {
      SetDataIterator(nullptr);
      return;
    }
    index_iter_.Next();
    InitDataBlock();
    //定位到新data block的第一条
    if (data_iter_.iter() != nullptr) data_iter_.SeekToFirst();
  }
}

//逻辑类似上面
void TwoLevelIterator::SkipEmptyDataBlocksBackward() {
  while (data_iter_.iter() == nullptr || !data_iter_.Valid()) {
    // Move to next block
    if (!index_iter_.Valid()) {
      SetDataIterator(nullptr);
      return;
    }
    index_iter_.Prev();
    InitDataBlock();
    if (data_iter_.iter() != nullptr) data_iter_.SeekToLast();
  }
}

//保存第二级迭代器
void TwoLevelIterator::SetDataIterator(Iterator* data_iter) {
  if (data_iter_.iter() != nullptr) SaveError(data_iter_.status());
  data_iter_.Set(data_iter);
}

void TwoLevelIterator::InitDataBlock() {
  if (!index_iter_.Valid()) {
    SetDataIterator(nullptr);
  } else {
    //index_iter_现在指向的条目是所找data block的handle
    Slice handle = index_iter_.value();
    //现在data_iter_恰好指向的是将要查询的data block
    //则什么都不做
    if (data_iter_.iter() != nullptr &&
        handle.compare(data_block_handle_) == 0) {
      // data_iter_ is already constructed with this iterator, so
      // no need to change anything
    } else {
      //block_function_是函数指针，因此需要*
      //构造一个指向data block的迭代器
      Iterator* iter = (*block_function_)(arg_, options_, handle);
      data_block_handle_.assign(handle.data(), handle.size());
      SetDataIterator(iter);
    }
  }
}

}  // namespace

Iterator* NewTwoLevelIterator(Iterator* index_iter,
                              BlockFunction block_function, void* arg,
                              const ReadOptions& options) {
  return new TwoLevelIterator(index_iter, block_function, arg, options);
}

}  // namespace leveldb
