// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//2.24✔

#ifndef STORAGE_LEVELDB_TABLE_BLOCK_H_
#define STORAGE_LEVELDB_TABLE_BLOCK_H_

#include <cstddef>
#include <cstdint>

#include "leveldb/iterator.h"

namespace leveldb {

struct BlockContents;
class Comparator;

class Block {
 public:
  virtual ~Block() = default;

  virtual size_t size() const = 0;
  virtual Iterator* NewIterator(const Comparator* comparator, const bool is_data_block) = 0;
};

class PrefixBlock : public Block {
 public:
  // Initialize the block with the specified contents.
  explicit PrefixBlock(const BlockContents& contents);

  PrefixBlock(const PrefixBlock&) = delete;
  PrefixBlock& operator=(const PrefixBlock&) = delete;

  ~PrefixBlock() override;

  size_t size() const override { return size_; }
  Iterator* NewIterator(const Comparator* comparator, const bool is_data_block) override;

 private:
  class Iter;

  uint32_t NumRestarts() const;

  const char* data_;
  size_t size_;
  uint32_t restart_offset_;  // Offset in data_ of restart array
  bool owned_;               // Block owns data_[]
};


class FlatBlock : public Block {
 public:
  // Initialize the block with the specified contents.
  explicit FlatBlock(const BlockContents& contents);

  FlatBlock(const FlatBlock&) = delete;
  FlatBlock& operator=(const FlatBlock&) = delete;

  ~FlatBlock() override;

  size_t size() const override { return size_; }
  Iterator* NewIterator(const Comparator* comparator, const bool is_data_block) override;

 private:
  class Iter;

  uint32_t NumRestarts() const;

  const char* data_;
  size_t size_;
  bool owned_;               // Block owns data_[]

  size_t key_size_;
  size_t value_size_;
  size_t num_cnt_;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_TABLE_BLOCK_H_