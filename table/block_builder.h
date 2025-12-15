// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//2.24✔

#ifndef STORAGE_LEVELDB_TABLE_BLOCK_BUILDER_H_
#define STORAGE_LEVELDB_TABLE_BLOCK_BUILDER_H_

#include <cstdint>
#include <vector>

#include "leveldb/slice.h"

namespace leveldb {

struct Options;

class BlockBuilder {
 public:
  virtual ~BlockBuilder() = default;

  // Reset the contents as if the BlockBuilder was just constructed.
  //重置内容，就像刚刚构建了BlockBuilder一样。
  virtual void Reset() = 0;

  // REQUIRES: Finish() has not been called since the last call to Reset().
  // REQUIRES: key is larger than any previously added key
  virtual void Add(const Slice& key, const Slice& value) = 0;

  // Finish building the block and return a slice that refers to the
  // block contents.  The returned slice will remain valid for the
  // lifetime of this builder or until Reset() is called.
  virtual Slice Finish() = 0;

  // Returns an estimate of the current (uncompressed) size of the block
  // we are building.
  virtual size_t CurrentSizeEstimate() const = 0;

  // Return true iff no entries have been added since the last Reset()
  virtual bool empty() const = 0;

  virtual std::string LargestKey() const = 0;
  
  virtual std::string SmallestKey() const = 0;
};

class PrefixBlockBuilder : public BlockBuilder {
 public:
  explicit PrefixBlockBuilder(const Options* options);

  PrefixBlockBuilder(const PrefixBlockBuilder&) = delete;
  PrefixBlockBuilder& operator=(const PrefixBlockBuilder&) = delete;

  void Reset() override;
  void Add(const Slice& key, const Slice& value) override;
  Slice Finish() override;
  size_t CurrentSizeEstimate() const override;
  bool empty() const override { return buffer_.empty(); }

  std::string LargestKey() const override{
    return "";
  }

  std::string SmallestKey() const override{
    return "";
  }

 private:
  const Options* options_;
  std::string buffer_;              // Destination buffer
  std::vector<uint32_t> restarts_;  // Restart points
  int counter_;                     // Number of entries emitted since restart
  bool finished_;                   // Has Finish() been called?
  std::string last_key_;
  std::string largest_key_;
  std::string smallest_key_;
};

class FlatBlockBuilder : public BlockBuilder {
 public:
  explicit FlatBlockBuilder(const Options* options);

  FlatBlockBuilder(const FlatBlockBuilder&) = delete;
  FlatBlockBuilder& operator=(const FlatBlockBuilder&) = delete;

  void Reset() override;
  void Add(const Slice& key, const Slice& value) override;
  Slice Finish() override;
  size_t CurrentSizeEstimate() const override;
  bool empty() const override { return buffer_.empty(); }

  std::string LargestKey() const override{
    assert(largest_key_.size() > 0);
    return largest_key_;
  }

  std::string SmallestKey() const override{
    assert(smallest_key_.size() > 0);
    return smallest_key_;
  }

 private:
  const Options* options_;
  std::string buffer_;              // Destination buffer
  int counter_ = 0;                     // Number of entries emitted since restart
  bool finished_;                   // Has Finish() been called?
  size_t key_size_ = 0;
  size_t value_size_ = 0;
  std::string largest_key_;
  std::string smallest_key_;
};


}  // namespace leveldb

#endif  // STORAGE_LEVELDB_TABLE_BLOCK_BUILDER_H_
