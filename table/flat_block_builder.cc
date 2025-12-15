// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// BlockBuilder generates blocks where keys are prefix-compressed:
//
// When we store a key, we drop the prefix shared with the previous
// string.  This helps reduce the space requirement significantly.
// Furthermore, once every K keys, we do not apply the prefix
// compression and store the entire key.  We call this a "restart
// point".  The tail end of the block stores the offsets of all of the
// restart points, and can be used to do a binary search when looking
// for a particular key.  Values are stored as-is (without compression)
// immediately following the corresponding key.
//
// An entry for a particular key-value pair has the form:
//     shared_bytes: varint32 
//     unshared_bytes: varint32 
//     value_length: varint32 
//     key_delta: char[unshared_bytes]
//     value: char[value_length]
// shared_bytes == 0 for restart points.
//
// The trailer of the block has the form:
//     restarts: uint32[num_restarts]
//     num_restarts: uint32 
// restarts[i] contains the offset within the block of the ith restart point.
//2.24✔

#include "table/block_builder.h"

#include <algorithm>
#include <cassert>

#include "leveldb/comparator.h"
#include "leveldb/options.h"
#include "util/coding.h"

namespace leveldb {

FlatBlockBuilder::FlatBlockBuilder(const Options* options)
    : options_(options), counter_(0), finished_(false) {
  assert(options->block_restart_interval >= 1);
}

void FlatBlockBuilder::Reset() {//重置
  buffer_.clear();
  counter_ = 0;
  finished_ = false;
  key_size_ = 0;
  value_size_ = 0;
  largest_key_.clear();
  smallest_key_.clear();
}

size_t FlatBlockBuilder::CurrentSizeEstimate() const {
  return (buffer_.size());                       // Raw data buffer
}

Slice FlatBlockBuilder::Finish() {
  // Append restart array
  PutFixed32(&buffer_, key_size_);
  PutFixed32(&buffer_, value_size_);
  PutFixed32(&buffer_, counter_);  // No more entries for flat block

  finished_ = true;
  return Slice(buffer_);
}

void FlatBlockBuilder::Add(const Slice& key, const Slice& value) {
  assert(!finished_);

  if(smallest_key_.empty()){
    smallest_key_ = key.ToString();
  }
  largest_key_ = key.ToString();

  key_size_ = key.size();
  value_size_ = value.size();

  // Add string delta to buffer_ followed by value
  buffer_.append(key.data(), key.size());//只要保存不相同的部分就可以
  buffer_.append(value.data(), value.size());

  // Update state
  counter_++;
}

}  // namespace leveldb