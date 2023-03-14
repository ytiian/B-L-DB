// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//2.27✔
//测试代码博客：https://izualzhy.cn/filter-block

#include "table/filter_block.h"

#include "leveldb/filter_policy.h"
#include "util/coding.h"

namespace leveldb {

// See doc/table_format.md for an explanation of the filter block format.

// Generate new filter every 2KB of data
//每2KB生成一个filter
static const size_t kFilterBaseLg = 11;
static const size_t kFilterBase = 1 << kFilterBaseLg;

//FilterPolicy 包含Name(),CreateFilter(),KeyMayMatch()
FilterBlockBuilder::FilterBlockBuilder(const FilterPolicy* policy)
    : policy_(policy) {}

//每个data block写入文件后，都会调用StartBlock，
//传入的block_offset并不是这次的data block自己的offset，而是这次data block的offset + size，
//也就是下一个data block的offset，
//也等于当前写入的所有data block的总size。
void FilterBlockBuilder::StartBlock(uint64_t block_offset) {
  //当前该data block需要构建的filter数量
  uint64_t filter_index = (block_offset / kFilterBase);//每kFilterBase大小就有一个Filter
  assert(filter_index >= filter_offsets_.size());
  //filter_offsets_,数组，记录当前已知filter的offset
  while (filter_index > filter_offsets_.size()) {
    GenerateFilter();
    //假如一个data block需要生成3个filter
    //则其实针对一整个data block生成整个的filter
    //而三个偏移量指向的都是这一个filter
    //（实际并不是2KB一个filter，而是2KB记录一次偏移）
  }
}

void FilterBlockBuilder::AddKey(const Slice& key) {
  Slice k = key;
  start_.push_back(keys_.size());//每个key在keys_中的偏移量
  keys_.append(k.data(), k.size());//key追加到key_中
}

Slice FilterBlockBuilder::Finish() {
  if (!start_.empty()) {//start不为空
    GenerateFilter();
  }

  // Append array of per-filter offsets
  //result_存放的是所有filter的内容
  //array_offset是filter偏移量数组开始的位置
  const uint32_t array_offset = result_.size();
  for (size_t i = 0; i < filter_offsets_.size(); i++) {
    PutFixed32(&result_, filter_offsets_[i]);//偏移量数组写入result_
  }

  PutFixed32(&result_, array_offset);//偏移量数组开始的位置写入result_
  result_.push_back(kFilterBaseLg);  // Save encoding parameter in result
  //也就是写入lg(base)
  //最后的result_就是整个filter block的内容
  //[filter][filter offset array][filter offset array offset][lg[base]]
  return Slice(result_);
}

void FilterBlockBuilder::GenerateFilter() {
  //start_保存key在key_中的偏移量
  const size_t num_keys = start_.size();//待构建filter的key数量
  if (num_keys == 0) {
    // Fast path if there are no keys for this filter
    //和之前的索引指向同一个filter位置
    filter_offsets_.push_back(result_.size());
    return;
  }

  // Make list of keys from flattened key structure
  start_.push_back(keys_.size());  // Simplify length computation
  tmp_keys_.resize(num_keys);
  for (size_t i = 0; i < num_keys; i++) {
    const char* base = keys_.data() + start_[i];
    size_t length = start_[i + 1] - start_[i];
    tmp_keys_[i] = Slice(base, length);
  }

  // Generate filter for current set of keys and append to result_.
  filter_offsets_.push_back(result_.size());
  //filter内容存放在result_中
  policy_->CreateFilter(&tmp_keys_[0], static_cast<int>(num_keys), &result_);

  tmp_keys_.clear();
  keys_.clear();
  start_.clear();
}

FilterBlockReader::FilterBlockReader(const FilterPolicy* policy,
                                     const Slice& contents)
    : policy_(policy), data_(nullptr), offset_(nullptr), num_(0), base_lg_(0) {
  size_t n = contents.size();//单位：字节
  //至少需要5字节
  if (n < 5) return;  // 1 byte for base_lg_ and 4 for start of offset array
  base_lg_ = contents[n - 1];//最后1字节是lg
  //offset array 偏移量 字段 开始的位置
  //last_word：offset array开始的位置
  uint32_t last_word = DecodeFixed32(contents.data() + n - 5);
  if (last_word > n - 5) return;//n-5是前面部分的字节数，显然偏移量不可能超过n-5
  data_ = contents.data();
  offset_ = data_ + last_word;//起始位置+数组开始位置偏移量=数组开始位置指针
  num_ = (n - 5 - last_word) / 4;
  //n-5-last_word得到整个array占用的大小，
  //array中每个字节占4KB
  //最终得到array中的条目数量
  //（即有多少给filter索引）
}

bool FilterBlockReader::KeyMayMatch(uint64_t block_offset, const Slice& key) {
  uint64_t index = block_offset >> base_lg_;//相当于block_offset/2KB，得到filter索引
  if (index < num_) {
    //offset是array开始的位置
    //最终得到该索引（数组内容
    uint32_t start = DecodeFixed32(offset_ + index * 4);
    //下一个索引（数组下一条内容
    uint32_t limit = DecodeFixed32(offset_ + index * 4 + 4);
    //limit最大也只能指向offset_-data_的这段位置
    if (start <= limit && limit <= static_cast<size_t>(offset_ - data_)) {
      //data_+start：该filter开始位置
      //limit-start：该filter的长度
      Slice filter = Slice(data_ + start, limit - start);
      //前半部分就是获取filter的内容
      //从filter中检索key
      return policy_->KeyMayMatch(key, filter);
    } else if (start == limit) {//这个filter是空的
      // Empty filters do not match any keys
      return false;
    }
  }
  return true;  // Errors are treated as potential matches
}

}  // namespace leveldb
