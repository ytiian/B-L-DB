// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//2.28 ✔ 
//构造table
//TableBuilder
//table->file

#include "leveldb/table_builder.h"

#include <cassert>

#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "leveldb/options.h"
#include "table/block_builder.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {

struct TableBuilder::Rep {
  Rep(const Options& opt, WritableFile* f)
      : options(opt),
        index_block_options(opt),
        file(f),
        offset(0),
        data_block(&options),
        index_block(&index_block_options),
        num_entries(0),
        closed(false),
        filter_block(opt.filter_policy == nullptr
                         ? nullptr
                         : new FilterBlockBuilder(opt.filter_policy)),
        pending_index_entry(false) {
    index_block_options.block_restart_interval = 1;
  }

  Options options;
  Options index_block_options;
  WritableFile* file;
  uint64_t offset;
  Status status;
  BlockBuilder data_block;
  BlockBuilder index_block;
  std::string last_key;
  int64_t num_entries;
  bool closed;  // Either Finish() or Abandon() has been called.
  FilterBlockBuilder* filter_block;

  // We do not emit the index entry for a block until we have seen the
  // first key for the next data block.  This allows us to use shorter
  // keys in the index block.  [For example, consider a block boundary
  // between the keys "the quick brown fox" and "the who".  We can use
  // "the r" as the key for the index block entry since it is >= all
  // entries in the first block and < all entries in subsequent
  // blocks.]
  //
  // Invariant: r->pending_index_entry is true only if data_block is empty.
  bool pending_index_entry;//只有data_block是空的时候，这个项才为true
  BlockHandle pending_handle;  // Handle to add to index block

  std::string compressed_output;
};

TableBuilder::TableBuilder(const Options& options, WritableFile* file)
    : rep_(new Rep(options, file)) {
  if (rep_->filter_block != nullptr) {
    rep_->filter_block->StartBlock(0);
  }
}

TableBuilder::~TableBuilder() {
  assert(rep_->closed);  // Catch errors where caller forgot to call Finish()
  delete rep_->filter_block;
  delete rep_;
}

Status TableBuilder::ChangeOptions(const Options& options) {
  // Note: if more fields are added to Options, update
  // this function to catch changes that should not be allowed to
  // change in the middle of building a Table.
  if (options.comparator != rep_->options.comparator) {
    return Status::InvalidArgument("changing comparator while building table");
  }

  // Note that any live BlockBuilders point to rep_->options and therefore
  // will automatically pick up the updated options.
  rep_->options = options;
  rep_->index_block_options = options;
  rep_->index_block_options.block_restart_interval = 1;
  return Status::OK();
}

//rep保存构造table所需要的所有信息
void TableBuilder::Add(const Slice& key, const Slice& value) {//插入kv对
  Rep* r = rep_;
  assert(!r->closed);
  if (!ok()) return;
  if (r->num_entries > 0) {
    //必须比已经存在的key大，才能追加在后面
    assert(r->options.comparator->Compare(key, Slice(r->last_key)) > 0);
  }

  //true：说明data block是空的（一块新的待写的data block）
  if (r->pending_index_entry) {
    assert(r->data_block.empty());
    //找一个介于r->last_key和待插入的key之间的一个较短key，节省一些空间
    r->options.comparator->FindShortestSeparator(&r->last_key, key);
    std::string handle_encoding;
    //handle：某个块的偏移量和大小，写入handle_encoding
    r->pending_handle.EncodeTo(&handle_encoding);
    //data block的handle是写入index block
    //格式：某个data block的最后一个key，handle
    //index block的内容也是kv对的形式
    r->index_block.Add(r->last_key, Slice(handle_encoding));
    r->pending_index_entry = false;//对上一个块的处理已经完成
  }

  if (r->filter_block != nullptr) {
    r->filter_block->AddKey(key);//插入filter_block
  }

  r->last_key.assign(key.data(), key.size());//重置last_key为当前key
  r->num_entries++;
  r->data_block.Add(key, value);//插入data_block

  //预估当前block的大小
  const size_t estimated_block_size = r->data_block.CurrentSizeEstimate();
  //如果超出了配置的大小，换新块
  if (estimated_block_size >= r->options.block_size) {
    Flush();
  }
}

//Add(当抄出配置的块大小时)->Flush()->WriteBlock()->WriteRawBlock()
//handle在WriteRawBlock()的时候才被调用
//作用：调用WriteBlock
//设置pending_index_entry以记录新块的开始
//向构建filter的函数传入本块的末偏移量，一次性构建本块相关的所有filter
void TableBuilder::Flush() {
  Rep* r = rep_;
  assert(!r->closed);
  if (!ok()) return;
  if (r->data_block.empty()) return;
  assert(!r->pending_index_entry);
  WriteBlock(&r->data_block, &r->pending_handle);//对旧块收尾
  if (ok()) {
    r->pending_index_entry = true;//意味着即将写新块
    r->status = r->file->Flush();
  }
  if (r->filter_block != nullptr) {
    r->filter_block->StartBlock(r->offset);//一个块结束，传入这个块的末偏移量，构建filter
  }
}

//作用：对旧块的收尾，添加type+crc（数据部分收尾：Finish()）,记录待写的块数据内容
//Write：把数据写入文件
void TableBuilder::WriteBlock(BlockBuilder* block, BlockHandle* handle) {
  // File format contains a sequence of blocks where each block has:
  //    block_data: uint8[n]
  //    type: uint8
  //    crc: uint32
  assert(ok());
  Rep* r = rep_;
  Slice raw = block->Finish();//一个块结束，收尾

  Slice block_contents;
  CompressionType type = r->options.compression;//是否压缩
  // TODO(postrelease): Support more compression options: zlib?
  switch (type) {
    case kNoCompression:
      block_contents = raw;//不压缩
      break;

    case kSnappyCompression: {
      std::string* compressed = &r->compressed_output;
      if (port::Snappy_Compress(raw.data(), raw.size(), compressed) &&
          compressed->size() < raw.size() - (raw.size() / 8u)) {
        block_contents = *compressed;
      } else {//压缩失败，存储的还是没压缩的
        // Snappy not supported, or compressed less than 12.5%, so just
        // store uncompressed form
        block_contents = raw;
        type = kNoCompression;
      }
      break;
    }
  }
  //handle是这个块的信息
  WriteRawBlock(block_contents, type, handle);
  r->compressed_output.clear();
  block->Reset();//清空块，意味着一个新块
}

void TableBuilder::WriteRawBlock(const Slice& block_contents,
                                 CompressionType type, BlockHandle* handle) {
  Rep* r = rep_;
  //记录handle
  handle->set_offset(r->offset);
  handle->set_size(block_contents.size());
  //追加写入文件（某个sstable文件），写数据部分
  r->status = r->file->Append(block_contents);
  if (r->status.ok()) {
    char trailer[kBlockTrailerSize];//type+crc
    trailer[0] = type;
    uint32_t crc = crc32c::Value(block_contents.data(), block_contents.size());
    crc = crc32c::Extend(crc, trailer, 1);  // Extend crc to cover block type
    EncodeFixed32(trailer + 1, crc32c::Mask(crc));
    r->status = r->file->Append(Slice(trailer, kBlockTrailerSize));//写type+crc部分
    if (r->status.ok()) {
      r->offset += block_contents.size() + kBlockTrailerSize;//更新偏移量（在某个sstable中？）
    }
  }
}

Status TableBuilder::status() const { return rep_->status; }

Status TableBuilder::Finish() {
  //data block的部分写完之后调用
  Rep* r = rep_;
  Flush();//最后一块落盘
  assert(!r->closed);
  r->closed = true;

  BlockHandle filter_block_handle, metaindex_block_handle, index_block_handle;
  //其中filter_block_handle保存在metaindex_block_handle中
  //metaindex_block_handle和index_block_handle存放在footer中

  // Write filter block
  if (ok() && r->filter_block != nullptr) {
    //写filter block
    WriteRawBlock(r->filter_block->Finish(), kNoCompression,
                  &filter_block_handle);
  }

  // Write metaindex block
  if (ok()) {
    BlockBuilder meta_index_block(&r->options);
    if (r->filter_block != nullptr) {
      // Add mapping from "filter.Name" to location of filter data
      //metaindex block保存：key——某个meta block的名字，value：这个meta block的handle
      std::string key = "filter.";
      //key："filter.name"
      key.append(r->options.filter_policy->Name());
      std::string handle_encoding;
      //把filter block的handle写进meta_index_block
      filter_block_handle.EncodeTo(&handle_encoding);
      meta_index_block.Add(key, handle_encoding);
    }

    // TODO(postrelease): Add stats and other meta blocks
    //meta block写入文件
    WriteBlock(&meta_index_block, &metaindex_block_handle);
  }

  // Write index block
  //本来对pending_index_entry的判断是在Add函数中的，而这个函数并没有调用Add
  //所以需要对最后一个块再特别处理
  if (ok()) {
    if (r->pending_index_entry) {//true 最后一块data block
      r->options.comparator->FindShortSuccessor(&r->last_key);
      std::string handle_encoding;
      r->pending_handle.EncodeTo(&handle_encoding);
      r->index_block.Add(r->last_key, Slice(handle_encoding));
      r->pending_index_entry = false;
    }
    //index_block写文件
    WriteBlock(&r->index_block, &index_block_handle);
  }

  // Write footer
  if (ok()) {
    Footer footer;
    //把metaindex block和index block的handle都传给footer
    footer.set_metaindex_handle(metaindex_block_handle);
    footer.set_index_handle(index_block_handle);
    std::string footer_encoding;
    footer.EncodeTo(&footer_encoding);
    //追加到文件末尾
    r->status = r->file->Append(footer_encoding);
    if (r->status.ok()) {
      //更新文件的指针
      r->offset += footer_encoding.size();
    }
  }
  return r->status;
}

void TableBuilder::Abandon() {
  Rep* r = rep_;
  assert(!r->closed);
  r->closed = true;
}

uint64_t TableBuilder::NumEntries() const { return rep_->num_entries; }

uint64_t TableBuilder::FileSize() const { return rep_->offset; }

}  // namespace leveldb
