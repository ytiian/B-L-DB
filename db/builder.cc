// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <iostream>
#include <string>
#include <string>
#include "db/builder.h"

#include "db/dbformat.h"
#include "db/filename.h"
#include "db/table_cache.h"
#include "db/version_edit.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "sindex/sindex_wrapper.h"

namespace leveldb {

Status BuildTable(const std::string& dbname, Env* env, const Options& options,
                  TableCache* table_cache, Iterator* iter, FileMetaData* meta,
                  SIndexWrapper* sindex) {
  Status s;
  meta->file_size = 0;
  iter->SeekToFirst();

  std::string fname = TableFileName(dbname, meta->number);
  if (iter->Valid()) {
    WritableFile* file;
    s = env->NewWritableFile(fname, &file);
    if (!s.ok()) {
      return s;
    }

    TableBuilder* builder = new TableBuilder(options, file);
    meta->smallest.DecodeFrom(iter->key());
    Slice key;

    std::vector<index_key_t> index_keys;

    std::string usr_key = "";
    std::string last_key = "";

    for (; iter->Valid(); iter->Next()) {
      //std::cout<<"before!!!!!!!!insert1:!!!!!!!!!!!!!!!!!!"<<std::endl;
      //std::cout<<skip_index->toString()<<std::endl;
      key = iter->key();
      //返回的是internalkey，需要减掉8bits的tag（internalkey=userkey+tag）
      Slice tmp(key.data(), key.size() - 8);
      last_key = usr_key;
      usr_key = tmp.ToString();

      builder->Add(key, iter->value());
      if(sindex->isNull()){
        if(usr_key != last_key){
          index_keys.emplace_back(usr_key);
        }
      }else{
        sindex->Insert(index_key_t(usr_key), meta->number, (uint32_t)WorkerType::FLUSH);
        //std::cout<<usr_key<<" "<<meta->number<<std::endl;
      }
      
    }

    if(sindex->isNull()){
      sindex->BuildSIndex(index_keys, config::worker_num, config::bg_n, meta->number);
    }
    

    if (!key.empty()) {
      meta->largest.DecodeFrom(key);
    }

    // Finish and check for builder errors
    s = builder->Finish();
    if (s.ok()) {
      meta->file_size = builder->FileSize();
      assert(meta->file_size > 0);
    }
    delete builder;

    // Finish and check for file errors
    if (s.ok()) {
      s = file->Sync();
    }
    if (s.ok()) {
      s = file->Close();
    }
    delete file;
    file = nullptr;

    if (s.ok()) {
      // Verify that the table is usable
      Iterator* it = table_cache->NewIterator(ReadOptions(), meta->number,
                                              meta->file_size);
      s = it->status();
      delete it;
    }
  }

  // Check for input iterator errors
  if (!iter->status().ok()) {
    s = iter->status();
  }

  if (s.ok() && meta->file_size > 0) {
    // Keep it
  } else {
    env->RemoveFile(fname);
  }
  return s;
}

}  // namespace leveldb
