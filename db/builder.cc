// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/builder.h"

#include "db/dbformat.h"
#include "db/filename.h"
#include "db/table_cache.h"
#include "db/version_edit.h"
#include "db/vlog_builder.h"
#include "db/vlog_manager.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"

namespace leveldb {

Status BuildTable(const std::string& dbname, Env* env, const Options& options,
                  TableCache* table_cache, Iterator* iter, FileMetaData* meta,
                  VlogManager* vlog_manager, uint64_t vlog_number) {
  Status s;
  meta->file_size = 0;
  iter->SeekToFirst();
  std::string fname = TableFileName(dbname, meta->number);
  //change:3.16新建vlogfile
  //warning:和key的file编号一致会不会有问题？
  //std::string vname = VlogFileName(dbname, meta->number);
  if (iter->Valid()) {
    WritableFile* file;
    //WritableFile* vlog;
    s = env->NewWritableFile(fname, &file);
    if (!s.ok()) {
      return s;
    }
    //s = env->NewWritableFile(vname, &vlog);
    //if (!s.ok()) {
    //  return s;
    //}

    TableBuilder* builder = new TableBuilder(options, file);
    log::VlogBuilder* vlog_builder = new log::VlogBuilder(dbname, env, options, vlog_number);
    s = vlog_builder->NewVlog();
    
    meta->smallest.DecodeFrom(iter->key());
    Slice key;
    for (; iter->Valid(); iter->Next()) {
      key = iter->key();
      //change:3.16
      Slice address;
      address = vlog_builder->AddRecord(key, iter->value());
      builder->Add(key, address);
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
    s = vlog_builder->Finish();
    if (s.ok()){
      uint64_t number = vlog_builder->GetNumber();
      uint64_t file_size = vlog_builder->GetSize();
      vlog_manager->AddNewVlog(number, file_size);
    }
    delete builder;
    delete vlog_builder;

    // Finish and check for file errors
    if (s.ok()) {
      s = file->Sync();
    }
    if (s.ok()) {
      s = file->Close();
    }
    delete file;
    file = nullptr;

    //if (s.ok()) {
    //  s = vlog->Sync();
    //}
    //if (s.ok()) {
    //  s = vlog->Close();
    //}
    //delete vlog;
    //vlog = nullptr;

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
