#ifndef STORAGE_LEVELDB_DB_VLOG_WRITER_H_
#define STORAGE_LEVELDB_DB_VLOG_WRITER_H_

#include <stdint.h>
#include <string.h>
#include <cstdint>

#include "leveldb/slice.h"
#include "leveldb/status.h"
#include "leveldb/env.h"
#include "leveldb/options.h"
#include "db/version_set.h"
#include "db/log_writer.h"

namespace leveldb {

class WritableFile;

namespace log { 

class LEVELDB_EXPORT VlogBuilder {
 public:
  VlogBuilder(const std::string& dbname, 
              Env* env, const Options& options,
               uint64_t vlog_number);

  VlogBuilder(const VlogBuilder&) = delete;
  VlogBuilder& operator=(const VlogBuilder&) = delete;

  // REQUIRES: Either Finish() or Abandon() has been called.
  ~VlogBuilder();

  Status NewVlog();

  Slice AddRecord(const Slice& key, const Slice& value);

  Status Finish();

  uint64_t GetNumber();

  uint64_t GetSize();

 private:
  WritableFile* vlog_;//当前持有的vlog
  Options options_;
  std::string buffer_;//缓冲区
  uint64_t file_offset_;//当前在file的位置
  uint64_t next_offset_;
  uint64_t number_;//文件号
  //static const size_t kVlogTrailerSize = 4;
  uint64_t file_size_;//记录文件有多少字节数
  //uint64_t entry_count_;//当前file有多少个条目
  Writer* vlog_writer_;
  std::string dbname_;
  Env* env_;
};

}
}

#endif 