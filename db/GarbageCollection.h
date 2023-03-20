#include<vector>

#include "leveldb/status.h"
#include "leveldb/options.h"
#include "db/vlog_manager.h"
#include "leveldb/write_batch.h"
#include "db/db_impl.h"

namespace leveldb{

class DBImpl;

class GarbageCollection{

 public:
  void AddInput(uint64_t number);
  
  Status DoGCWork(Env* env);

  void ParseFromRecord(Slice* record, 
                        Slice* key, Slice* value);

 private:
  std::vector<uint64_t> input_;
  DBImpl* db_;
  VlogManager* manager_;
  WriteBatch* batch_;
  ReadOptions read_option_;
  WriteOptions write_option_;
};


}