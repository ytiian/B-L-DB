#include<vector>

#include "GarbageCollection.h"
#include "leveldb/status.h"
#include "leveldb/iterator.h"
#include "table/merger.h"
#include "db/dbformat.h"
#include "db/filename.h"
#include "db/db_impl.h"
#include "db/log_reader.h"
#include "util/coding.h"
#include "leveldb/slice.h"

namespace leveldb{

//待修改
class CorruptionReporter : public log::Reader::Reporter {
 public:
  void Corruption(size_t bytes, const Status& status) override {
    std::string r = "corruption: ";
    AppendNumberTo(&r, bytes);
    r += " bytes; ";
    r += status.ToString();
    r.push_back('\n');
    dst_->Append(r);
  }

  WritableFile* dst_;
};

void GarbageCollection::AddInput(uint64_t number){
  input_.push_back(number);  
}

Status GarbageCollection::DoGCWork(Env* env){
  //Log(options_.info_log, "GC %d + %d + %d files",
    //input_.at(0),input_.at(1),input_.at(2));

  std::vector<uint64_t>::iterator iter;
  for(iter = input_.begin(); iter != input_.end(); iter++){
    SequentialFile* vlog;//对于队列中的每个vlog
    std::string fname = VlogFileName(db_->GetName(), *iter);
    Status s = env->NewSequentialFile(fname, &vlog);
    if (!s.ok()) {
      return s;
    }

    //待修改
    CorruptionReporter reporter;
    WritableFile* dst;
    reporter.dst_ = dst;
    log::Reader reader(vlog, &reporter, true, 0);
    
    //开始逐条解析record
    Slice record;
    std::string scratch;
    while (reader.ReadRecord(&record, &scratch)){
      Slice key;
      Slice value;
      ParseFromRecord(&record, &key, &value);
      
      ParsedInternalKey ikey;//存放解析后的internalkey
      if(!ParseInternalKey(key, &ikey)){
      //出错
      }else{
        uint64_t get_number;
        uint64_t get_offset;
        Slice user_key = ikey.user_key;
        db_->GetPtr(read_option_, user_key, &get_number, &get_offset);
        if((get_number == *iter) && (get_offset == reader.GetRecordOffset())){//待修改
          db_->Put(write_option_, user_key, value);
        }
      }
    }
    //删除文件
    //是否要增加vlog的引用计数结构？
  }
}

void GarbageCollection::ParseFromRecord(Slice* record, Slice* key, Slice* value){
  Slice key;
  Slice value;
  uint64_t number;
  uint64_t offset;
  uint32_t key_size;
  uint32_t value_size;
  GetVarint32(record, &key_size);
  key = new Slice(record->data(), key_size);
  record->remove_prefix(key_size);
  GetVarint32(record, &value_size);
  value = new Slice(record->data(), value_size);

}

}