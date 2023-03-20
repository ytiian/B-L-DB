#include "leveldb/env.h"
#include "util/coding.h"
#include "util/crc32c.h"
#include "db/filename.h"
#include "db/version_set.h"
#include "db/vlog_builder.h"

namespace leveldb {
namespace log {    

//待补充
VlogBuilder::VlogBuilder(const std::string& dbname, 
                        Env* env, const Options& options,
                        uint64_t vlog_number)
  :vlog_(nullptr),
  options_(options),
  file_offset_(0),
  next_offset_(0),
  number_(vlog_number),
  file_size_(0),
  vlog_writer_(nullptr),
  dbname_(dbname),
  env_(env)
  {}

//设置vlog_和对应的vlog_writer_初始化
Status VlogBuilder::NewVlog(){
  assert(number_>0);
  std::string vname = VlogFileName(dbname_, number_);
  Log(options_.info_log, "New vlog #%llu: started",
      (unsigned long long)number_);  
  WritableFile* vlog;
  Status s = env_->NewWritableFile(vname, &vlog);
  if (s.ok()) {
    vlog_=vlog;
    vlog_writer_=new Writer(vlog_);
  }
  return s;
}

//
Slice VlogBuilder::AddRecord(const Slice& key, const Slice& value)
{
  //每条数据的格式为[key的长度（定长）][key的内容][value的长度（定长）][value的内容]
  PutVarint32(&buffer_, key.size());
  buffer_.append(key.data(), key.size());
  PutVarint32(&buffer_, value.size());
  buffer_.append(value.data(), value.size());
  Status s = vlog_writer_->AddRecord(buffer_);
  if(s.ok()){//记录文件存的KV对总大小，用于统计gc信息
    file_size_ += key.size();
    file_size_ += value.size();
  }
  buffer_.clear();
  file_offset_=vlog_writer_->GetOffset(); 
  std::string address_;
  PutVarint64(&address_, number_);
  PutVarint64(&address_, file_offset_);
  //指针内容(no,offset)
  //需要加入长度吗？
  //没加：因为从offset可以先读到keysize，从而获得key。
  //再读valuesize，可以获得value
  return Slice(address_);
}

//构造完成了一个vlog文件
Status VlogBuilder::Finish(){
  Status s = vlog_->Sync();
  if(s.ok()){
    vlog_->Close();
  }
  if(s.ok()){
    delete vlog_;
    vlog_=nullptr;   
  }
  return s;
}

uint64_t VlogBuilder::GetNumber(){
  return number_;
}

uint64_t VlogBuilder::GetSize(){
  return file_size_;
}

}//namespace log
}//namespace leveldb