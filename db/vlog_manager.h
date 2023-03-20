#ifndef STORAGE_LEVELDB_DB_VLOG_MANAGER_H_
#define STORAGE_LEVELDB_DB_VLOG_MANAGER_H_

#include<map>
#include<vector>

namespace leveldb{

class VlogManager{
 public:
  struct VlogMeta{
    //VlogReader* reader_;
    uint64_t garbage_size_;//待回收字节数
    uint64_t vlog_size_;//总大小
  };
  
  void AddNewVlog(uint64_t number, uint64_t size);

  void UpdateGarbageSize(uint64_t number, uint64_t update_size);

  void UpdateNeedGCFile();

  void PickGCFile();

  //记录每个文件对应的信息（总大小，回收量）
  std::map<uint64_t, VlogMeta> vlog_files_;
  //需要gc的vlog
  std::vector<uint64_t> need_clean_;

  static const double CleanThreshold;

};

const double VlogManager::CleanThreshold = 0.7;


};

#endif