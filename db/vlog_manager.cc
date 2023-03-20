#include<vector>

#include "db/vlog_manager.h"
#include "db/GarbageCollection.h"

namespace leveldb{

//把一个新的vlog记录下来
void VlogManager::AddNewVlog(uint64_t number, uint64_t size){
  VlogMeta meta;
  meta.garbage_size_ = 0;
  meta.vlog_size_ = size;
  vlog_files_.insert(std::make_pair(number, meta));
}

//更新某个文件的垃圾量
void VlogManager::UpdateGarbageSize(uint64_t number, uint64_t update_size){
  VlogMeta meta = vlog_files_.at(number);
  meta.garbage_size_ += update_size;
}    

//检查是否有文件的垃圾量超出阈值，加入待gc队列
void VlogManager::UpdateNeedGCFile(){
  std::map<uint64_t, VlogMeta>::iterator iter ;
  for(iter = vlog_files_.begin(); iter != vlog_files_.end(); iter++ ){
    if(iter->second.garbage_size_ > iter->second.vlog_size_ * CleanThreshold ){
      need_clean_.push_back(iter->first);
    }
  }
}

//把所有待回收的文件都加入新结构gc中的队列
void VlogManager::PickGCFile(){
  GarbageCollection* gc;
  
  while(!need_clean_.empty()){
    uint64_t number = need_clean_.back();
    need_clean_.pop_back();
    gc->AddInput(number);
  }

}


}