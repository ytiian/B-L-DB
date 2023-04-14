#ifndef STORAGE_LEVELDB_DB_RUN_MANAGER_H_
#define STORAGE_LEVELDB_DB_RUN_MANAGER_H_

#include <unordered_map>
#include <vector>
#include <utility>
#include <iostream>
#include "version_edit.h"

namespace leveldb{

/*class SortedRun{//结构1
 public:
  SortedRun(){}

  SortedRun(uint64_t id, int level):id_(id),
                              level_(level),
                              next_(nullptr),
                              ref_(0),
                              contain_file_(new std::vector<FileMetaData*>),
                              run_to_L0_file_(new std::vector<uint64_t>){}

  void InsertContainFile(FileMetaData* file){
    contain_file_->push_back(file);
  }

  void InsertL0File(uint64_t file){
    run_to_L0_file_->push_back(file);
  }

  void UpdateNext(SortedRun* new_run){
    next_ = new_run;
  }

  SortedRun* GetNext(){
    return next_;
  }

  int GetLevel() const{
    return level_;
  }

  uint64_t GetID() const{
    return id_;
  }

  std::vector<FileMetaData*>* GetContainFile(){
    return contain_file_;
  }

  std::vector<uint64_t>* GetRunToL0() {
    return run_to_L0_file_;
  }

  int ref_;

 protected:
  uint64_t id_;
  int level_;
  std::vector<FileMetaData*>* contain_file_;
  std::vector<uint64_t>* run_to_L0_file_;
  SortedRun* next_;//结构3
};*/

/*class RunManager{
 public:
  RunManager():next_run_number_(1){}

  //外部调用
  //SortedRun* final_run
  //FindFinalRun(L0_file, final_run)
  bool FindFinalRun(FileMetaData* L0_file, SortedRun** final_run){
    SortedRun* tmp_run = nullptr;
    std::unordered_map<FileMetaData*, SortedRun*>::iterator it = L0_file_to_run.find(L0_file);
    if(it == L0_file_to_run.end()){
      return false;//该L0文件还存在
    }else{
      tmp_run = it->second;
    }
    
    SortedRun* next = tmp_run->GetNext();
    while(next != nullptr){
      tmp_run = next;
      next = tmp_run->GetNext();
    }
    *final_run = tmp_run;
    return true;
  }

  void Arrange(){//整理 L0_file_to_run
    
  }

 protected:
  std::unordered_map<FileMetaData*, SortedRun*> L0_file_to_run;
  uint64_t next_run_number_;//下一个run分配的id
};*/

}


#endif