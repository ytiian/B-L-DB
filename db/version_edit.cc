// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//3.1 ✔
//在对VersionEdit调用AddFile、Set等函数时
//对象内部的信息会被修改
//这之后EncodeTo，将信息平铺成string
//DecodeFrom，从string中解析信息

#include "db/version_edit.h"

#include "db/version_set.h"
#include "util/coding.h"
#include <iostream>

namespace leveldb {

// Tag numbers for serialized VersionEdit.  These numbers are written to
// disk and should not be changed.
enum Tag {
  kComparator = 1,
  kLogNumber = 2,
  kNextFileNumber = 3,
  kLastSequence = 4,
  kCompactPointer = 5,
  kDeletedFile = 6,
  kNewFile = 7,
  // 8 was used for large value refs
  kPrevLogNumber = 9,
  kDeletedRun = 10,
  kSnapShotRun = 11,
  kNewRun = 12,
  kInputLevel = 13,
  kOutputLevel = 14,
  kDeletedMap = 15
};

//Version记录db中的所有文件
//包括：sstable文件，log文件，manifest文件，current文件，lock文件..
void VersionEdit::Clear() {
  comparator_.clear();
  log_number_ = 0;
  prev_log_number_ = 0;
  last_sequence_ = 0;
  next_file_number_ = 0;
  has_comparator_ = false;
  has_log_number_ = false;
  has_prev_log_number_ = false;
  has_next_file_number_ = false;
  has_last_sequence_ = false;
  //compact_pointers_.clear();
  //deleted_files_.clear();
  //new_files_.clear();
  deleted_map_.clear();
  //deleted_runs_.clear();
  snapshot_runs_.clear();
  input_level_ = -1;
  output_level_ = -1;
}

//VersionEdit记录的所有信息
void VersionEdit::EncodeTo(std::string* dst) const {
  if (has_comparator_) {
    PutVarint32(dst, kComparator);
    PutLengthPrefixedSlice(dst, comparator_);
  }
  if (has_log_number_) {
    PutVarint32(dst, kLogNumber);
    PutVarint64(dst, log_number_);
  }
  if (has_prev_log_number_) {
    PutVarint32(dst, kPrevLogNumber);
    PutVarint64(dst, prev_log_number_);
  }
  if (has_next_file_number_) {
    PutVarint32(dst, kNextFileNumber);
    PutVarint64(dst, next_file_number_);
  }
  if (has_last_sequence_) {
    PutVarint32(dst, kLastSequence);
    PutVarint64(dst, last_sequence_);
  }

  if(snapshot_runs_.empty() && output_level_ != -1){
    PutVarint32(dst, kInputLevel);
    PutVarint32(dst, input_level_);
    PutVarint32(dst, kOutputLevel);
    PutVarint32(dst, output_level_);

    /*for(const auto& deleted_run_kvp : deleted_runs_) {
      PutVarint32(dst, kDeletedFile);
      PutVarint64(dst, deleted_run_kvp);  // file number
    }*/

    for(const auto& deleted_map_kvp : deleted_map_){
      PutVarint32(dst, kDeletedMap);
      PutVarint64(dst, deleted_map_kvp.first);
      PutVarint32(dst, deleted_map_kvp.second.size());
      const auto& number = deleted_map_kvp.second;
      for(const auto& n : number){
        PutVarint64(dst, n);
      }
    }

    PutVarint32(dst, kNewRun);
    EncodeRun(dst, new_run_);

  }else{
    for(size_t i = 0; i < snapshot_runs_.size(); i++){
      PutVarint32(dst, kSnapShotRun);
      const SortedRun& run = snapshot_runs_[i];
      EncodeRun(dst, run);
    }
  }

  //？compaction pointer？
  /*for (size_t i = 0; i < compact_pointers_.size(); i++) {
    PutVarint32(dst, kCompactPointer);
    PutVarint32(dst, compact_pointers_[i].first);  // level
    PutLengthPrefixedSlice(dst, compact_pointers_[i].second.Encode());
  }*/

  /*for (const auto& deleted_file_kvp : deleted_files_) {
    PutVarint32(dst, kDeletedFile);
    PutVarint32(dst, deleted_file_kvp.first);   // level
    PutVarint64(dst, deleted_file_kvp.second);  // file number
  }*/

  /*for (size_t i = 0; i < new_files_.size(); i++) {
    const FileMetaData& f = new_files_[i].second;
    PutVarint32(dst, kNewFile);
    PutVarint32(dst, new_files_[i].first);  // level
    PutVarint64(dst, f.number);
    PutVarint64(dst, f.file_size);
    PutLengthPrefixedSlice(dst, f.smallest.Encode());
    PutLengthPrefixedSlice(dst, f.largest.Encode());
  }*/
}

//从input中解析得到InternalKey
static bool GetInternalKey(Slice* input, InternalKey* dst) {
  Slice str;
  if (GetLengthPrefixedSlice(input, &str)) {
    return dst->DecodeFrom(str);
  } else {
    return false;
  }
}

//从input中解析得到level
static bool GetLevel(Slice* input, int* level) {
  uint32_t v;
  if (GetVarint32(input, &v) && v < config::kNumLevels) {
    *level = v;
    return true;
  } else {
    return false;
  }
}

void VersionEdit::EncodeRun(std::string* dst, const SortedRun& run) const{
  std::vector<FileMetaData*>* files = run.GetContainFile();
  PutVarint64(dst, run.GetID());
  PutVarint32(dst, run.GetLevel());
  PutVarint32(dst, files->size());
  //std::cout<<"file->size():"<<files->size()<<std::endl;
  for(size_t j = 0; j < files->size(); j++){
    const FileMetaData& f = *(files->at(j));
    PutVarint32(dst, run.GetLevel());  // level
    PutVarint64(dst, f.number);
    PutVarint64(dst, f.file_size);
    PutLengthPrefixedSlice(dst, f.smallest.Encode());
    PutLengthPrefixedSlice(dst, f.largest.Encode());        
  }
  std::vector<uint64_t>* L0 = run.GetRunToL0();
  PutVarint32(dst, L0->size());
  //std::cout<<"L0->size()"<<L0->size()<<std::endl;
  for(size_t j = 0; j < L0->size(); j++){
    PutVarint64(dst, L0->at(j));
  } 
}

bool VersionEdit::DecodeRun(Slice* input, SortedRun* run){
  const char* msg = nullptr;
  uint64_t id;
  if(!GetVarint64(input, &id)){
    std::cout<<"id wrong"<<std::endl;
  }
  run->SetId(id);
  int level;
  if(!GetLevel(input, &level)){
    std::cout<<"level wrong"<<std::endl;    
  }
  run->SetLevel(level);
  uint32_t file_num;//包含的文件数
  if(!GetVarint32(input, &file_num)){
    std::cout<<"file num wrong"<<std::endl;     
  }
  int file_level;
  FileMetaData f;
  //std::cout<<"file_num:"<<file_num<<std::endl;
  for(size_t i = 0; i < file_num; i++){
    if (GetLevel(input, &file_level) && GetVarint64(input, &f.number) &&
        GetVarint64(input, &f.file_size) &&
        GetInternalKey(input, &f.smallest) &&
        GetInternalKey(input, &f.largest)) {
        FileMetaData* file = new FileMetaData(f);
        run->InsertContainFile(file);
    } else {
      msg = "new-file entry";
      std::cout<<msg<<std::endl;
    }
    if(msg != nullptr){
      return false;
    }
  } 
  uint32_t L0_size;
  uint64_t L0_id;
  GetVarint32(input, &L0_size);
  //std::cout<<"L0_size:"<<L0_size<<std::endl;
  for(size_t i = 0; i < L0_size; i++){
    GetVarint64(input, &L0_id);
    run->InsertL0File(L0_id);
  }
  return true;
}

//和EncodeTo相对应，从src中解析
Status VersionEdit::DecodeFrom(const Slice& src) {
  Clear();
  Slice input = src;
  const char* msg = nullptr;
  uint32_t tag;

  // Temporary storage for parsing
  int level;
  uint64_t number;
  FileMetaData f;
  Slice str;
  InternalKey key;
  SortedRun r;
  bool flag;
  uint32_t file_number;
  uint64_t L0_number;
  std::vector<uint64_t> run_to_L0;

  while (msg == nullptr && GetVarint32(&input, &tag)) {
    //std::cout<<tag<<std::endl;
    switch (tag) {
      case kComparator:
        if (GetLengthPrefixedSlice(&input, &str)) {
          comparator_ = str.ToString();
          has_comparator_ = true;
        } else {//出错
          msg = "comparator name";
        }
        break;

      case kLogNumber:
        if (GetVarint64(&input, &log_number_)) {
          has_log_number_ = true;
        } else {//出错
          msg = "log number";
        }
        break;

      case kPrevLogNumber:
        if (GetVarint64(&input, &prev_log_number_)) {
          has_prev_log_number_ = true;
        } else {
          msg = "previous log number";
        }
        break;

      case kNextFileNumber:
        if (GetVarint64(&input, &next_file_number_)) {
          has_next_file_number_ = true;
        } else {
          msg = "next file number";
        }
        break;

      case kLastSequence:
        if (GetVarint64(&input, &last_sequence_)) {
          has_last_sequence_ = true;
        } else {
          msg = "last sequence number";
        }
        break;

      case kInputLevel:
        if(GetVarint32(&input, &input_level_)){
          
        }else{
          msg = "input level";
        }
        break;
      
      case kOutputLevel:
        if(GetVarint32(&input, &output_level_)){

        }else{
          msg = "output level";
        }
        break;

      case kSnapShotRun:
        r.clear();
        if(DecodeRun(&input, &r)){
          snapshot_runs_.push_back(r);
        }else{
          msg = "snapshot runs";
        }
        /*for(int i=0;i<snapshot_runs_.size();i++){
          std::cout<<"run:"<<snapshot_runs_[i].GetID()<<std::endl;
          std::cout<<"files:"<<(snapshot_runs_[i].GetContainFile())->size()<<std::endl;
          std::cout<<"L0:"<<(snapshot_runs_[i].GetRunToL0())->size()<<std::endl;
        }*/
        break;

      /*case kDeletedRun:
        if(GetVarint64(&input, &number)){
          deleted_runs_.insert(number);
        }else{
          msg = "deleted runs";
        }
        break;*/

      case kDeletedMap:
        flag = true;
        run_to_L0.clear();
        if(GetVarint64(&input, &number) && GetVarint32(&input, &file_number)){
          for(int i = 0; i < file_number; i++){
            if(GetVarint64(&input, &L0_number)){
              run_to_L0.push_back(L0_number);
            }else{
              msg = "run to L0";
              flag = false;
              break;
            }
          }
          if(flag){
            deleted_map_.insert(std::make_pair(number, run_to_L0));
          }
        }else{
          msg = "deleted map";
        }
        break;

      case kNewRun:
        r.clear();
        if(DecodeRun(&input, &r)){
          new_run_ = r;
        }else{
          msg = "new run";
        }
        break;

      /*case kCompactPointer:
        if (GetLevel(&input, &level) && GetInternalKey(&input, &key)) {
          compact_pointers_.push_back(std::make_pair(level, key));
        } else {
          msg = "compaction pointer";
        }
        break;*/

      /*case kDeletedFile:
        if (GetLevel(&input, &level) && GetVarint64(&input, &number)) {
          deleted_files_.insert(std::make_pair(level, number));
        } else {
          msg = "deleted file";
        }
        break;*/

      /*case kNewFile:
        if (GetLevel(&input, &level) && GetVarint64(&input, &f.number) &&
            GetVarint64(&input, &f.file_size) &&
            GetInternalKey(&input, &f.smallest) &&
            GetInternalKey(&input, &f.largest)) {
          new_files_.push_back(std::make_pair(level, f));
        } else {
          msg = "new-file entry";
        }
        break;*/

      default:
        msg = "unknown tag";
        break;
    }
  }

  if (msg == nullptr && !input.empty()) {
    msg = "invalid tag";
  }

  Status result;
  if (msg != nullptr) {
    result = Status::Corruption("VersionEdit", msg);
  }
  return result;
}

std::string VersionEdit::DebugString() const {
  std::string r;
  r.append("VersionEdit {");
  if (has_comparator_) {
    r.append("\n  Comparator: ");
    r.append(comparator_);
  }
  if (has_log_number_) {
    r.append("\n  LogNumber: ");
    AppendNumberTo(&r, log_number_);
  }
  if (has_prev_log_number_) {
    r.append("\n  PrevLogNumber: ");
    AppendNumberTo(&r, prev_log_number_);
  }
  if (has_next_file_number_) {
    r.append("\n  NextFile: ");
    AppendNumberTo(&r, next_file_number_);
  }
  if (has_last_sequence_) {
    r.append("\n  LastSeq: ");
    AppendNumberTo(&r, last_sequence_);
  }
  /*for (size_t i = 0; i < compact_pointers_.size(); i++) {
    r.append("\n  CompactPointer: ");
    AppendNumberTo(&r, compact_pointers_[i].first);
    r.append(" ");
    r.append(compact_pointers_[i].second.DebugString());
  }*/
  /*for (const auto& deleted_files_kvp : deleted_files_) {
    r.append("\n  RemoveFile: ");
    AppendNumberTo(&r, deleted_files_kvp.first);
    r.append(" ");
    AppendNumberTo(&r, deleted_files_kvp.second);
  }*/
  /*for (size_t i = 0; i < new_files_.size(); i++) {
    const FileMetaData& f = new_files_[i].second;
    r.append("\n  AddFile: ");
    AppendNumberTo(&r, new_files_[i].first);
    r.append(" ");
    AppendNumberTo(&r, f.number);
    r.append(" ");
    AppendNumberTo(&r, f.file_size);
    r.append(" ");
    r.append(f.smallest.DebugString());
    r.append(" .. ");
    r.append(f.largest.DebugString());
  }*/
  r.append("\n}\n");
  return r;
}

}  // namespace leveldb
