// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_VERSION_EDIT_H_
#define STORAGE_LEVELDB_DB_VERSION_EDIT_H_

#include <set>
#include <utility>
#include <vector>

#include "db/run_manager.h"
#include "db/dbformat.h"

namespace leveldb {

class VersionSet;

struct FileMetaData {
  FileMetaData() : refs(0), allowed_seeks(1 << 30), file_size(0) {}

  int refs;//引用计数
  int allowed_seeks;  // Seeks allowed until compaction
  uint64_t number;
  uint64_t file_size;    // File size in bytes
  InternalKey smallest;  // Smallest internal key served by table
  InternalKey largest;   // Largest internal key served by table
};

class SortedRun{//结构1
 public:
  SortedRun(uint64_t id = 0, int level = 0):id_(id),
                              level_(level),
                              ref_(0),
                              contain_file_(new std::vector<FileMetaData*>),
                              run_to_L0_file_(new std::vector<uint64_t>){}

  void InsertContainFile(FileMetaData* file){
    contain_file_->push_back(file);
  }

  void InsertL0File(uint64_t file){
    run_to_L0_file_->push_back(file);
  }

  void SetLevel(int level){
    level_ = level;
  }

  void SetId(uint64_t id){
    id_ = id;
  }

  int GetLevel() const{
    return level_;
  }

  uint64_t GetID() const{
    return id_;
  }

  std::vector<FileMetaData*>* GetContainFile() const{
    return contain_file_;
  }

  std::vector<uint64_t>* GetRunToL0() const{
    return run_to_L0_file_;
  }

  int ref_;

 protected:
  uint64_t id_;
  int level_;
  std::vector<FileMetaData*>* contain_file_;
  std::vector<uint64_t>* run_to_L0_file_;
};

class VersionEdit {
 public:
  VersionEdit() { Clear(); }
  ~VersionEdit() = default;

  void Clear();

  void SetComparatorName(const Slice& name) {
    has_comparator_ = true;
    comparator_ = name.ToString();
  }
  void SetLogNumber(uint64_t num) {
    has_log_number_ = true;
    log_number_ = num;
  }
  void SetPrevLogNumber(uint64_t num) {
    has_prev_log_number_ = true;
    prev_log_number_ = num;
  }
  void SetNextFile(uint64_t num) {
    has_next_file_number_ = true;
    next_file_number_ = num;
  }
  void SetLastSequence(SequenceNumber seq) {
    has_last_sequence_ = true;
    last_sequence_ = seq;
  }
  /*void SetCompactPointer(int level, const InternalKey& key) {
    compact_pointers_.push_back(std::make_pair(level, key));
  }*/

  // Add the specified file at the specified number.
  // REQUIRES: This version has not been saved (see VersionSet::SaveTo)
  // REQUIRES: "smallest" and "largest" are smallest and largest keys in file
  /*void AddFile(int level, uint64_t file, uint64_t file_size,
               const InternalKey& smallest, const InternalKey& largest) {
    FileMetaData f;
    f.number = file;
    f.file_size = file_size;
    f.smallest = smallest;
    f.largest = largest;
    new_files_.push_back(std::make_pair(level, f));
  }*/

  void AddFileToRun(uint64_t file, uint64_t file_size,
               const InternalKey& smallest, const InternalKey& largest){
    FileMetaData f;
    f.number = file;
    f.file_size = file_size;
    f.smallest = smallest;
    f.largest = largest;
    FileMetaData* f_p = new FileMetaData(f);
    //f_p->refs = 1;
    new_run_.InsertContainFile(f_p);
  }

  void AddRun(SortedRun run){
    new_run_ = run;
  }
  // Delete the specified "file" from the specified "level".
  /*void RemoveFile(int level, uint64_t file) {
    deleted_files_.insert(std::make_pair(level, file));
  }*/

  void AddSnapshotRun(SortedRun& run){
    snapshot_runs_.push_back(run);
  }

  void RemoveRun(SortedRun* run) {
    deleted_map_.insert(std::make_pair(run->GetID(), *(run->GetRunToL0())));
  }

  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(const Slice& src);
  void EncodeRun(std::string* dst, const SortedRun& run) const;
  bool DecodeRun(Slice* input, SortedRun* run);

  std::string DebugString() const;

  void SetLevel(int input_level, int output_level){
    input_level_ = input_level;
    output_level_ = output_level;
  }

  int GetInputLevel() const{
    return input_level_;
  }

  int GetOutputLevel() const{
    return output_level_;
  }

 private:
  friend class VersionSet;

  //typedef std::set<std::pair<int, uint64_t>> DeletedFileSet;
  //typedef std::set<uint64_t> DeletedRunSet;
  typedef std::vector<SortedRun> SnapShotRunSet;
  typedef std::unordered_map<uint64_t, std::vector<uint64_t>> DeletedMap;

  std::string comparator_;
  uint64_t log_number_;
  uint64_t prev_log_number_;
  uint64_t next_file_number_;
  SequenceNumber last_sequence_;
  bool has_comparator_;
  bool has_log_number_;
  bool has_prev_log_number_;
  bool has_next_file_number_;
  bool has_last_sequence_;
  SnapShotRunSet snapshot_runs_;

  //std::vector<std::pair<int, InternalKey>> compact_pointers_;
  //DeletedFileSet deleted_files_;
  //DeletedRunSet deleted_runs_;
  uint32_t input_level_;
  uint32_t output_level_;
  DeletedMap deleted_map_;
  //<level,file meta>
  //std::vector<FileMetaData> new_files_;
  SortedRun new_run_;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_VERSION_EDIT_H_
