// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/version_edit.h"

#include "gtest/gtest.h"

namespace leveldb {

static void TestEncodeDecode(const VersionEdit& edit) {
  std::string encoded, encoded2;
  edit.EncodeTo(&encoded);
  VersionEdit parsed;
  Status s = parsed.DecodeFrom(encoded);
  ASSERT_TRUE(s.ok()) << s.ToString();
  parsed.EncodeTo(&encoded2);
  ASSERT_EQ(encoded, encoded2);
}

/*TEST(VersionEditTest, EncodeDecode) {
  static const uint64_t kBig = 1ull << 50;

  VersionEdit edit;
  for (int i = 0; i < 4; i++) {
    TestEncodeDecode(edit);
    edit.AddFile(3, kBig + 300 + i, kBig + 400 + i,
                 InternalKey("foo", kBig + 500 + i, kTypeValue),
                 InternalKey("zoo", kBig + 600 + i, kTypeDeletion));
    edit.RemoveFile(4, kBig + 700 + i);
    edit.SetCompactPointer(i, InternalKey("x", kBig + 900 + i, kTypeValue));
  }

  edit.SetComparatorName("foo");
  edit.SetLogNumber(kBig + 100);
  edit.SetNextFile(kBig + 200);
  edit.SetLastSequence(kBig + 1000);
  TestEncodeDecode(edit);
}*/

TEST(VersionEditTest, SnapshotEncodeDecode){
  static const uint64_t kBig = 1ull << 50;
  VersionEdit edit;
  SortedRun run111(111, 1);
  FileMetaData f;
  f.number = kBig + 300 + 1;
  f.file_size = kBig + 400 + 1;
  f.smallest = InternalKey("foo", kBig + 500 + 1, kTypeValue);
  f.largest = InternalKey("zoo", kBig + 600 + 1, kTypeDeletion);
  FileMetaData *fp = new FileMetaData(f);
  run111.InsertContainFile(fp);
  run111.InsertL0File(f.number);

  SortedRun run21(21, 2);
  f.number = kBig + 300 + 2;
  f.file_size = kBig + 400 + 2;
  f.smallest = InternalKey("foo", kBig + 500 + 2, kTypeValue);
  f.largest = InternalKey("zoo", kBig + 600 + 2, kTypeDeletion);
  FileMetaData *fp2 = new FileMetaData(f);
  run21.InsertContainFile(fp2);
  f.number = kBig + 300 + 3;
  f.file_size = kBig + 400 + 3;
  f.smallest = InternalKey("foo", kBig + 500 + 3, kTypeValue);
  f.largest = InternalKey("zoo", kBig + 600 + 3, kTypeDeletion);
  FileMetaData *fp3 = new FileMetaData(f);
  run21.InsertContainFile(fp3);
  run21.InsertL0File(f.number + 10);
  run21.InsertL0File(f.number + 11);

  SortedRun run22(22, 2);
  f.number = kBig + 300 + 4;
  f.file_size = kBig + 400 + 4;
  f.smallest = InternalKey("foo", kBig + 500 + 4, kTypeValue);
  f.largest = InternalKey("zoo", kBig + 600 + 4, kTypeDeletion);
  FileMetaData *fp4 = new FileMetaData(f);  
  run22.InsertContainFile(fp4);  
  f.number = kBig + 300 + 5;
  f.file_size = kBig + 400 + 5;
  f.smallest = InternalKey("foo", kBig + 500 + 5, kTypeValue);
  f.largest = InternalKey("zoo", kBig + 600 + 5, kTypeDeletion);
  FileMetaData *fp5 = new FileMetaData(f);  
  //run22.InsertContainFile(fp5);   
  run22.InsertL0File(f.number + 20);
  //run22.InsertL0File(f.number + 21);  

  edit.AddSnapshotRun(run111);
  edit.AddSnapshotRun(run21);
  edit.AddSnapshotRun(run22);
  
  edit.SetComparatorName("foo");
  edit.SetLogNumber(kBig + 100);
  edit.SetNextFile(kBig + 200);
  edit.SetLastSequence(kBig + 1000);
  TestEncodeDecode(edit);
}

TEST(VersionEditTest, NormalEncodeDecode){
  static const uint64_t kBig = 1ull << 50;
  VersionEdit edit;
  edit.SetLevel(1, 2);
  SortedRun run111(111, 1);
  FileMetaData f;
  f.number = kBig + 300 + 1;
  f.file_size = kBig + 400 + 1;
  f.smallest = InternalKey("foo", kBig + 500 + 1, kTypeValue);
  f.largest = InternalKey("zoo", kBig + 600 + 1, kTypeDeletion);
  FileMetaData *fp = new FileMetaData(f);
  run111.InsertContainFile(fp);
  run111.InsertL0File(f.number);
  edit.AddRun(run111);
  SortedRun run21(2, 21);
  f.number = kBig + 300 + 2;
  f.file_size = kBig + 400 + 2;
  f.smallest = InternalKey("foo", kBig + 500 + 2, kTypeValue);
  f.largest = InternalKey("zoo", kBig + 600 + 2, kTypeDeletion);
  FileMetaData *fp2 = new FileMetaData(f);
  run21.InsertContainFile(fp2);  
  run21.InsertL0File(f.number + 10);
  run21.InsertL0File(f.number + 11);
  SortedRun *r = new SortedRun(run21);
  edit.RemoveRun(r);  

  edit.SetComparatorName("foo");
  edit.SetLogNumber(kBig + 100);
  edit.SetNextFile(kBig + 200);
  edit.SetLastSequence(kBig + 1000);
  TestEncodeDecode(edit);
}


}  // namespace leveldb

