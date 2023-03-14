// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//3.7 ✔

#include "db/dbformat.h"

#include <cstdio>
#include <sstream>

#include "port/port.h"
#include "util/coding.h"

namespace leveldb {

static uint64_t PackSequenceAndType(uint64_t seq, ValueType t) {
  assert(seq <= kMaxSequenceNumber);
  assert(t <= kValueTypeForSeek);
  return (seq << 8) | t;//其实就是返回seq+type
}

void AppendInternalKey(std::string* result, const ParsedInternalKey& key) {
  result->append(key.user_key.data(), key.user_key.size());//先放userkey
  PutFixed64(result, PackSequenceAndType(key.sequence, key.type));//再放seq+tag
}

std::string ParsedInternalKey::DebugString() const {
  std::ostringstream ss;
  ss << '\'' << EscapeString(user_key.ToString()) << "' @ " << sequence << " : "
     << static_cast<int>(type);
  return ss.str();
}

std::string InternalKey::DebugString() const {
  ParsedInternalKey parsed;
  if (ParseInternalKey(rep_, &parsed)) {
    return parsed.DebugString();
  }
  std::ostringstream ss;
  ss << "(bad)" << EscapeString(rep_);
  return ss.str();
}

const char* InternalKeyComparator::Name() const {
  return "leveldb.InternalKeyComparator";
}

//对internalkey的排序规则
int InternalKeyComparator::Compare(const Slice& akey, const Slice& bkey) const {
  // Order by:
  //    increasing user key (according to user-supplied comparator)
  //    decreasing sequence number
  //    decreasing type (though sequence# should be enough to disambiguate)
  int r = user_comparator_->Compare(ExtractUserKey(akey), ExtractUserKey(bkey));
  if (r == 0) {//userkey相同的情况
  //
    const uint64_t anum = DecodeFixed64(akey.data() + akey.size() - 8);
    const uint64_t bnum = DecodeFixed64(bkey.data() + bkey.size() - 8);
    if (anum > bnum) {
      r = -1;//seq+type大的反而小
    } else if (anum < bnum) {
      r = +1;
    }
  }
  return r;
}

//InternalKeyComparator实现了Comparator的抽象
//本质上叫FindShortestSeparator的函数要实现的缩短都是对于user_key而言的
void InternalKeyComparator::FindShortestSeparator(std::string* start,
                                                  const Slice& limit) const {
  // Attempt to shorten the user portion of the key
  Slice user_start = ExtractUserKey(*start);
  Slice user_limit = ExtractUserKey(limit);
  std::string tmp(user_start.data(), user_start.size());
  //这里的user_comparator_来自option中的comparator
  //Options::Options() : comparator(BytewiseComparator()), 
  //此处调用的是BytewiseComparator实现的FindShortestSeparator
  user_comparator_->FindShortestSeparator(&tmp, user_limit);
  if (tmp.size() < user_start.size() &&
      user_comparator_->Compare(user_start, tmp) < 0) {
    // User key has become shorter physically, but larger logically.
    // Tack on the earliest possible number to the shortened user key.
    PutFixed64(&tmp,
              //最大的seq+type
               PackSequenceAndType(kMaxSequenceNumber, kValueTypeForSeek));
    assert(this->Compare(*start, tmp) < 0);
    assert(this->Compare(tmp, limit) < 0);
    start->swap(tmp);
  }
}

void InternalKeyComparator::FindShortSuccessor(std::string* key) const {
  Slice user_key = ExtractUserKey(*key);
  std::string tmp(user_key.data(), user_key.size());
  user_comparator_->FindShortSuccessor(&tmp);
  if (tmp.size() < user_key.size() &&
      user_comparator_->Compare(user_key, tmp) < 0) {
    // User key has become shorter physically, but larger logically.
    // Tack on the earliest possible number to the shortened user key.
    PutFixed64(&tmp,
               PackSequenceAndType(kMaxSequenceNumber, kValueTypeForSeek));
    assert(this->Compare(*key, tmp) < 0);
    key->swap(tmp);
  }
}

//实际还是为了给userkey构建filter
const char* InternalFilterPolicy::Name() const { return user_policy_->Name(); }

void InternalFilterPolicy::CreateFilter(const Slice* keys, int n,
                                        std::string* dst) const {
  // We rely on the fact that the code in table.cc does not mind us
  // adjusting keys[].
  Slice* mkey = const_cast<Slice*>(keys);
  for (int i = 0; i < n; i++) {
    mkey[i] = ExtractUserKey(keys[i]);
    // TODO(sanjay): Suppress dups?
  }
  user_policy_->CreateFilter(keys, n, dst);
}

bool InternalFilterPolicy::KeyMayMatch(const Slice& key, const Slice& f) const {
  return user_policy_->KeyMayMatch(ExtractUserKey(key), f);
}

  // We construct a char array of the form:
  //    klength  varint32               <-- start_ //哪一部分的length？
  //    userkey  char[klength]          <-- kstart_
  //    tag      uint64                 //可能是sequence+type？
  //                                    <-- end_
  // The array is a suitable MemTable key.
  // The suffix starting with "userkey" can be used as an InternalKey.
LookupKey::LookupKey(const Slice& user_key, SequenceNumber s) {
  size_t usize = user_key.size();
  size_t needed = usize + 13;  // A conservative estimate
  char* dst;
  if (needed <= sizeof(space_)) {
    dst = space_;
  } else {
    dst = new char[needed];
  }
  start_ = dst;
  //写klength字段：整个internalkey的长度
  dst = EncodeVarint32(dst, usize + 8);
  kstart_ = dst;
  //写userkey字段
  std::memcpy(dst, user_key.data(), usize);
  dst += usize;
  //写seq+type
  //kValueTypeForSeek最后会定位到userkey为user_key，快照为s的key前面
  //直接迭代就可以得到想要的内容
  EncodeFixed64(dst, PackSequenceAndType(s, kValueTypeForSeek));
  dst += 8;
  end_ = dst;
}

}  // namespace leveldb
