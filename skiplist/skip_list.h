#ifndef STORAGE_LEVELDB_DB_SKIP_H_
#define STORAGE_LEVELDB_DB_SKIP_H_

#include "folly/ConcurrentSkipList.h"
#include "leveldb/comparator.h"
#include <cstdint>
#include <memory>

namespace leveldb {

struct Handle{
  uint64_t value;
  size_t key_length;
  char key_data[1];
  Slice key() const {
    return Slice(key_data, key_length);
  }
};

class IndexComparator {
 public:
  IndexComparator() : user_cmp(BytewiseComparator()) {}
  IndexComparator(const Comparator* user_cmp) : user_cmp(user_cmp) {}
  int Compare(const Slice& akey, const Slice& bkey) const {
    return user_cmp->Compare(akey, bkey);
  }
  bool operator() (Handle* const& lhs, Handle* const& rhs) const {
    Slice akey = lhs->key();
    Slice bkey = rhs->key();
    //[todo] 插入的是user key还是internal key？
    //要求插入必须只插入user key
    int res = Compare(Slice(akey.data(), akey.size()), Slice(bkey.data(), bkey.size()));
    return res < 0;
  }
  private:
    const Comparator* user_cmp;
};

using SkipListT = folly::ConcurrentSkipList<Handle*, IndexComparator>;
using SkipListAccessor = SkipListT::Accessor;
using SkipListSkipper = SkipListT::Skipper;

class SkipListBase{
public:
  SkipListBase(const Comparator* cmp)
      : compare_(cmp),
        sl_(SkipListT::createInstance()) {}

  void Insert(const Slice& key, uint64_t value) {
    Handle* e =
      reinterpret_cast<Handle*>(malloc(sizeof(Handle) - 1 + key.size()));    
    e->value = value;
    e->key_length = key.size();
    std::memcpy(e->key_data, key.data(), key.size());
    SkipListAccessor accessor(sl_);
    auto ret = accessor.insert(e);
    //0: not insert; 1: insert success
    //return ret.second;
  }

  void Lookup(const Slice& key, uint64_t& value) {
    Handle* x = nullptr;
    Handle* tmp = 
        reinterpret_cast<Handle*>(malloc(sizeof(Handle) - 1 + key.size()));
    tmp->key_length = key.size();
    std::memcpy(tmp->key_data, key.data(), key.size());
    {
      SkipListAccessor accessor(sl_);
      SkipListT::iterator iter = accessor.lower_bound(tmp);
      if(iter != accessor.end()){
        x = *iter;
      }
    }    
    if(x != nullptr){
      if(Equal(key, x->key())){
        value = x->value;
      }else{
        value = 0;
      }
    }  
    free(tmp);  
  }

  class Iterator {
   public:
    // Initialize an iterator over the specified list.
    // The returned iterator is not valid.

    explicit Iterator(SkipListBase* list);

    // Returns true iff the iterator is positioned at a valid node.
    bool Valid() const;

    // Returns the key at the current position.
    // REQUIRES: Valid()
    Handle* node() const;

    // Advances to the next position.
    // REQUIRES: Valid()
    void Next();

    // Advances to the previous position.
    // REQUIRES: Valid()
    //void Prev();
    Handle* NextIterNext();

    Handle* NowIterNext(); 

    // Advance to the first entry with a key >= target
    void Seek(const Slice& target);

    // Position at the first entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    void SeekToFirst();

    // Position at the last entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    //void SeekToLast();

    Slice Key();

    uint64_t Value();

   private:
    SkipListAccessor accessor_;
    SkipListT::iterator now_iter_;
    SkipListBase* list_;
    Handle* now_node_;
  };

  Iterator* NewIterator() { return new Iterator(this); }

private:

  bool Equal(const Slice& a, const Slice& b) const { return (compare_.Compare(a, b) == 0); }

  std::shared_ptr<SkipListT> sl_;

  IndexComparator compare_;
};

inline SkipListBase::Iterator::Iterator(SkipListBase* list):
    list_(list),
    accessor_(list->sl_), 
    now_node_(nullptr){}

inline bool SkipListBase::Iterator::Valid() const {
  if(now_iter_ == accessor_.end()){
    return false;
  } else{
    return true;
  }
}

inline Handle* SkipListBase::Iterator::node() const {
  assert(Valid());
  return now_node_;
}

inline void SkipListBase::Iterator::Next() {
  now_iter_++;
}

inline void SkipListBase::Iterator::Seek(const Slice& key) {
  Handle* tmp = 
      reinterpret_cast<Handle*>(malloc(sizeof(Handle) - 1 + key.size()));
  tmp->key_length = key.size();
  std::memcpy(tmp->key_data, key.data(), key.size());
  now_iter_ = accessor_.lower_bound(tmp);
  if(now_iter_ != accessor_.end()){
    now_node_ = *now_iter_;
  }
  free(tmp);
}

inline void SkipListBase::Iterator::SeekToFirst() {
  //Handle tmp(smallest);
  //now_iter_ = accessor_.lower_bound(&tmp);
  //now_node_ = *now_iter_;
  now_iter_ = accessor_.begin();
  now_node_ = *now_iter_;
}

inline Slice SkipListBase::Iterator::Key() {
  assert(Valid());
  now_node_ = *now_iter_;
  return now_node_->key();
}

inline uint64_t SkipListBase::Iterator::Value() {
  assert(Valid());
  now_node_ = *now_iter_;
  return now_node_->value;
}

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_SKIP_H_