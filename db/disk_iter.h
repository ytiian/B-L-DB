#ifndef STORAGE_LEVELDB_DISK_ITER_H_
#define STORAGE_LEVELDB_DISK_ITER_H_

#include "skiplist/skip_list.h"
#include "sindex/sindex_wrapper.h"
#include <vector>
#include <unordered_map>

namespace leveldb {

class Comparator;
class Iterator;

Iterator* NewDiskIterator(const Comparator* comparator, Iterator** children,
                             int n, SIndexWrapper* sindex, 
                             const std::unordered_map<uint64_t, int>* index_map_);
}
#endif