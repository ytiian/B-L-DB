#ifndef STORAGE_LEVELDB_DISK_ITER_H_
#define STORAGE_LEVELDB_DISK_ITER_H_

#include "trees/vanilla_b_plus_tree.h"
#include <vector>
#include <unordered_map>

namespace leveldb {

class Comparator;
class Iterator;

Iterator* NewDiskIterator(const Comparator* comparator, Iterator** children,
                             int n, VanillaBPlusTree<std::string, uint64_t>* btree, 
                             const std::unordered_map<uint64_t, int>* index_map_);
}
#endif