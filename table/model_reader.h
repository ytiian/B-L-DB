#ifndef STORAGE_LEVELDB_MODEL_READER_H_
#define STORAGE_LEVELDB_MODEL_READER_H_

#include "leveldb/iterator.h"
#include "leveldb/slice.h"


namespace leveldb {

class ModelReader {
  public:
    ModelReader(Iterator* model_iter,
                int block_contain_keys,
                int common_prefix_len,
                int error_bound)
        : model_iter_(model_iter),
          block_contain_keys_(block_contain_keys),
          common_prefix_len_(common_prefix_len),
          error_bound_(error_bound) {}
    std::pair<size_t,size_t> SeekModel(Slice k);
    int GetBlockContainKeys();
  private:
    Iterator* model_iter_;
    int block_contain_keys_;
    int common_prefix_len_;
    int error_bound_;
};

ModelReader* NewModelReader(Iterator* model_iter,
                                    int block_contain_keys,
                                    int common_prefix_len,
                                    int error_bound);

}  // namespace leveldb

#endif 