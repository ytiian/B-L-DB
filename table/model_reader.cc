#include "table/model_reader.h"
#include "table/format.h"
#include "sindex/greedy_plr.h"
#include "util/coding.h"

namespace leveldb {

std::pair<size_t,size_t> ModelReader::SeekModel(Slice k) {
  std::string target = Slice(k.data(), k.size()-8).ToString();   
  PutFixed32(&target, 1);
  PutFixed32(&target, 0);  
  model_iter_->Seek(target);
  if(model_iter_->Valid()){
    Slice handle_value = model_iter_->value();
    ModelParam param;
    if (param.DecodeFrom(&handle_value).ok()) {
      std::pair<size_t, size_t> range = 
          sindex::GreedyPLR::GetSearchRange(Slice(k.data(), k.size()-8).ToString(), 
            common_prefix_len_, param.slope(), param.intercept(), error_bound_);
      //std::cout<<"model predicted range: ["<<range.first<<","<<range.second<<"]"<<std::endl;
      size_t start_index = range.first;
      size_t end_index = range.second;

      return {start_index, end_index};
    }    
  }
  return {0,0};
}

int ModelReader::GetBlockContainKeys() {
  return block_contain_keys_;
}


ModelReader* NewModelReader(Iterator* model_iter,
                                        int block_contain_keys,
                                        int common_prefix_len,
                                        int error_bound) {
  return new ModelReader(model_iter, block_contain_keys, common_prefix_len, error_bound);
}

}  // namespace leveldb