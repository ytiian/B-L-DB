/*
 * The code is part of the SIndex project.
 *
 *    Copyright (C) 2020 Institute of Parallel and Distributed Systems (IPADS),
 * Shanghai Jiao Tong University. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

#include "helper.h"
#include "sindex_buffer.h"
#include "sindex_group.h"
#include "sindex_model.h"
#include "sindex_root.h"
#include "sindex_util.h"

#if !defined(SINDEX_H)
#define SINDEX_H

namespace sindex {

template <class key_t, class val_t, bool seq = false>
class SIndex {
  typedef Group<key_t, val_t, seq> group_t;
  typedef Root<key_t, val_t, seq> root_t;
  typedef void iterator_t;

 public:
  SIndex(const std::vector<key_t> &keys, const std::vector<val_t> &vals,
         size_t worker_num, size_t bg_n);
  ~SIndex();

  inline bool get(const key_t &key, val_t &val, const uint32_t worker_id);
  inline bool put(const key_t &key, const val_t &val, const uint32_t worker_id);
  inline bool remove(const key_t &key, const uint32_t worker_id);
  inline size_t scan(const key_t &begin, const size_t n,
                     std::vector<std::pair<key_t, val_t>> &result,
                     const uint32_t worker_id);
  size_t range_scan(const key_t &begin, const key_t &end,
                    std::vector<std::pair<key_t, val_t>> &result,
                    const uint32_t worker_id);

  class Iterator {
    public:
    // Initialize an iterator over the specified list.
    // The returned iterator is not valid.

    explicit Iterator(root_t* root, const uint32_t worker_id):
                      worker_id(worker_id),
                      root_(root),
                      root_iterator_(root_->NewIterator()) {};

    ~Iterator() {
        if (root_iterator_) {
            delete root_iterator_;
            root_iterator_ = nullptr;
        }
    }                      


    // Returns true iff the iterator is positioned at a valid node.
    bool Valid();

    // Advances to the next position.
    // REQUIRES: Valid()
    void Next();

    // Advance to the first entry with a key >= target
    void Seek(const key_t& target);

    // Position at the first entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    void SeekToFirst();

    // Position at the last entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    //void SeekToLast();

    key_t Key();

    val_t Value();

  private:
    const uint32_t worker_id;
    root_t *volatile root_;
    typename root_t::Iterator *root_iterator_;
  };

  Iterator* NewIterator(const uint32_t worker_id){
    return new Iterator(root, worker_id);
  }

 private:
  void start_bg();
  void terminate_bg();

  // this function should periodically check and perform structure updates
  static void *background(void *this_);

  root_t *volatile root = nullptr;
  pthread_t bg_master;
  size_t bg_num;
  volatile bool bg_running = true;
};

}  // namespace sindex

#endif  // SINDEX_H
