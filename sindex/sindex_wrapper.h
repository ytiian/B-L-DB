#ifndef STORAGE_LEVELDB_DB_SINDEX_H_
#define STORAGE_LEVELDB_DB_SINDEX_H_

#include "sindex/sindex.h"
#include "sindex/sindex_impl.h"


enum class WorkerType : uint32_t {
  FLUSH = 0,
};

template <size_t len>
class StrKey;

typedef StrKey<16> index_key_t;
typedef sindex::SIndex<index_key_t, uint64_t> sindex_t;

template <size_t len>
class StrKey {
  typedef std::array<double, len> model_key_t;

 public:
  static constexpr size_t model_key_size() { return len; }

  static StrKey max() {
    static StrKey max_key;
    memset(&max_key.buf, 255, len);
    return max_key;
  }
  static StrKey min() {
    static StrKey min_key;
    memset(&min_key.buf, 0, len);
    return min_key;
  }

  StrKey() { memset(&buf, 0, len); }
  StrKey(uint64_t key) { COUT_N_EXIT("str key no uint64"); }
  StrKey(const std::string &s) {
    memset(&buf, 0, len);
    memcpy(&buf, s.data(), s.size());
  }
  StrKey(const StrKey &other) { memcpy(&buf, &other.buf, len); }
  StrKey &operator=(const StrKey &other) {
    memcpy(&buf, &other.buf, len);
    return *this;
  }

  model_key_t to_model_key() const {
    model_key_t model_key;
    for (size_t i = 0; i < len; i++) {
      model_key[i] = buf[i];
    }
    return model_key;
  }

  void get_model_key(size_t begin_f, size_t l, double *target) const {
    for (size_t i = 0; i < l; i++) {
      target[i] = buf[i + begin_f];
    }
  }

  bool less_than(const StrKey &other, size_t begin_i, size_t l) const {
    return memcmp(buf + begin_i, other.buf + begin_i, l) < 0;
  }

  friend bool operator<(const StrKey &l, const StrKey &r) {
    return memcmp(&l.buf, &r.buf, len) < 0;
  }
  friend bool operator>(const StrKey &l, const StrKey &r) {
    return memcmp(&l.buf, &r.buf, len) > 0;
  }
  friend bool operator>=(const StrKey &l, const StrKey &r) {
    return memcmp(&l.buf, &r.buf, len) >= 0;
  }
  friend bool operator<=(const StrKey &l, const StrKey &r) {
    return memcmp(&l.buf, &r.buf, len) <= 0;
  }
  friend bool operator==(const StrKey &l, const StrKey &r) {
    return memcmp(&l.buf, &r.buf, len) == 0;
  }
  friend bool operator!=(const StrKey &l, const StrKey &r) {
    return memcmp(&l.buf, &r.buf, len) != 0;
  }

  friend std::ostream &operator<<(std::ostream &os, const StrKey &key) {
    os << "key [" << std::hex;
    for (size_t i = 0; i < sizeof(StrKey); i++) {
      os << "0x" << key.buf[i] << " ";
    }
    os << "] (as byte)" << std::dec;
    return os;
  }

  uint8_t buf[len];
} ATTR_PACKED;

namespace leveldb {
class SIndexWrapper {
 public:
  SIndexWrapper() : sindex_(nullptr) {}
  ~SIndexWrapper() {
    if (sindex_ != nullptr) {
      delete sindex_;
      sindex_ = nullptr;
    }
  }

  bool isNull() const { return sindex_ == nullptr; }

  void BuildSIndex(const std::vector<index_key_t> &data,
                   size_t worker_num, size_t bg_n, uint64_t L0_id) {
    if (sindex_ != nullptr) {
      delete sindex_;
      sindex_ = nullptr;
    }
    
    std::vector<uint64_t> vals(data.size(), L0_id);
    sindex_ = new sindex_t(data, vals, worker_num, bg_n);
  }

  bool Insert(const index_key_t &key, uint64_t value, const uint32_t worker_id) {
    if (sindex_ == nullptr) {
      return false;
    }
    return sindex_->put(key, value, worker_id);
  }


  bool Lookup(const index_key_t &key, uint64_t &value, const uint32_t worker_id) {
    if (sindex_ == nullptr) {
      return false;
    }
    return sindex_->get(key, value, worker_id);
  }

 private:
  sindex_t *sindex_;
};


} // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_SINDEX_H_