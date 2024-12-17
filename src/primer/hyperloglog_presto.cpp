#include "primer/hyperloglog_presto.h"

namespace bustub {

template <typename KeyType>
HyperLogLogPresto<KeyType>::HyperLogLogPresto(int16_t n_leading_bits) : cardinality_(0), leadingbits_(n_leading_bits) {
  try
  {
    if (n_leading_bits <=0 || n_leading_bits >= 64) {
        throw std::invalid_argument("n_leading_bits not in range");
    }
  }
  catch(const std::invalid_argument& e)
  {
    std::cerr << e.what() << '\n';
    leadingbits_ = 0;
  }

  dense_bucket_.resize(std::pow(2, leadingbits_));
}

template <typename KeyType>
auto HyperLogLogPresto<KeyType>::AddElem(KeyType val) -> void {
  /** @TODO(student) Implement this function! */
  hash_t hash_val = CalculateHash(val);
  std::bitset<BITSET_CAPACITY> bset(hash_val);
  std::cout << bset << std::endl;
  uint16_t zero_count = 0;
  for (int i = 0; i < BITSET_CAPACITY-leadingbits_; i++) {
    if (bset[i] == 0) {
      zero_count++;
      continue;
    }
    break;
  }

  uint16_t index = 0;
  if (leadingbits_ != 0) {
    index = hash_val >> (BITSET_CAPACITY - leadingbits_);
  }

  uint64_t raw_val = dense_bucket_[index].to_ulong();
  std::unordered_map<uint16_t, std::bitset<OVERFLOW_BUCKET_SIZE>>::iterator it = overflow_bucket_.find(index);
  if (it != overflow_bucket_.end()) {
    uint64_t overflow_val = it->second.to_ulong();
    raw_val = raw_val | (overflow_val << DENSE_BUCKET_SIZE);
  }

  if (zero_count > raw_val) {
    if (zero_count <= 0xF) {
      dense_bucket_[index] = std::bitset<DENSE_BUCKET_SIZE> (zero_count);
    }
    else {
      uint16_t lsb_val = zero_count & 0xF;
      uint16_t msb_val = zero_count >> DENSE_BUCKET_SIZE;
      dense_bucket_[index] = std::bitset<DENSE_BUCKET_SIZE> (lsb_val);
      overflow_bucket_[index] = std::bitset<OVERFLOW_BUCKET_SIZE>(msb_val);
    }
  }
}

template <typename T>
auto HyperLogLogPresto<T>::ComputeCardinality() -> void {
  /** @TODO(student) Implement this function! */
  size_t lsb = 0;
  size_t msb = 0;
  double sum = 0;
  size_t m = dense_bucket_.capacity();

  for (size_t i = 0; i < m; i++) {
    lsb = dense_bucket_[i].to_ulong();
    if (overflow_bucket_.find(i) != overflow_bucket_.end()) {
      msb = overflow_bucket_[i].to_ulong() << DENSE_BUCKET_SIZE;
      lsb |= msb;
    }
    int exp = -lsb;
    sum += std::pow(2, exp);
  }

  cardinality_ = std::floor((HyperLogLogPresto<T>::CONSTANT * m * m) / sum);
}

template class HyperLogLogPresto<int64_t>;
template class HyperLogLogPresto<std::string>;
}  // namespace bustub
