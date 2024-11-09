#include "primer/hyperloglog.h"

namespace bustub {

template <typename KeyType>
HyperLogLog<KeyType>::HyperLogLog(int16_t n_bits) : cardinality_(0), bits_(n_bits){
  try
  {
    if (n_bits <=0 || n_bits >= BITSET_CAPACITY) {
        throw std::invalid_argument("n_bits not in range");
    }
  }
  catch(const std::invalid_argument& e)
  {
    std::cerr << e.what() << '\n';
    bits_ = 0;
  }
  
  registers_.resize(std::pow(2, n_bits));
}

template <typename KeyType>
auto HyperLogLog<KeyType>::ComputeBinary(const hash_t &hash) const -> std::bitset<BITSET_CAPACITY> {
  /** @TODO(student) Implement this function! */
  std::bitset<BITSET_CAPACITY> result(hash);
  return result;
}

template <typename KeyType>
auto HyperLogLog<KeyType>::PositionOfLeftmostOne(const std::bitset<BITSET_CAPACITY> &bset) const -> uint64_t {
  /** @TODO(student) Implement this function! */
  uint64_t begin = BITSET_CAPACITY - 1 - bits_;
  for (uint64_t i = begin; i > 0; i--)
  {
    if (bset[i] == 1)
    {
      return begin - i + 1;
    }
  }
  return 0;
}

template <typename KeyType>
auto HyperLogLog<KeyType>::AddElem(KeyType val) -> void {
  /** @TODO(student) Implement this function! */
  hash_t hash_value = CalculateHash(val);

  std::bitset<BITSET_CAPACITY> bset = ComputeBinary(hash_value);

  uint64_t p = PositionOfLeftmostOne(bset);

  uint64_t index = 0;
  if (bits_ != 0) {
    index = bset.to_ulong() >> (BITSET_CAPACITY - bits_);
  }

  registers_[index] = std::max(registers_[index], p);
}

template <typename KeyType>
auto HyperLogLog<KeyType>::ComputeCardinality() -> void {
  /** @TODO(student) Implement this function! */
  double sum = 0;
  size_t m = registers_.capacity();

  for (auto &value : registers_)
  {
    int exp = -value;
    sum += std::pow(2, exp);
  }
  cardinality_ = std::floor((HyperLogLog<KeyType>::CONSTANT * m * m) / sum);
}

template class HyperLogLog<int64_t>;
template class HyperLogLog<std::string>;

}  // namespace bustub
