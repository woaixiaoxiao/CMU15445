//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstddef>
#include <cstdlib>
#include <functional>
#include <iostream>
#include <iterator>
#include <list>
#include <memory>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
  dir_.emplace_back(std::make_shared<Bucket>(bucket_size_));
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  // std::scoped_lock<std::mutex> lock(latch_);
  // std::unique_lock<std::mutex> lock(latch_);
  std::scoped_lock<std::mutex> lock(latch_);
  size_t index = IndexOf(key);
  return dir_[index]->Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  // std::scoped_lock<std::mutex> lock(latch_);
  // std::unique_lock<std::mutex> lock(latch_);
  // std::cout<<"I am in Remove"<<std::endl;
  std::scoped_lock<std::mutex> lock(latch_);
  // std::cout<<"I am in "<<std::endl;
  size_t index = IndexOf(key);
  return dir_[index]->Remove(key);
  // latch_.unlock();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::RedistributeBucket(std::shared_ptr<Bucket> oldbucket, std::shared_ptr<Bucket> newbucket)
    -> void {
  // 遍历原桶中的键值对
  auto &items = oldbucket->GetItems();
  for (auto it = items.begin(); it != items.end();) {
    const K &key = it->first;
    const V &value = it->second;

    // 计算键的目录索引
    size_t index = IndexOf(key);

    // 如果目录索引的最低有效位与目标桶的局部深度相同，则将键值对移动到新桶中
    int localdepth = newbucket->GetDepth();
    int localmask = 1 << (localdepth - 1);
    if ((index & localmask) == localmask) {
      newbucket->Insert(key, value);
      it = items.erase(it);
    } else {
      ++it;
    }
  }
}

void PrintDebug(int v) {
  for (int i = 0; i < 10; i++) {
    std::cout << v;
  }
  std::cout << std::endl;
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::InsertImplemantation(const K &key, const V &value) {
  size_t index = IndexOf(key);
  std::shared_ptr<Bucket> oldbucket = dir_[index];
  if (oldbucket->Insert(key, value)) {
    return;
  }
  if (oldbucket->GetDepth() == GetGlobalDepthInternal()) {
    global_depth_++;
    size_t length = dir_.size();
    dir_.resize(length << 1);

    for (size_t i = length; i < 2 * length; i++) {
      dir_[i] = dir_[i - length];
    }
  }

  int oldlocaldepth = oldbucket->GetDepth();
  size_t oldlocalmask = (1 << oldlocaldepth) - 1;

  int newlocaldepth = oldlocaldepth + 1;
  size_t newlocalmask = (1 << newlocaldepth) - 1;

  oldbucket->IncrementDepth();
  std::shared_ptr<Bucket> newbucket = std::make_shared<Bucket>(bucket_size_, oldbucket->GetDepth());
  num_buckets_++;

  // RedistributeBucket(oldbucket, newbucket);
  auto &items = oldbucket->GetItems();
  for (auto it = items.begin(); it != items.end();) {
    const K &key = it->first;
    const V &value = it->second;

    // 计算键的目录索引
    size_t index = IndexOf(key);

    // 如果目录索引的最低有效位与目标桶的局部深度相同，则将键值对移动到新桶中
    size_t validpos = oldlocalmask + 1;
    if ((index & validpos) == validpos) {
      newbucket->Insert(key, value);
      it = items.erase(it);
    } else {
      ++it;
    }
  }

  size_t start = (index & oldlocalmask) + (oldlocalmask + 1);

  for (size_t i = start; i < dir_.size(); i += (newlocalmask + 1)) {
    dir_[i] = newbucket;
  }

  InsertImplemantation(key, value);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  std::scoped_lock<std::mutex> lock(latch_);
  InsertImplemantation(key, value);
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  for (const auto &p : list_) {
    if (p.first == key) {
      value = p.second;
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  auto it = list_.begin();
  while (it != list_.end()) {
    if (it->first == key) {
      list_.erase(it);
      return true;
    }
    ++it;
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  // updated
  for (auto &p : list_) {
    if (p.first == key) {
      p.second = value;
      return true;
    }
  }
  // full
  if (list_.size() == size_) {
    return false;
  }
  // insert
  list_.emplace_back(key, value);
  return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
