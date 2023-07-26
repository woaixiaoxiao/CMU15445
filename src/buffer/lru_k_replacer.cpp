//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"

namespace bustub {

LRUKFrameRecord::LRUKFrameRecord(size_t frame_id, size_t k) : frame_id_(frame_id), k_(k) {}

auto LRUKFrameRecord::IsEvictable() const -> bool { return is_evictable_; }

auto LRUKFrameRecord::SetEvictable(bool is_evictable) -> void { is_evictable_ = is_evictable; }

auto LRUKFrameRecord::Access(uint64_t time) -> void {
  while (access_records_.size() >= k_) {
    access_records_.pop();
  }
  access_records_.push(time);
}

auto LRUKFrameRecord::LastKAccessTime() const -> uint64_t { return access_records_.front(); }

auto LRUKFrameRecord::EarliestAccessTime() const -> uint64_t { return access_records_.front(); }

auto LRUKFrameRecord::GetFrameId() const -> size_t { return frame_id_; }

auto LRUKFrameRecord::AccessSize() const -> size_t { return access_records_.size(); }

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : k_(k) {
  // 初始化frames_的大小为frame的数量
  frames_.resize(num_frames, nullptr);
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  // 如果没有可以淘汰的
  if (lru_mature_.empty() && lru_premature_.empty()) {
    return false;
  }
  // 优先淘汰不足k次的，如果没有，那就淘汰k次的
  auto has_premature = !lru_premature_.empty();
  auto first_iter = has_premature ? lru_premature_.begin() : lru_mature_.begin();
  *frame_id = (*first_iter)->GetFrameId();
  // 在set中删除这个页框
  DeallocateFrameRecord(first_iter, has_premature);
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  // 如果这个页框还没有分配
  if (frames_[frame_id] == nullptr) {
    AllocateFrameRecord(frame_id);
  }
  // 可能需要修改这个页框所在的set，或者需要修改这个页框在set中的顺序，所以在需要的情况下删除
  // 这个页框的访问数量是否小于k
  auto is_premature = frames_[frame_id]->AccessSize() < k_;
  // 这个页框是否可以被淘汰，即这个页框是否存在于某个set中
  auto is_evictable = frames_[frame_id]->IsEvictable();
  if (is_evictable && is_premature && frames_[frame_id]->AccessSize() == (k_ - 1)) {
    lru_premature_.erase(frames_[frame_id]);
  }
  if (is_evictable && (!is_premature)) {
    lru_mature_.erase(frames_[frame_id]);
  }
  // 更新访问信息
  frames_[frame_id]->Access(CurrTime());
  // 如果之前在小于k的set中，并且再访问一次刚好进入等于k的set中，则插入
  if (is_evictable && is_premature && frames_[frame_id]->AccessSize() == k_) {
    lru_mature_.insert(frames_[frame_id]);
  }
  // 可能出现调整顺序的情况
  if (is_evictable && (!is_premature)) {
    lru_mature_.insert(frames_[frame_id]);
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock<std::mutex> lock(latch_);
  // 首先判断这个页框是否在被使用
  if (frames_[frame_id] == nullptr) {
    return;
  }
  // 只有当前状态和要设置的状态不同时，才需要设置
  auto is_premature = frames_[frame_id]->AccessSize() < k_;
  if (set_evictable && !frames_[frame_id]->IsEvictable()) {
    replacer_size_++;
    if (is_premature) {
      lru_premature_.insert(frames_[frame_id]);
    } else {
      lru_mature_.insert(frames_[frame_id]);
    }
  }
  if (!set_evictable && frames_[frame_id]->IsEvictable()) {
    replacer_size_--;
    if (is_premature) {
      lru_premature_.erase(frames_[frame_id]);
    } else {
      lru_mature_.erase(frames_[frame_id]);
    }
  }
  // 更新对应的frame中的状态
  frames_[frame_id]->SetEvictable(set_evictable);
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  // 如果没有这个页框，直接return
  if (frames_[frame_id] == nullptr) {
    return;
  }
  // 如果有这个页框，则将这个页框从set中删除，并且delete掉，最后更新size
  DeallocateFrameRecord(frame_id, frames_[frame_id]->AccessSize() < k_);
}

auto LRUKReplacer::Size() -> size_t { return replacer_size_; }

auto LRUKReplacer::AllocateFrameRecord(size_t frame_id) -> void {
  frames_[frame_id] = new LRUKFrameRecord(frame_id, k_);
  curr_size_++;
}

auto LRUKReplacer::DeallocateFrameRecord(size_t frame_id, bool is_premature) -> void {
  if (is_premature) {
    lru_premature_.erase(frames_[frame_id]);
  } else {
    lru_mature_.erase(frames_[frame_id]);
  }
  delete frames_[frame_id];
  frames_[frame_id] = nullptr;
  curr_size_--;
  replacer_size_--;
}

auto LRUKReplacer::DeallocateFrameRecord(LRUKReplacer::container_iterator it, bool is_premature) -> void {
  frame_id_t frame_to_delete = (*it)->GetFrameId();
  if (is_premature) {
    lru_premature_.erase(it);
  } else {
    lru_mature_.erase(it);
  }
  delete frames_[frame_to_delete];
  frames_[frame_to_delete] = nullptr;
  curr_size_--;
  replacer_size_--;
}

}  // namespace bustub
