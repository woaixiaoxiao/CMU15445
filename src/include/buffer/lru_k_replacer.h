//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.h
//
// Identification: src/include/buffer/lru_k_replacer.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstdint>
#include <iostream>
#include <limits>
#include <list>
#include <memory>
#include <mutex>  // NOLINT
#include <queue>
#include <set>
#include <unordered_map>
#include <vector>
#include "common/config.h"
#include "common/macros.h"

namespace bustub {

// 存储一个页框相关的各种信息
class LRUKFrameRecord {
 public:
  LRUKFrameRecord(size_t frame_id, size_t k);
  // 访问操作
  auto Access(uint64_t time) -> void;

  auto IsEvictable() const -> bool;
  auto SetEvictable(bool is_evictable) -> void;

  auto LastAccessTime() const -> uint64_t;
  auto AccessTimes() const -> size_t;

  auto GetFrameId() const -> size_t;

 private:
  bool is_evictable_;
  size_t frame_id_;
  size_t k_;
  std::queue<uint64_t> access_records_;
};

// 定义了访问次数小于k的情况下的set的比较规则
struct LessKFrameComp {
  auto operator()(const LRUKFrameRecord *lhs, const LRUKFrameRecord *rhs) const -> bool {
    return lhs->LastAccessTime() < rhs->LastAccessTime();
  }
};

// 定义了访问次数大于等于k的情况下的set的比较规则
struct MoreKFrameComp {
  auto operator()(const LRUKFrameRecord *lhs, const LRUKFrameRecord *rhs) const -> bool {
    return lhs->LastAccessTime() < rhs->LastAccessTime();
  }
};

/**
 * LRUKReplacer implements the LRU-k replacement policy.
 *
 * The LRU-k algorithm evicts a frame whose backward k-distance is maximum
 * of all frames. Backward k-distance is computed as the difference in time between
 * current timestamp and the timestamp of kth previous access.
 *
 * A frame with less than k historical references is given
 * +inf as its backward k-distance. When multiple frames have +inf backward k-distance,
 * classical LRU algorithm is used to choose victim.
 */
class LRUKReplacer {
 public:
  /**
   * @brief a new LRUKReplacer.
   * @param num_frames the maximum number of frames the LRUReplacer will be required to store
   */
  explicit LRUKReplacer(size_t num_frames, size_t k);

  // 防止内存泄漏
  ~LRUKReplacer() {
    for (auto &frame : frames_) {
      delete frame;
    }
  }

  /**
   * @brief Find the frame with largest backward k-distance and evict that frame. Only frames
   * that are marked as 'evictable' are candidates for eviction.
   *
   * A frame with less than k historical references is given +inf as its backward k-distance.
   * If multiple frames have inf backward k-distance, then evict the frame with the earliest
   * timestamp overall.
   *
   * Successful eviction of a frame should decrement the size of replacer and remove the frame's
   * access history.
   *
   * @param[out] frame_id id of frame that is evicted.
   * @return true if a frame is evicted successfully, false if no frames can be evicted.
   */
  auto Evict(frame_id_t *frame_id) -> bool;

  /**
   * @brief Record the event that the given frame id is accessed at current timestamp.
   * Create a new entry for access history if frame id has not been seen before.
   *
   * If frame id is invalid (ie. larger than replacer_size_), throw an exception. You can
   * also use BUSTUB_ASSERT to abort the process if frame id is invalid.
   *
   * @param frame_id id of frame that received a new access.
   */
  void RecordAccess(frame_id_t frame_id);

  /**
   * @brief Toggle whether a frame is evictable or non-evictable. This function also
   * controls replacer's size. Note that size is equal to number of evictable entries.
   *
   * If a frame was previously evictable and is to be set to non-evictable, then size should
   * decrement. If a frame was previously non-evictable and is to be set to evictable,
   * then size should increment.
   *
   * If frame id is invalid, throw an exception or abort the process.
   *
   * For other scenarios, this function should terminate without modifying anything.
   *
   * @param frame_id id of frame whose 'evictable' status will be modified
   * @param set_evictable whether the given frame is evictable or not
   */
  void SetEvictable(frame_id_t frame_id, bool set_evictable);

  /**
   * @brief Remove an evictable frame from replacer, along with its access history.
   * This function should also decrement replacer's size if removal is successful.
   *
   * Note that this is different from evicting a frame, which always remove the frame
   * with largest backward k-distance. This function removes specified frame id,
   * no matter what its backward k-distance is.
   *
   * If Remove is called on a non-evictable frame, throw an exception or abort the
   * process.
   *
   * If specified frame is not found, directly return from this function.
   *
   * @param frame_id id of frame to be removed
   */
  void Remove(frame_id_t frame_id);

  /**
   * @brief Return replacer's size, which tracks the number of evictable frames.
   *
   * @return size_t
   */
  auto Size() -> size_t;

  // 返回当前的时间戳，并给它加1
  auto CurrTime() -> uint64_t { return curr_time_++; }

  // 给这个页框分配一个lrurecored类，并更新大小
  auto AllocateFrameRecord(size_t frame_id) -> void;

  uint64_t curr_time_{0};
  size_t curr_size_{0};
  size_t replacer_size_{0};
  size_t k_;
  std::mutex latch_;
  // 页框号->页框的统计信息类
  std::vector<LRUKFrameRecord *> frames_;
  // 记录可淘汰的页框，分别是访问次数小于k和大于等于k的
  std::set<LRUKFrameRecord *, LessKFrameComp> lessk_record_;
  std::set<LRUKFrameRecord *, MoreKFrameComp> morek_record_;
};

}  // namespace bustub
