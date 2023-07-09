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
#include <cstddef>
#include <iostream>
#include <iterator>
#include <memory>
#include <ostream>
#include <stdexcept>
#include <utility>

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {
  // std::cout << "******num_frames  " << num_frames << std::endl;
  // std::cout << "******k_  " << k_ << std::endl;
}

auto LRUKReplacer::DeleteFrame(std::unordered_map<frame_id_t, std::list<std::unique_ptr<Frame>>::iterator> &map_,
                               std::list<std::unique_ptr<Frame>> &list_, frame_id_t *frame_id) -> bool {
  for (auto &frame : list_) {
    // 如果可以被删
    if (frame->IsEvictable()) {
      size_t frameid = frame->GetFrameId();
      *frame_id = frameid;
      auto frameiterator = map_[frameid];
      list_.erase(frameiterator);
      map_.erase(frameid);
      curr_size_--;
      return true;
    }
  }
  return false;
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  // std::cout << "******Evict******" << std::endl;
  std::lock_guard<std::mutex> lock(latch_);
  // 如果没有帧
  if (curr_size_ == 0) {
    return false;
  }
  // printf("111111111111111111\n");
  // 存在帧
  // 先去距离为inf的链表里找
  if (DeleteFrame(lesskmap_, lessklist_, frame_id)) {
    // std::cout << "******EvictID: " << *frame_id << std::endl;
    return true;
  }
  // inf找不到可以删除的，来有k距离的链表里找

  return DeleteFrame(overkmap_, overklist_, frame_id);

  // if (DeleteFrame(overkmap_, overklist_, frame_id)) {
  //   return true;
  // }
  // // 没有可以删除的
  // return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  // std::cout << "******Access  " << frame_id << std::endl;
  // 加锁
  std::lock_guard<std::mutex> lock(latch_);
  // frameid不合法，超出了最大页面数量
  // 有个小问题，这里的页面id是从1开始计数还是从0开始计数
  if (static_cast<size_t>(frame_id) >= replacer_size_) {
    throw std::invalid_argument("Invalid frame_id_t");
  }
  // 当前有插入，增加时间戳
  current_timestamp_++;
  // 如果这个id在overklist中，即已经被访问了大于等于k次
  if (overkmap_.find(frame_id) != overkmap_.end()) {
    // 这个frame的访问记录的迭代器
    auto access_record_iterator = overkmap_[frame_id];
    // 将当前访问插入访问记录中，并获取frame的倒数第k次的访问时间
    size_t prek_access_time = (*access_record_iterator)->InsertAccess(current_timestamp_);
    // 更新当前页框在overklist中的位置(先删除，后插入)，并更新overkmap
    std::unique_ptr<Frame> uniqurptr = std::move(*(access_record_iterator));
    overklist_.erase(access_record_iterator);
    overkmap_.erase((*uniqurptr).GetFrameId());
    for (auto start = overklist_.begin(); start != overklist_.end(); start++) {
      if ((*start)->GetPreK() > prek_access_time) {
        overkmap_[frame_id] = overklist_.insert(start, std::move(uniqurptr));
        return;
      }
    }
    // 没有哪个页面的倒数第k次访问晚于当前记录
    overklist_.push_back(std::move(uniqurptr));
    overkmap_[frame_id] = std::prev(overklist_.end());
    return;
  }
  //   这个frame之前访问次数不到k次
  if (lesskmap_.find(frame_id) != lesskmap_.end()) {
    auto access_record_iterator = lesskmap_[frame_id];
    size_t prek_access_time = (*access_record_iterator)->InsertAccess(current_timestamp_);

    // 插入之后达到了k次
    if ((*access_record_iterator)->OverKAccess()) {
      std::unique_ptr<Frame> uniqurptr = std::move(*(access_record_iterator));
      lessklist_.erase(access_record_iterator);
      lesskmap_.erase(frame_id);
      // 存下当前frame的信息，然后将lessk里和这个frame相关的都删除了
      // 插入overk的数据结构里面
      size_t count = 0;
      for (auto start = overklist_.begin(); start != overklist_.end(); start++) {
        count++;
        if ((*start)->GetPreK() > prek_access_time) {
          // std::cout << count << std::endl;
          overkmap_[frame_id] = overklist_.insert(start, std::move(uniqurptr));
          return;
        }
      }
      // std::cout<<flag<<std::endl;
      // 没有哪个页面的倒数第k次访问晚于当前记录
      // std::cout<<"I am 3"<<std::endl;
      overklist_.push_back(std::move(uniqurptr));
      overkmap_[frame_id] = std::prev(overklist_.end());
      // std::cout<<"I am 4"<<std::endl;
      return;
    }
    // 插入之后依然没有k次
    // lessklist_.push_back(std::move(uniqurptr));
    // lesskmap_[frame_id] = std::prev(lessklist_.end());
    return;
  }
  // 之前根本就没有这个frame
  std::unique_ptr<Frame> new_frame = std::make_unique<Frame>(frame_id, k_);
  new_frame->InsertAccess(current_timestamp_);
  lessklist_.push_back(std::move(new_frame));
  lesskmap_[frame_id] = std::prev(lessklist_.end());
}

auto LRUKReplacer::SetFrame(std::unordered_map<frame_id_t, std::list<std::unique_ptr<Frame>>::iterator> &map_,
                            frame_id_t frame_id, bool set_evictable) -> bool {
  if (map_.find(frame_id) != map_.end()) {
    auto it = map_[frame_id];
    bool is_evictable = (*it)->IsEvictable();

    if (static_cast<bool>(is_evictable ^ set_evictable)) {
      if (set_evictable) {
        curr_size_++;
      } else {
        curr_size_--;
      }
    }
    (*it)->SetEvictable(set_evictable);
    return true;
  }
  return false;
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  // 加锁
  std::lock_guard<std::mutex> lock(latch_);
  // std::cout << "******SetEvictable  " << frame_id << set_evictable << std::endl;
  // frameid不合法，超出了最大页面数量
  // 有个小问题，这里的页面id是从1开始计数还是从0开始计数
  if (static_cast<size_t>(frame_id) >= replacer_size_) {
    throw std::invalid_argument("Invalid frame_id_t");
  }

  if (SetFrame(lesskmap_, frame_id, set_evictable)) {
    return;
  }
  SetFrame(overkmap_, frame_id, set_evictable);
}

auto LRUKReplacer::DeleteCertainFrame(std::unordered_map<frame_id_t, std::list<std::unique_ptr<Frame>>::iterator> &map_,
                                      std::list<std::unique_ptr<Frame>> &list_, frame_id_t frame_id) -> bool {
  if (map_.find(frame_id) != map_.end()) {
    auto it = map_[frame_id];
    if (!((*it)->IsEvictable())) {
      return false;
    }
    // auto uniqueptr=std::move(*it);
    list_.erase(it);
    map_.erase(frame_id);
    curr_size_--;
  }
  return true;
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);
  // std::cout << "******Remove  " << frame_id << std::endl;
  if (!DeleteCertainFrame(overkmap_, overklist_, frame_id)) {
    throw std::invalid_argument("UnEvictable");
  }
  if (!DeleteCertainFrame(lesskmap_, lessklist_, frame_id)) {
    throw std::invalid_argument("UnEvictable");
  }
}

auto LRUKReplacer::Size() -> size_t {
  std::lock_guard<std::mutex> lock(latch_);
  // std::cout << "******Size  " << curr_size_ << std::endl;
  return curr_size_;
}

}  // namespace bustub
