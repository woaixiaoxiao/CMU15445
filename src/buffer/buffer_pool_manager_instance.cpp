//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"
#include <cstddef>

#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "storage/disk/disk_manager.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }

  // TODO(students): remove this line after you have implemented the buffer pool manager
  // throw NotImplementedException(
  //     "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //     "exception line in `buffer_pool_manager_instance.cpp`.");
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}
// 置换一个页面回磁盘，并把这个页面的状态置为原始状态,最后在各种数据结构里除去这个页面
auto BufferPoolManagerInstance::SetPageGreen(frame_id_t curframe) -> void {
  if (pages_[curframe].IsDirty()) {
    disk_manager_->WritePage(pages_[curframe].GetPageId(), pages_[curframe].GetData());
  }
  page_table_->Remove(pages_[curframe].page_id_);
  replacer_->Remove(curframe);
  pages_[curframe].ResetMemory();
  pages_[curframe].page_id_ = INVALID_PAGE_ID;
  pages_[curframe].pin_count_ = 0;
  pages_[curframe].is_dirty_ = false;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  std::lock_guard<std::mutex> lock(latch_);
  frame_id_t curframe;
  // 有空闲的页框
  if (!free_list_.empty()) {
    // std::cout<<"***********!!!!!!!!!!!!"<<std::endl;
    curframe = free_list_.front();
    free_list_.pop_front();
    // 淘汰一个页框
  } else if (replacer_->Evict(&curframe)) {
    SetPageGreen(curframe);
  } else {
    return nullptr;
  }
  // 已经得到了一个可用的页框号，现在可以创建页面，并将页框和页面关联，放入这个缓存池系统
  Page *curpage = &pages_[curframe];
  curpage->page_id_ = AllocatePage();
  *page_id = curpage->page_id_;
  curpage->pin_count_ = 1;
  page_table_->Insert(curpage->page_id_, curframe);
  replacer_->RecordAccess(curframe);
  replacer_->SetEvictable(curframe, false);
  return curpage;
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  std::lock_guard<std::mutex> lock(latch_);
  frame_id_t curframe;
  if (page_table_->Find(page_id, curframe)) {
    pages_[curframe].pin_count_++;
    // fetch也是在访问这个页面
    replacer_->RecordAccess(curframe);
    replacer_->SetEvictable(curframe, false);
    return &pages_[curframe];
  }
  // 有空闲的页框
  if (!free_list_.empty()) {
    curframe = free_list_.front();
    free_list_.pop_front();
    // 淘汰一个页框
  } else if (replacer_->Evict(&curframe)) {
    SetPageGreen(curframe);
  } else {
    return nullptr;
  }
  Page *curpage = &pages_[curframe];
  curpage->page_id_ = page_id;
  curpage->pin_count_ = 1;

  disk_manager_->ReadPage(page_id, curpage->GetData());

  page_table_->Insert(curpage->page_id_, curframe);
  replacer_->RecordAccess(curframe);
  replacer_->SetEvictable(curframe, false);

  return curpage;
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  frame_id_t curframe;
  if (page_table_->Find(page_id, curframe)) {
    if (pages_[curframe].GetPinCount() != 0) {
      pages_[curframe].pin_count_--;
      if (pages_[curframe].pin_count_ == 0) {
        replacer_->SetEvictable(curframe, true);
      }
      // 只有当原来是不脏，现在脏，才需要赋值
      if (is_dirty) {
        pages_[curframe].is_dirty_ = is_dirty;
      }
      return true;
    }
    return false;
  }
  return false;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  frame_id_t curframe;
  if (page_table_->Find(page_id, curframe)) {
    disk_manager_->WritePage(page_id, pages_[curframe].data_);
    pages_->is_dirty_ = false;
    return true;
  }
  return false;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  std::lock_guard<std::mutex> lock(latch_);
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].page_id_ != INVALID_PAGE_ID) {
      FetchPgImp(pages_[i].page_id_);
    }
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  frame_id_t curframe;
  if (page_table_->Find(page_id, curframe)) {
    if (pages_[curframe].pin_count_ != 0) {
      return false;
    }
    SetPageGreen(curframe);
    free_list_.push_back(curframe);
    DeallocatePage(page_id);
  }
  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub
