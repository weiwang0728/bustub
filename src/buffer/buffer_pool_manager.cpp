//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"
#include <cstddef>
#include <cstdlib>
#include <mutex>
#include <optional>

#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "storage/disk/disk_manager.h"
#include "storage/disk/disk_scheduler.h"
#include "storage/page/page.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)), log_manager_(log_manager) {
  // TODO(students): remove this line after you have implemented the buffer pool manager

  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  std::lock_guard<std::mutex> lock(latch_);

  bool all_pined = true;
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].pin_count_ == 0) {
      all_pined = false;
      break;
    }
  }

  if (all_pined) {
    return nullptr;
  }

  frame_id_t free_frame;
  if (!free_list_.empty()) {
    free_frame = free_list_.front();
    free_list_.pop_front();
  } else {
    if (!replacer_->Evict(&free_frame)) {
      return nullptr;
    }

    if (pages_[free_frame].is_dirty_) {
      DiskRequest r{true, pages_[free_frame].GetData(), pages_[free_frame].page_id_, disk_scheduler_->CreatePromise()};
      disk_scheduler_->Schedule(std::move(r));
      pages_[free_frame].is_dirty_ = false;
    }

    free_pages_.push_back(pages_[free_frame].page_id_);
  }

  if (static_cast<size_t>((*page_id = AllocatePage())) >= pool_size_) {
    return nullptr;
  }

  if (page_table_.find(*page_id) != page_table_.end()) {
    DiskRequest r{false, pages_[free_frame].GetData(), *page_id, disk_scheduler_->CreatePromise()};
    disk_scheduler_->Schedule(std::move(r));
  }

  // 3. 加载新的页面
  pages_[free_frame].page_id_ = *page_id;
  pages_[free_frame].pin_count_ = 1;
  pages_[free_frame].is_dirty_ = false;
  replacer_->SetEvictable(free_frame, false);

  // 4. 更新页面表
  page_table_[*page_id] = free_frame;

  return &pages_[free_frame];
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  std::lock_guard<std::mutex> lock(latch_);
  bool all_pined = true;
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].pin_count_ == 0) {
      all_pined = false;
      break;
    }
  }

  if (all_pined) {
    return nullptr;
  }

  if (page_table_.find(page_id) != page_table_.end()) {
    frame_id_t frame_id = page_table_[page_id];
    Page *page = &pages_[frame_id];
    page->pin_count_++;
    replacer_->SetEvictable(frame_id, false);
    return page;
  }

  frame_id_t free_frame;
  if (!free_list_.empty()) {
    free_frame = free_list_.front();
    free_list_.pop_front();
  } else {
    if (!replacer_->Evict(&free_frame)) {
      return nullptr;
    }

    if (pages_[free_frame].is_dirty_) {
      pages_[free_frame].is_dirty_ = false;
    }
  }

  DiskRequest r{false, pages_[free_frame].GetData(), page_id, disk_scheduler_->CreatePromise()};
  disk_scheduler_->Schedule(std::move(r));

  // 3. 加载新的页面
  pages_[free_frame].page_id_ = page_id;
  pages_[free_frame].pin_count_++;
  pages_[free_frame].is_dirty_ = false;
  replacer_->SetEvictable(free_frame, false);

  return &pages_[free_frame];
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::lock_guard<std::mutex> lock(latch_);

  if (page_table_.find(page_id) == page_table_.end()) {
    return false;
  }

  frame_id_t frame_id = page_table_[page_id];
  Page *page = &pages_[frame_id];

  if (page->pin_count_ <= 0) {
    return false;
  }

  page->pin_count_--;
  if (is_dirty) {
    page->is_dirty_ = true;
  }

  if (page->pin_count_ == 0) {
    replacer_->SetEvictable(frame_id, true);
  }

  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);

  if (page_table_.find(page_id) == page_table_.end()) {
    return false;
  }

  frame_id_t frame_id = page_table_[page_id];
  Page *page = &pages_[frame_id];

  if (page->IsDirty()) {
    page->is_dirty_ = false;
    DiskRequest r{true, page->GetData(), page->GetPageId(), disk_scheduler_->CreatePromise()};
    disk_scheduler_->Schedule(std::move(r));
  }
  DeallocatePage(page_id);
  return true;
}

void BufferPoolManager::FlushAllPages() {
  std::lock_guard<std::mutex> lock(latch_);

  for (auto it : page_table_) {
    page_id_t page_id = it.first;
    frame_id_t frame_id = it.second;

    Page *page = &pages_[frame_id];
    if (page->IsDirty()) {
      page->is_dirty_ = false;
      DiskRequest r{true, page->GetData(), page->GetPageId(), disk_scheduler_->CreatePromise()};
      disk_scheduler_->Schedule(std::move(r));
    }
    page_table_.erase(page_id);
    DeallocatePage(page_id);
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);

  if (page_table_.find(page_id) != page_table_.end()) {
    frame_id_t frame_id = page_table_[page_id];
    if (pages_[frame_id].pin_count_ > 0) {
      return false;
    }
    Page *page = &pages_[frame_id];
    if (page->IsDirty()) {
      page->is_dirty_ = false;
      DiskRequest r{true, page->GetData(), page->GetPageId(), disk_scheduler_->CreatePromise()};
      disk_scheduler_->Schedule(std::move(r));
    }
    replacer_->Remove(frame_id);
    page_table_.erase(page_id);
    pages_[frame_id].ResetMemory();
    free_list_.push_back(frame_id);
    DeallocatePage(page_id);
  }
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t {
  std::lock_guard<std::mutex> lock(latch_);
  if (!free_pages_.empty()) {
    page_id_t page_id = free_pages_.front();
    free_pages_.pop_front();
    return page_id;
  }
  return next_page_id_++;
}

void BufferPoolManager::DeallocatePage(page_id_t page_id) {
  std::lock_guard<std::mutex> lock(latch_);
  free_pages_.push_back(page_id);
}

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard { return {this, nullptr}; }

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, nullptr}; }

}  // namespace bustub
