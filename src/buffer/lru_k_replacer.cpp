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
#include <cstdlib>
#include <map>
#include <mutex>
#include <stdexcept>
#include <utility>
#include "common/config.h"
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);

  if (evictable_list_.empty()) {
    return false;
  }

  std::map<size_t, std::list<frame_id_t>::iterator> less_k_frames;
  auto it = evictable_list_.begin();
  for (; it != evictable_list_.end(); it++) {
    if (node_store_[*it].history_.size() < k_) {
      less_k_frames.insert({node_store_[*it].history_.front(), it});
    }
  }

  if (!less_k_frames.empty()) {
    auto erase_frame_id = *(less_k_frames.begin()->second);
    evictable_list_.erase(less_k_frames.begin()->second);
    node_store_.erase(erase_frame_id);
    --curr_size_;
    *frame_id = erase_frame_id;
    return true;
  }

  it = evictable_list_.begin();
  auto evict_it = evictable_list_.begin();
  for (; evict_it != evictable_list_.end(); evict_it++) {
    auto &node = node_store_[*evict_it];
    if (node.history_.back() < node_store_[*it].history_.back()) {
      it = evict_it;
    }
  }

  *frame_id = *it;
  evictable_list_.erase(it);
  node_store_.erase(*frame_id);
  --curr_size_;
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  std::lock_guard<std::mutex> lock(latch_);
  if (static_cast<size_t>(frame_id) >= replacer_size_) {
    throw std::invalid_argument("Invalid frame id");
  }

  auto &node = node_store_[frame_id];
  node.history_.push_front(current_timestamp_++);

  if (node.history_.size() > k_) {
    node.history_.pop_back();
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> lock(latch_);

  if (static_cast<size_t>(frame_id) >= replacer_size_) {
    throw std::invalid_argument("Invalid frame id");
  }

  auto &node = node_store_[frame_id];
  if (set_evictable && !node.is_evictable_) {
    evictable_list_.push_back(frame_id);
    ++curr_size_;
  } else if (!set_evictable && node.is_evictable_) {
    evictable_list_.remove(frame_id);
    --curr_size_;
  }
  node.is_evictable_ = set_evictable;
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);

  if (static_cast<size_t>(frame_id) >= replacer_size_) {
    throw std::invalid_argument("Invalid frame id");
  }

  auto it = node_store_.find(frame_id);
  if (it == node_store_.end() || !it->second.is_evictable_) {
    throw std::invalid_argument("can not remove a non-eviction or non-existence frame");
  }

  evictable_list_.remove(frame_id);
  node_store_.erase(frame_id);
  curr_size_--;
}

auto LRUKReplacer::Size() -> size_t {
  std::lock_guard<std::mutex> lock(latch_);
  return curr_size_;
}

}  // namespace bustub
