#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept
    : bpm_(that.bpm_), page_(that.page_), is_dirty_(that.is_dirty_) { 
  that.bpm_ = nullptr;
  that.page_ = nullptr;
  that.is_dirty_ = false;
}

void BasicPageGuard::Drop() {
  if (page_ != nullptr && bpm_ != nullptr) {
    bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
    page_ = nullptr;
  }
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  if (this != &that) {
    Drop();
    this->bpm_ = that.bpm_;
    this->page_ = that.page_;
    this->is_dirty_ = that.is_dirty_;
    this->bpm_ = nullptr;
  }
  return *this;
}

BasicPageGuard::~BasicPageGuard() { Drop(); };  // NOLINT

auto BasicPageGuard::UpgradeRead() -> ReadPageGuard {
  ReadPageGuard guard(bpm_, page_);
  return guard;
}

auto BasicPageGuard::UpgradeWrite() -> WritePageGuard {
  WritePageGuard guard(bpm_, page_);
  return guard;
}

ReadPageGuard::ReadPageGuard(BufferPoolManager *bpm, Page *page) : guard_(bpm, page) {}

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept : guard_(std::move(that.guard_)) {
  this->guard_ = BasicPageGuard();
}

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  if (this != &that) {
    guard_.Drop();
    guard_ = std::move(that.guard_);
    that.guard_ = BasicPageGuard();
  }
  return *this;
}

void ReadPageGuard::Drop() { guard_.Drop(); }

ReadPageGuard::~ReadPageGuard() { guard_.Drop(); }  // NOLINT

WritePageGuard::WritePageGuard(BufferPoolManager *bpm, Page *page) : guard_(bpm, page) {}

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept {
  guard_.Drop();
  guard_ = std::move(that.guard_);
  that.guard_ = BasicPageGuard();
}

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  if (this != &that) {
    this->Drop();
    this->guard_ = std::move(that.guard_);
    that.guard_ = BasicPageGuard();
  }
  return *this;
}

void WritePageGuard::Drop() { guard_.Drop(); }

WritePageGuard::~WritePageGuard() { guard_.Drop(); }  // NOLINT

}  // namespace bustub
