#include "primer/trie.h"
#include <cstddef>
#include <memory>
#include <string_view>
#include <utility>
#include "common/exception.h"

namespace bustub {

/*
思路流程：
1. 获取当前的Trie根节点.
2. 遍历key中的每一个字符，查看字符是否存在。若存在则继续遍历， 不存在则返回nullptr。
3. 返回遍历完之后节点所对应的value。
*/
template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  // throw NotImplementedException("Trie::Get is not implemented.");

  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.

  auto current = this->root_;

  for (auto ch : key) {
    // 如果当前节点为空 或者 没有对应字符的子节点， 返回nullptr
    if (!current || current->children_.find(ch) == current->children_.end()) {
      return nullptr;
    }
    // 当前字符存在，接着往下找
    current = current->children_.at(ch);
  }

  // check the type match
  auto value_node = dynamic_cast<const TrieNodeWithValue<T> *>(current.get());
  if (value_node == nullptr) {
    std::cerr << "Type mismatch or not a TrieNodeWithValue<T>." << std::endl;
    return nullptr;
  }

  if (!value_node->is_value_node_) {
    return nullptr;
  }

  return value_node->value_.get();
}

template <class T>
void Putcycle(const std::shared_ptr<TrieNode> &new_root, std::string_view key, T value) {
  // 在new_root的children_中寻找key的第一个元素
  if (new_root->children_.find(key.at(0)) != new_root->children_.end()) {
    // 如果当前children_中已经存在key[0]
    auto &iter = new_root->children_[key.at(0)];
    if (key.size() > 1) {
      // 当前还未递归到最后的key， 继续往下递归
      // 复制一个当前子节点， 更新new_root
      std::shared_ptr<TrieNode> ptr = iter->Clone();
      Putcycle<T>(ptr, key.substr(1), std::move(value));
      iter = ptr;
    } else {
      // 当前已经递归到最后的key，直接修改原节点为带value的节点
      iter = std::make_shared<TrieNodeWithValue<T>>(iter->children_, std::make_shared<T>(std::move(value)));
    }
    return;
  }

  // 如果new_root中的children_未找到key
  // 1. 如果当前的key已经是最后的元素，插入一个带value的节点
  // 2. 如果当前的key不是最后的元素， 插入一个不带value的节点, 然后继续递归往下插入
  char c = key.at(0);
  if (key.size() == 1) {
    new_root->children_.insert({c, std::make_shared<TrieNodeWithValue<T>>(std::make_shared<T>(std::move(value)))});
  } else {
    auto ptr = std::make_shared<TrieNode>();
    Putcycle<T>(ptr, key.substr(1), std::move(value));
    new_root->children_.insert({c, ptr});
  }
}
/*
思路流程：
1. 复制当前的Trie根节点，创建新的Trie实例
2. 遍历key中的每一个字符，将字符对应节点加入Trie中。 遍历完之后， 更新叶子节点的值。
3. 返回新的Trie实例。
*/
template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
  // throw NotImplementedException("Trie::Put is not implemented.");

  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.
  if (key.empty()) {
    // key为空的情况下，一定要创建一个带value的节点， 这里需要判断root是否有children来区分情况
    std::unique_ptr<TrieNodeWithValue<T>> new_root = nullptr;
    if (root_->children_.empty()) {
      new_root = std::make_unique<TrieNodeWithValue<T>>(std::move(std::make_shared<T>(std::move(value))));
    } else {
      new_root =
          std::make_unique<TrieNodeWithValue<T>>(root_->children_, std::move(std::make_shared<T>(std::move(value))));
    }
    return Trie(std::move(new_root));
  }

  // 如果key不为空，值不放在根节点
  // 根节点如果为空，创建一个空的TrieNode
  // 根节点不为空， Clone根节点
  std::shared_ptr<TrieNode> new_root = nullptr;
  if (root_ == nullptr) {
    new_root = std::make_shared<TrieNode>();
  } else {
    new_root = root_->Clone();
  }

  // 递归插入， 传递根节点, key和value
  Putcycle<T>(new_root, key, std::move(value));
  return Trie(std::move(new_root));
}

/*
思路流程：
1. 创建新的Trie实例， 复制当前Trie的根节点。
2. 使用栈记录遍历路径中的每个节点和对应的字符。
3. 找到目标节点之后，删除其存储值。
4. 从路径的最后一个节点向上清理无用的父节点。 如果某个节点没有子节点并且不存储值，则将其从父节点的子节点中删除。
5. 返回包含删除操作结果的Trie副本。
*/
auto Trie::Remove(std::string_view key) const -> Trie {
  // throw NotImplementedException("Trie::Remove is not implemented.");

  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.
  if (!this->root_) {
    return *this;
  }
  auto new_root = std::shared_ptr<TrieNode>(root_->Clone());
  auto current = new_root.get();
  if (!root_->is_value_node_) {
    current->is_value_node_ = false;
  }
  std::vector<std::pair<char, TrieNode *>> stacks;

  if (key.empty() && new_root->is_value_node_) {
    // key为空的情况下，一定要创建一个带value的节点， 这里需要判断root是否有children来区分情况
    new_root->is_value_node_ = false;
    return Trie(new_root);
  }

  for (char ch : key) {
    auto it = current->children_.find(ch);
    if (it == current->children_.end()) {
      return *this;
    }

    auto new_node = std::shared_ptr<TrieNode>(it->second->Clone());
    new_node->is_value_node_ = it->second->is_value_node_;
    current->children_[ch] = new_node;
    stacks.emplace_back(ch, current);
    current = new_node.get();
  }

  if (current->is_value_node_) {
    current->is_value_node_ = false;
  } else {
    return *this;
  }

  while (!stacks.empty()) {
    auto [ch, parent] = stacks.back();
    stacks.pop_back();
    auto child_it = parent->children_.find(ch);
    if (child_it != parent->children_.end() && child_it->second->children_.empty() &&
        !child_it->second->is_value_node_) {
      parent->children_.erase(child_it);
    } else {
      break;
    }
  }

  if (!new_root->is_value_node_ && new_root->children_.empty()) {
    new_root = nullptr;
  }
  return Trie(new_root);
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub
