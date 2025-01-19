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
#include "common/exception.h"

namespace bustub {

LRUKNode::LRUKNode(frame_id_t id) : fid_(id) {}

void LRUKNode::SetAccessHistory(size_t seconds) { history_.push_back(seconds); }

auto LRUKNode::GetNodeEvictable() -> bool { return is_evictable_; }

void LRUKNode::SetNodeEvictable(bool evictable) { is_evictable_ = evictable; }

auto LRUKNode::GetHistorySize() const -> size_t { return history_.size(); }

auto LRUKNode::GetFirstTimeStamp() const -> size_t { return history_.front(); }

auto LRUKNode::GetFrameId() const -> size_t { return fid_; }

void LRUKNode::SetBackwardDistance() { k_ = history_.back() - history_.front(); }

auto LRUKNode::GetBackwardDistance() const -> size_t { return k_; }

void LRUKNode::ClearHistory() { history_.clear(); }

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::EvictFromList(std::list<LRUKNode> &node_list) -> std::optional<frame_id_t> {
  LRUKNode &node = node_list.front();
  frame_id_t fid = node.GetFrameId();
  node_store_.erase(fid);
  node.ClearHistory();
  node_list.pop_front();
  curr_size_--;
  return fid;
}

auto LRUKReplacer::Evict() -> std::optional<frame_id_t> {
  std::scoped_lock slk(latch_);

  std::optional<frame_id_t> result;
  if (!cold_list_.empty()) {
    result = EvictFromList(cold_list_);
  } else if (!hot_list_.empty()) {
    result = EvictFromList(hot_list_);
  }

  return result;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  if (static_cast<size_t>(frame_id) > replacer_size_) {
    throw Exception("frame id is invalid");
  }

  std::scoped_lock slk(latch_);
  auto now = std::chrono::system_clock::now();
  current_timestamp_ = std::chrono::system_clock::to_time_t(now);

  if (node_store_.find(frame_id) == node_store_.end()) {
    LRUKNode node(frame_id);
    node_store_[frame_id] = node;
    node_store_[frame_id].SetAccessHistory(current_timestamp_);
  } else {
    node_store_[frame_id].SetAccessHistory(current_timestamp_);

    if (node_store_[frame_id].GetHistorySize() >= k_) {
      node_store_[frame_id].SetBackwardDistance();
    }

    MoveNodeFromList(frame_id);
  }
}

void LRUKReplacer::AddNodeToTail(frame_id_t fid) {
  auto iter = node_store_.find(fid);
  if (iter->second.GetHistorySize() >= k_) {
    hot_list_.push_back(iter->second);
  } else {
    cold_list_.push_back(iter->second);
  }
}

void LRUKReplacer::DeleteNodeFromList(frame_id_t fid) {
  auto iter = node_store_.find(fid);
  if (iter->second.GetHistorySize() >= k_) {
    hot_list_.remove_if([&iter](const LRUKNode &n) { return iter->second.GetFrameId() == n.GetFrameId(); });
  } else {
    cold_list_.remove_if([&iter](const LRUKNode &n) { return iter->second.GetFrameId() == n.GetFrameId(); });
  }
}

auto LRUKReplacer::CompareDesc(LRUKNode &a, LRUKNode &b) -> bool {
  return a.GetBackwardDistance() > b.GetBackwardDistance();
}

void LRUKReplacer::MoveNodeFromList(frame_id_t fid) {
  auto iter = node_store_.find(fid);

  if (!iter->second.GetNodeEvictable()) {
    return;
  }

  if (iter->second.GetHistorySize() >= k_) {
    if (iter->second.GetHistorySize() == k_) {
      cold_list_.remove_if([&iter](const LRUKNode &n) { return iter->second.GetFrameId() == n.GetFrameId(); });

      AddNodeToTail(fid);
    }

    hot_list_.sort(CompareDesc);
  } else {
    DeleteNodeFromList(fid);

    AddNodeToTail(fid);
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock slk(latch_);
  auto iter = node_store_.find(frame_id);
  if (iter != node_store_.end()) {
    bool previous_evictable = iter->second.GetNodeEvictable();
    iter->second.SetNodeEvictable(set_evictable);
    if (set_evictable && !previous_evictable) {
      curr_size_++;
      AddNodeToTail(frame_id);
    } else if (!set_evictable && previous_evictable) {
      curr_size_--;
      DeleteNodeFromList(frame_id);
    }
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock slk(latch_);
  auto iter = node_store_.find(frame_id);
  if (iter != node_store_.end()) {
    if (!iter->second.GetNodeEvictable()) {
      throw Exception("frame is not evictable");
    }

    DeleteNodeFromList(frame_id);
    node_store_.erase(frame_id);
    curr_size_--;
  }
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
