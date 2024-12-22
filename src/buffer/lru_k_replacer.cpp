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

LRUKNode::LRUKNode() {}

LRUKNode::LRUKNode(frame_id_t id) : fid_(id) {}

void LRUKNode::SetNodeHistory(size_t seconds) {
    history_.push_back(seconds);
}

bool LRUKNode::GetNodeEvictable() {
    return is_evictable_;
}

void LRUKNode::SetNodeEvictable(bool evictable) {
    is_evictable_ = evictable;
}

size_t LRUKNode::GetHistorySize() {
    return history_.size();
}

size_t LRUKNode::GetFirstTimeStamp() {
    return history_.front();
}

size_t LRUKNode::GetFrameId() const{
    return fid_;
}

void LRUKNode::SetBackDistance() {
    k_ = history_.back() - history_.front();
}

size_t LRUKNode::GetBackDistance() {
    return k_;
}

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict() -> std::optional<frame_id_t> { return std::nullopt; }

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
    if ((size_t)frame_id > replacer_size_) {
        throw Exception("frame id is invalid");
    }

    auto now = std::chrono::system_clock::now();
    auto epoch = std::chrono::system_clock::to_time_t(now);

    if (node_store_.find(frame_id) == node_store_.end()) {
        LRUKNode node(frame_id);
        node_store_[frame_id] = std::move(node);
        node_store_[frame_id].SetNodeHistory(epoch);
    }
    else {
        node_store_[frame_id].SetNodeHistory(epoch);
    }
}

void LRUKReplacer::AddNodeToList(std::unordered_map<frame_id_t, LRUKNode>::iterator &iter) {
    if (iter->second.GetHistorySize() >= k_) {
        hot_list_.push_back(iter->second);
    }
    else {
        clod_list_.push_back(iter->second);
    }
}

void LRUKReplacer::DeleteNodeFromList(std::unordered_map<frame_id_t, LRUKNode>::iterator &iter) {
    if (iter->second.GetHistorySize() >= k_) {
        hot_list_.remove_if([&iter](const LRUKNode &n) {
            return iter->second.GetFrameId() == n.GetFrameId();
        });
    }
    else {
        clod_list_.remove_if([&iter](const LRUKNode &n) {
            return iter->second.GetFrameId() == n.GetFrameId();
        });
    }    
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
    auto iter = node_store_.find(frame_id);
    if (iter != node_store_.end())
    {
        bool previous_evictable = iter->second.GetNodeEvictable();
        iter->second.SetNodeEvictable(set_evictable);
        if (set_evictable && !previous_evictable) {
            curr_size_++;
            AddNodeToList(iter);
        }
        else if (!set_evictable && previous_evictable) {
            curr_size_--;
            DeleteNodeFromList(iter);
        }
    }
    else {
        throw Exception("frame id is invalid");
    }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {}

auto LRUKReplacer::Size() -> size_t {
    return curr_size_; 
}

}  // namespace bustub
