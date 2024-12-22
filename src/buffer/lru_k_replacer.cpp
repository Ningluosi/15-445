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

void LRUKNode::SetAccessHistory(size_t seconds) {
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

void LRUKNode::SetBackwardDistance() {
    k_ = history_.back() - history_.front();
}

size_t LRUKNode::GetBackwardDistance() {
    return k_;
}

void LRUKNode::ClearHistory() {
    history_.clear();
}

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict() -> std::optional<frame_id_t> {
    std::optional<frame_id_t> fid;
    frame_id_t tmpId;

    if (clod_list_.size() > 0) {
        tmpId = clod_list_.front().GetFrameId();
        fid = frame_id_t(tmpId);
        node_store_.erase(tmpId);
        clod_list_.front().ClearHistory();
        clod_list_.pop_front();
        curr_size_--;
        return fid;
    }
    else if (hot_list_.size() > 0) {
        tmpId = hot_list_.front().GetFrameId();
        fid = frame_id_t(tmpId);
        node_store_.erase(tmpId);
        hot_list_.front().ClearHistory();
        hot_list_.pop_front();
        curr_size_--;
        return fid;
    }

    return std::nullopt;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
    if ((size_t)frame_id > replacer_size_) {
        throw Exception("frame id is invalid");
    }

    auto now = std::chrono::system_clock::now();
    current_timestamp_ = std::chrono::system_clock::to_time_t(now);

    if (node_store_.find(frame_id) == node_store_.end()) {
        LRUKNode node(frame_id);
        node_store_[frame_id] = std::move(node);
        node_store_[frame_id].SetAccessHistory(current_timestamp_);
    }
    else {
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
    }
    else {
        clod_list_.push_back(iter->second);
    }
}

void LRUKReplacer::DeleteNodeFromList(frame_id_t fid) {
    auto iter = node_store_.find(fid);
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

bool LRUKReplacer::CompareDesc(LRUKNode &a, LRUKNode &b) {
    return a.GetBackwardDistance() > b.GetBackwardDistance();
}

void LRUKReplacer::MoveNodeFromList(frame_id_t fid) {
    auto iter = node_store_.find(fid);

    if (!iter->second.GetNodeEvictable()) {
        return;
    }

    if (iter->second.GetHistorySize() >= k_) {
        if (iter->second.GetHistorySize() == k_) {
            clod_list_.remove_if([&iter](const LRUKNode &n) {
                return iter->second.GetFrameId() == n.GetFrameId();
            });

            AddNodeToTail(fid);
        }

        hot_list_.sort(CompareDesc);
    }
    else {
        DeleteNodeFromList(fid);
    
        AddNodeToTail(fid);
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
            AddNodeToTail(frame_id);
        }
        else if (!set_evictable && previous_evictable) {
            curr_size_--;
            DeleteNodeFromList(frame_id);
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
