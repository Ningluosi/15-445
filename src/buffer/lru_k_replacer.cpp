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

LRUKNode::LRUKNode(size_t k, frame_id_t id) : k_(k), fid_(id) {}

bool LRUKNode::getNodeEvictable() {
    return is_evictable_;
}

void LRUKNode::setNodeEvictable(bool evictable) {
    is_evictable_ = evictable;
}

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict() -> std::optional<frame_id_t> { return std::nullopt; }

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
    if ((size_t)frame_id > replacer_size_) {
        throw Exception("frame id is invalid");
    }

    if (node_store_.find(frame_id) == node_store_.end()) {
        LRUKNode node(k_, frame_id);
        node_store_[frame_id] = std::move(node);
    }   
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
    auto iter = node_store_.find(frame_id);
    if (iter != node_store_.end())
    {
        bool previous_evictable = iter->second.getNodeEvictable();
        iter->second.setNodeEvictable(set_evictable);
        if (set_evictable && !previous_evictable) {
            curr_size_++;
        }
        else if (!set_evictable && previous_evictable) {
            curr_size_--;
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
