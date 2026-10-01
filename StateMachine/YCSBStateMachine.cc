#include "YCSBStateMachine.h"

namespace {

// Parse "<owner>[#<row>]": returns the owner (YCSB_NO_OWNER if malformed) and
// sets row to what follows the first '#', if any.
uint64_t ParseOwner(const Value& v, std::string* row = nullptr) {
   if (v.get_kind() != Value::STR) {
      return YCSB_NO_OWNER;
   }
   const std::string& s = v.get_str();
   size_t sep = s.find('#');
   if (row != nullptr) {
      *row = (sep == std::string::npos) ? "" : s.substr(sep + 1);
   }
   try {
      return std::stoull(s.substr(0, sep));
   } catch (...) {
      return YCSB_NO_OWNER;
   }
}

}  // namespace

YCSBStateMachine::YCSBStateMachine(const uint32_t shardId,
                                   const uint32_t replicaId,
                                   const uint32_t shardNum,
                                   const uint32_t replicaNum,
                                   const YAML::Node& config)
    : StateMachine(shardId, replicaId, shardNum, replicaNum, config) {
   kvStore_.resize(YCSB_MAX_KEY_NUM);
   lockOwner_.resize(YCSB_MAX_KEY_NUM, YCSB_NO_OWNER);
}

std::string YCSBStateMachine::RTTI() { return "YCSBStateMachine"; }

uint32_t YCSBStateMachine::MappedRecordId(const int32_t key) {
   uint32_t int_key = key;
   return int_key / shardNum_ + YCSB_MAX_KEY_NUM / shardNum_ * shardId_;
}

std::string& YCSBStateMachine::Row(const uint32_t mappedRecordId) {
   uint32_t fieldId = 0;
   if (fieldId >= kvStore_[mappedRecordId].size()) {
      kvStore_[mappedRecordId].resize(fieldId + 1, "");
   }
   return kvStore_[mappedRecordId][fieldId];
}

void YCSBStateMachine::InitializeRelatedShards(
    const uint32_t txnType, std::map<int32_t, Value>* ws,
    std::map<uint32_t, std::set<int32_t>>* shardKeyMap) {
   shardKeyMap->clear();
   for (auto& kv : *ws) {
      uint32_t cell_key = kv.first;
      uint32_t int_key = cell_key;
      (*shardKeyMap)[int_key % shardNum_].insert(cell_key);
   }
}

void YCSBStateMachine::Execute(const uint32_t txnType,
                               const std::vector<int32_t>* localKeys,
                               std::map<int32_t, Value>* input,
                               std::map<int32_t, Value>* output,
                               const uint64_t txnId) {
   output->clear();
   for (auto& key : (*localKeys)) {
      uint32_t mappedRecordId = MappedRecordId(key);
      std::string& row = Row(mappedRecordId);
      uint64_t& lockOwner = lockOwner_[mappedRecordId];

      if (txnType == YCSB_TXN_TYPE::YCSB_READ) {
         (*output)[key].set_str(row);
      } else if (txnType == YCSB_TXN_TYPE::YCSB_UPDATE ||
                 txnType == YCSB_TXN_TYPE::YCSB_INSERT) {
         if (lockOwner != YCSB_NO_OWNER) {
            // Locked by a decomposed transaction: skip the write
            (*output)[YCSB_LOCK_CONFLICT_FLAG].set_i32(1);
            continue;
         }
         row = (*input)[key].get_str();
         (*output)[key].set_str(row);
      } else if (txnType == YCSB_TXN_TYPE::YCSB_SWAP) {
         std::vector<std::pair<int32_t, std::string>> sortedKeys;
         for (auto& kv : *input) {
            sortedKeys.push_back({kv.first, kv.second.get_str()});
         }
         std::sort(sortedKeys.begin(), sortedKeys.end());
         for (size_t i = 0; i < sortedKeys.size(); i++) {
            if (sortedKeys[i].first == key) {
               size_t prev = (i + sortedKeys.size() - 1) % sortedKeys.size();
               row = sortedKeys[prev].second;
               (*output)[key].set_str(row);
               break;
            }
         }
      } else if (txnType == YCSB_TXN_TYPE::YCSB_LOCK_READ) {
         uint64_t owner = ParseOwner((*input)[key]);
         if (owner != YCSB_NO_OWNER &&
             (lockOwner == YCSB_NO_OWNER || lockOwner == owner)) {
            lockOwner = owner;
            (*output)[key].set_str(YCSB_LOCK_OK + row);
         } else {
            (*output)[key].set_str(YCSB_LOCK_FAILED);
         }
      } else if (txnType == YCSB_TXN_TYPE::YCSB_WRITE_UNLOCK) {
         std::string newRow;
         uint64_t owner = ParseOwner((*input)[key], &newRow);
         if (owner != YCSB_NO_OWNER && lockOwner == owner) {
            row = newRow;
            lockOwner = YCSB_NO_OWNER;
            (*output)[key].set_str(YCSB_LOCK_OK);
         } else {
            (*output)[key].set_str(YCSB_LOCK_FAILED);
         }
      } else if (txnType == YCSB_TXN_TYPE::YCSB_UNLOCK) {
         uint64_t owner = ParseOwner((*input)[key]);
         if (owner != YCSB_NO_OWNER && lockOwner == owner) {
            lockOwner = YCSB_NO_OWNER;
         }
         (*output)[key].set_str(YCSB_LOCK_OK);
      }
   }
}

void YCSBStateMachine::SpecExecute(const uint32_t txnType,
                                   const std::vector<int32_t>* localKeys,
                                   std::map<int32_t, Value>* input,
                                   std::map<int32_t, Value>* output,
                                   const uint64_t txnId) {
   // Save the state of the local records (row and lock), then execute
   for (auto& key : *localKeys) {
      uint32_t mappedRecordId = MappedRecordId(key);
      undo_[key] = {txnId, Row(mappedRecordId), lockOwner_[mappedRecordId]};
   }
   Execute(txnType, localKeys, input, output, txnId);
}

void YCSBStateMachine::CommitExecute(const uint32_t txnType,
                                     const std::vector<int32_t>* localKeys,
                                     std::map<int32_t, Value>* input,
                                     std::map<int32_t, Value>* output,
                                     const uint64_t txnId) {
   // The speculative execution is already applied: drop the undo records
   for (auto& key : *localKeys) {
      assert(undo_[key].txnId_ == txnId);
      undo_.erase(key);
   }
}

void YCSBStateMachine::RollbackExecute(const uint32_t txnType,
                                       const std::vector<int32_t>* localKeys,
                                       std::map<int32_t, Value>* input,
                                       std::map<int32_t, Value>* output,
                                       const uint64_t txnId) {
   output->clear();
   for (auto& key : *localKeys) {
      auto it = undo_.find(key);
      if (it == undo_.end()) {
         continue;
      }
      assert(it->second.txnId_ == txnId);
      uint32_t mappedRecordId = MappedRecordId(key);
      Row(mappedRecordId) = it->second.row_;
      lockOwner_[mappedRecordId] = it->second.owner_;
      undo_.erase(it);
   }
}

void YCSBStateMachine::PreRead(const uint32_t txnType,
                                const std::map<int32_t, Value>* input,
                                std::map<int32_t, Value>* output) {
   output->clear();
   if (txnType == YCSB_TXN_TYPE::YCSB_SWAP) {
      for (auto& kv : *input) {
         int32_t key = kv.first;
         uint32_t int_key = key;
         // The dispatch request carries all the keys of the transaction: only
         // answer for the ones stored on this shard.
         if (int_key % shardNum_ != shardId_) {
            continue;
         }
         (*output)[key].set_str(Row(MappedRecordId(key)));
      }
   }
}

uint32_t YCSBStateMachine::TotalNumberofKeys() { return YCSB_MAX_KEY_NUM; }

YCSBStateMachine::~YCSBStateMachine() {}
