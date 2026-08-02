#include "YCSBStateMachine.h"

YCSBStateMachine::YCSBStateMachine(const uint32_t shardId,
                                   const uint32_t replicaId,
                                   const uint32_t shardNum,
                                   const uint32_t replicaNum,
                                   const YAML::Node& config)
    : StateMachine(shardId, replicaId, shardNum, replicaNum, config) {
   kvStore_.resize(YCSB_MAX_KEY_NUM);
}

std::string YCSBStateMachine::RTTI() { return "YCSBStateMachine"; }

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
      uint32_t int_key = key;
      uint32_t fieldId = 0;
      uint32_t mappedRecordId =
          int_key / shardNum_ + YCSB_MAX_KEY_NUM / shardNum_ * shardId_;
      
      if (fieldId >= kvStore_[mappedRecordId].size()) {
         kvStore_[mappedRecordId].resize(fieldId + 1, "");
      }

      if (txnType == YCSB_TXN_TYPE::YCSB_READ) {
         (*output)[key].set_str(kvStore_[mappedRecordId][fieldId]);
      } else if (txnType == YCSB_TXN_TYPE::YCSB_UPDATE || txnType == YCSB_TXN_TYPE::YCSB_INSERT) {
         std::string newValue = (*input)[key].get_str();
         kvStore_[mappedRecordId][fieldId] = newValue;
         (*output)[key].set_str(newValue);
      }
   }
}

void YCSBStateMachine::SpecExecute(const uint32_t txnType,
                                   const std::vector<int32_t>* localKeys,
                                   std::map<int32_t, Value>* input,
                                   std::map<int32_t, Value>* output,
                                   const uint64_t txnId) {
   output->clear();
   for (auto& key : *localKeys) {
      uint32_t int_key = key;
      uint32_t fieldId = 0;
      uint32_t mappedRecordId =
          int_key / shardNum_ + YCSB_MAX_KEY_NUM / shardNum_ * shardId_;
      
      if (fieldId >= kvStore_[mappedRecordId].size()) {
         kvStore_[mappedRecordId].resize(fieldId + 1, "");
      }

      std::string newValue = (*input)[key].get_str();
      speculativeVersion_[key] = {txnId, newValue};
      kvStore_[mappedRecordId][fieldId] = newValue;
      (*output)[key].set_str(newValue);
   }
}

void YCSBStateMachine::CommitExecute(const uint32_t txnType,
                                     const std::vector<int32_t>* localKeys,
                                     std::map<int32_t, Value>* input,
                                     std::map<int32_t, Value>* output,
                                     const uint64_t txnId) {
   for (auto& key : *localKeys) {
      uint32_t int_key = key;
      uint32_t fieldId = 0;
      uint32_t mappedRecordId =
          int_key / shardNum_ + YCSB_MAX_KEY_NUM / shardNum_ * shardId_;
      
      if (fieldId >= kvStore_[mappedRecordId].size()) {
         kvStore_[mappedRecordId].resize(fieldId + 1, "");
      }

      assert(speculativeVersion_[key].txnId_ == txnId);
      kvStore_[mappedRecordId][fieldId] = speculativeVersion_[key].value_;
      speculativeVersion_.erase(key);
   }
}

void YCSBStateMachine::RollbackExecute(const uint32_t txnType,
                                       const std::vector<int32_t>* localKeys,
                                       std::map<int32_t, Value>* input,
                                       std::map<int32_t, Value>* output,
                                       const uint64_t txnId) {
   output->clear();
   for (auto& key : *localKeys) {
      speculativeVersion_.erase(key);
   }
}

uint32_t YCSBStateMachine::TotalNumberofKeys() { return YCSB_MAX_KEY_NUM; }

YCSBStateMachine::~YCSBStateMachine() {}
