#include "YCSBStateMachine.h"

YCSBStateMachine::YCSBStateMachine(const uint32_t shardId,
                                   const uint32_t replicaId,
                                   const uint32_t shardNum,
                                   const uint32_t replicaNum,
                                   const YAML::Node& config)
    : StateMachine(shardId, replicaId, shardNum, replicaNum, config) {
   memset(kvStore_, '\0', sizeof(uint32_t) * YCSB_MAX_KEY_NUM);
}

std::string YCSBStateMachine::RTTI() { return "YCSBStateMachine"; }

void YCSBStateMachine::InitializeRelatedShards(
    const uint32_t txnType, std::map<int32_t, Value>* ws,
    std::map<uint32_t, std::set<int32_t>>* shardKeyMap) {
   shardKeyMap->clear();
   for (auto& kv : *ws) {
      uint32_t key = kv.first;
      (*shardKeyMap)[key % shardNum_].insert(key);
   }
}

void YCSBStateMachine::Execute(const uint32_t txnType,
                               const std::vector<int32_t>* localKeys,
                               std::map<int32_t, Value>* input,
                               std::map<int32_t, Value>* output,
                               const uint64_t txnId) {
   output->clear();
   for (auto& key : (*localKeys)) {
      uint32_t mappedKeyId =
          key / shardNum_ + YCSB_MAX_KEY_NUM / shardNum_ * shardId_;
      if (txnType == YCSB_TXN_TYPE::YCSB_READ) {
         (*output)[key].set_i32(kvStore_[mappedKeyId]);
      } else if (txnType == YCSB_TXN_TYPE::YCSB_UPDATE || txnType == YCSB_TXN_TYPE::YCSB_INSERT) {
         kvStore_[mappedKeyId]++;
         (*output)[key].set_i32(kvStore_[mappedKeyId]);
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
      uint32_t mappedKeyId =
          key / shardNum_ + YCSB_MAX_KEY_NUM / shardNum_ * shardId_;
      uint32_t value = kvStore_[mappedKeyId] + 1;
      speculativeVersion_[mappedKeyId] = {txnId, value};
      kvStore_[mappedKeyId]++;
      (*output)[key].set_i32(value);
   }
}

void YCSBStateMachine::CommitExecute(const uint32_t txnType,
                                     const std::vector<int32_t>* localKeys,
                                     std::map<int32_t, Value>* input,
                                     std::map<int32_t, Value>* output,
                                     const uint64_t txnId) {
   for (auto& key : *localKeys) {
      uint32_t mappedKeyId =
          key / shardNum_ + YCSB_MAX_KEY_NUM / shardNum_ * shardId_;
      assert(speculativeVersion_[mappedKeyId].txnId_ == txnId);
      kvStore_[mappedKeyId] = speculativeVersion_[mappedKeyId].value_;
      speculativeVersion_[mappedKeyId] = {UINT64_MAX, UINT32_MAX};
   }
}

void YCSBStateMachine::RollbackExecute(const uint32_t txnType,
                                       const std::vector<int32_t>* localKeys,
                                       std::map<int32_t, Value>* input,
                                       std::map<int32_t, Value>* output,
                                       const uint64_t txnId) {
   output->clear();
   for (auto& key : *localKeys) {
      uint32_t mappedKeyId =
          key / shardNum_ + YCSB_MAX_KEY_NUM / shardNum_ * shardId_;
      speculativeVersion_[mappedKeyId] = {UINT64_MAX, UINT32_MAX};
   }
}

uint32_t YCSBStateMachine::TotalNumberofKeys() { return YCSB_MAX_KEY_NUM; }

YCSBStateMachine::~YCSBStateMachine() {}
