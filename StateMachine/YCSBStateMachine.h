#pragma once
#include "StateMachine.h"

#define YCSB_MAX_KEY_NUM (2000005)

enum YCSB_TXN_TYPE {
   YCSB_READ = 1,
   YCSB_UPDATE,
   YCSB_INSERT
};

class YCSBStateMachine : public StateMachine {
  private:
   uint32_t kvStore_[YCSB_MAX_KEY_NUM];
   // Speculative execution support
   VersionInfo speculativeVersion_[YCSB_MAX_KEY_NUM];

  public:
   YCSBStateMachine(const uint32_t shardId, const uint32_t replicaId,
                    const uint32_t shardNum, const uint32_t replicaNum,
                    const YAML::Node& config);
   std::string RTTI() override;

   void Execute(const uint32_t txnType, const std::vector<int32_t>* localKeys,
                std::map<int32_t, Value>* input,
                std::map<int32_t, Value>* output,
                const uint64_t txnId = 0) override;
   void SpecExecute(const uint32_t txnType,
                    const std::vector<int32_t>* localKeys,
                    std::map<int32_t, Value>* input,
                    std::map<int32_t, Value>* output,
                    const uint64_t txnId = 0) override;
   void CommitExecute(const uint32_t txnType,
                      const std::vector<int32_t>* localKeys,
                      std::map<int32_t, Value>* input,
                      std::map<int32_t, Value>* output,
                      const uint64_t txnId = 0) override;
   void RollbackExecute(const uint32_t txnType,
                        const std::vector<int32_t>* localKeys,
                        std::map<int32_t, Value>* input,
                        std::map<int32_t, Value>* output,
                        const uint64_t txnId = 0) override;

   void InitializeRelatedShards(
       const uint32_t txnType, std::map<int32_t, Value>* ws,
       std::map<uint32_t, std::set<int32_t>>* shardKeyMap) override;

   uint32_t TotalNumberofKeys() override;
   ~YCSBStateMachine();
};
