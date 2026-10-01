#pragma once
#include "StateMachine.h"

#define YCSB_MAX_KEY_NUM (2000005)

enum YCSB_TXN_TYPE {
   YCSB_READ = 1,
   YCSB_UPDATE,
   YCSB_INSERT,
   YCSB_SWAP = 4,
   // Locking one-shot transactions, used to decompose a dependent transaction
   // (e.g., swap) into two one-shot ones (Tiga technical report, Figure 16).
   // The value of each key in the write-set starts with the lock owner id.
   YCSB_LOCK_READ = 5,     // ws_[k] = "<owner>"
   YCSB_WRITE_UNLOCK = 6,  // ws_[k] = "<owner>#<new row>"
   YCSB_UNLOCK = 7,        // ws_[k] = "<owner>"
};

// Results of the locking transactions: "1" (+ row for YCSB_LOCK_READ) if the
// lock is held by the owner, "0" otherwise.
#define YCSB_LOCK_OK "1"
#define YCSB_LOCK_FAILED "0"
// Result key set by YCSB_UPDATE/YCSB_INSERT when a key is locked by another
// owner (the write is then skipped). Record keys are non-negative.
#define YCSB_LOCK_CONFLICT_FLAG (-1)
#define YCSB_NO_OWNER (0)

#include <unordered_map>

// State of a record before a speculative execution, restored on rollback.
struct YCSBUndoRecord {
   uint64_t txnId_ = UINT64_MAX;
   std::string row_ = "";
   uint64_t owner_ = YCSB_NO_OWNER;
};

class YCSBStateMachine : public StateMachine {
  private:
   std::vector<std::vector<std::string>> kvStore_;
   // Owner of the (exclusive) lock of each record, YCSB_NO_OWNER if free
   std::vector<uint64_t> lockOwner_;
   // Speculative execution support: at most one speculative transaction per
   // key at a time
   std::unordered_map<int32_t, YCSBUndoRecord> undo_;

   uint32_t MappedRecordId(const int32_t key);
   std::string& Row(const uint32_t mappedRecordId);

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

    void PreRead(const uint32_t txnType,
                 const std::map<int32_t, Value>* input,
                 std::map<int32_t, Value>* output) override;

    uint32_t TotalNumberofKeys() override;
    ~YCSBStateMachine();
};
