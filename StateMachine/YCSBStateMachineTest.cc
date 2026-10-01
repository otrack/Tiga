// Tests of the locks of YCSBStateMachine (decomposed swap transactions).
#include "YCSBStateMachine.h"

#include <iostream>

namespace {

int failures = 0;

#define CHECK_EQ(a, b)                                                    \
   do {                                                                   \
      auto va = (a);                                                      \
      auto vb = (b);                                                      \
      if (!(va == vb)) {                                                  \
         std::cerr << __FILE__ << ":" << __LINE__ << ": " << #a << " = " \
                   << va << ", expected " << vb << std::endl;             \
         failures++;                                                      \
      }                                                                   \
   } while (0)

// A single-shard state machine, so that all keys are local
struct Fixture {
   YCSBStateMachine sm_{0, 0, 1, 1, YAML::Node()};

   std::map<int32_t, Value> Run(uint32_t txnType,
                                const std::map<int32_t, std::string>& ws,
                                bool spec = false, uint64_t txnId = 0) {
      std::map<int32_t, Value> input;
      std::vector<int32_t> keys;
      for (auto& kv : ws) {
         input[kv.first].set_str(kv.second);
         keys.push_back(kv.first);
      }
      std::map<int32_t, Value> output;
      if (spec) {
         sm_.SpecExecute(txnType, &keys, &input, &output, txnId);
      } else {
         sm_.Execute(txnType, &keys, &input, &output, txnId);
      }
      return output;
   }

   void Rollback(const std::vector<int32_t>& keys, uint64_t txnId) {
      std::map<int32_t, Value> input, output;
      sm_.RollbackExecute(0, &keys, &input, &output, txnId);
   }

   void Commit(const std::vector<int32_t>& keys, uint64_t txnId) {
      std::map<int32_t, Value> input, output;
      sm_.CommitExecute(0, &keys, &input, &output, txnId);
   }

   std::string Get(int32_t key) {
      auto out = Run(YCSB_READ, {{key, ""}});
      return out[key].get_str();
   }
};

void TestLockedSwap() {
   Fixture f;
   f.Run(YCSB_INSERT, {{1, "a"}, {2, "b"}, {3, "c"}});

   // T1 of owner 7 locks and reads
   auto r = f.Run(YCSB_LOCK_READ, {{1, "7"}, {2, "7"}, {3, "7"}});
   CHECK_EQ(r[1].get_str(), std::string("1a"));
   CHECK_EQ(r[3].get_str(), std::string("1c"));

   // Another owner cannot lock, nor update, the locked keys
   r = f.Run(YCSB_LOCK_READ, {{3, "8"}, {4, "8"}});
   CHECK_EQ(r[3].get_str(), std::string("0"));
   CHECK_EQ(r[4].get_str(), std::string("1"));
   r = f.Run(YCSB_UPDATE, {{2, "x"}});
   CHECK_EQ(r.count(YCSB_LOCK_CONFLICT_FLAG), 1u);
   CHECK_EQ(f.Get(2), std::string("b"));

   // Owner 8 aborts: releases its locks only
   f.Run(YCSB_UNLOCK, {{3, "8"}, {4, "8"}});
   r = f.Run(YCSB_LOCK_READ, {{3, "9"}});
   CHECK_EQ(r[3].get_str(), std::string("0"));
   r = f.Run(YCSB_LOCK_READ, {{4, "9"}});
   CHECK_EQ(r[4].get_str(), std::string("1"));

   // T2 of owner 7 writes the rotated values and unlocks
   r = f.Run(YCSB_WRITE_UNLOCK, {{1, "7#c"}, {2, "7#a"}, {3, "7#b"}});
   CHECK_EQ(r[1].get_str(), std::string("1"));
   CHECK_EQ(f.Get(1), std::string("c"));
   CHECK_EQ(f.Get(2), std::string("a"));
   CHECK_EQ(f.Get(3), std::string("b"));

   // The locks are free again; a write-unlock without the lock fails
   r = f.Run(YCSB_WRITE_UNLOCK, {{1, "7#z"}});
   CHECK_EQ(r[1].get_str(), std::string("0"));
   CHECK_EQ(f.Get(1), std::string("c"));
   r = f.Run(YCSB_UPDATE, {{1, "y"}});
   CHECK_EQ(r.count(YCSB_LOCK_CONFLICT_FLAG), 0u);
   CHECK_EQ(f.Get(1), std::string("y"));
}

void TestSpeculation() {
   Fixture f;
   f.Run(YCSB_INSERT, {{1, "a"}});

   // A rolled back speculative lock-read releases the lock
   f.Run(YCSB_LOCK_READ, {{1, "7"}}, true, 100);
   f.Rollback({1}, 100);
   auto r = f.Run(YCSB_LOCK_READ, {{1, "8"}});
   CHECK_EQ(r[1].get_str(), std::string("1a"));

   // A rolled back speculative write-unlock restores the row and the lock
   f.Run(YCSB_WRITE_UNLOCK, {{1, "8#b"}}, true, 101);
   CHECK_EQ(f.Get(1), std::string("b"));
   f.Rollback({1}, 101);
   CHECK_EQ(f.Get(1), std::string("a"));
   r = f.Run(YCSB_LOCK_READ, {{1, "9"}});
   CHECK_EQ(r[1].get_str(), std::string("0"));

   // A committed speculative write-unlock is kept
   f.Run(YCSB_WRITE_UNLOCK, {{1, "8#c"}}, true, 102);
   f.Commit({1}, 102);
   CHECK_EQ(f.Get(1), std::string("c"));
   r = f.Run(YCSB_LOCK_READ, {{1, "9"}});
   CHECK_EQ(r[1].get_str(), std::string("1c"));

   // Speculative reads do not modify the store
   Fixture g;
   g.Run(YCSB_INSERT, {{5, "v"}});
   std::map<int32_t, Value> input, output;
   input[5].set_i32(0);
   std::vector<int32_t> keys = {5};
   g.sm_.SpecExecute(YCSB_READ, &keys, &input, &output, 103);
   CHECK_EQ(output[5].get_str(), std::string("v"));
   g.Commit({5}, 103);
   CHECK_EQ(g.Get(5), std::string("v"));
}

}  // namespace

int main() {
   TestLockedSwap();
   TestSpeculation();
   if (failures > 0) {
      std::cerr << failures << " failure(s)" << std::endl;
      return 1;
   }
   std::cout << "OK" << std::endl;
   return 0;
}
