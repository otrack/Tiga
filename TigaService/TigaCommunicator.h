#pragma once
#include <atomic>
#include <mutex>
#include <glog/logging.h>
#include <yaml-cpp/yaml.h>
#include "TigaMessage.h"
#include "TigaServiceImpl.h"

#define MAX_LANE_NUM (1024)

// A communicator holds one or more lanes.  A lane is a poller thread and one
// connection to every replica: requests sent on a lane are written, and their
// replies read and handled, by that lane's poller only.
class TigaCommunicator {
  private:
   struct Lane {
      rrr::PollMgr* rpcPoll_;
      TigaProxy* proxies_[MAX_SHARD_NUM][MAX_REPLICA_NUM];
   };

   uint32_t id_;
   YAML::Node config_;
   uint32_t shardNum_;
   uint32_t replicaNum_;
   std::string serverAddrs_[MAX_SHARD_NUM][MAX_REPLICA_NUM];
   std::mutex laneMtx_;
   std::atomic<Lane*> lanes_[MAX_LANE_NUM];
   std::atomic<uint32_t> laneNum_;

  public:
   TigaCommunicator(const uint32_t id, const YAML::Node& config);
   YAML::Node Config();
   // Connects lane 0
   void Connect();
   // Connects a new lane and returns its index
   uint32_t AddLane();
   uint32_t LaneNum();
   TigaProxy* ProxyAt(const uint32_t shardId, const uint32_t replicaId,
                      const uint32_t lane = 0);
   ~TigaCommunicator();
};
