#include "TigaCommunicator.h"

TigaCommunicator::TigaCommunicator(const uint32_t id, const YAML::Node& config)
    : id_(id), config_(config), laneNum_(0) {
   shardNum_ = config["site"]["server"].size();
   replicaNum_ = config["site"]["server"][0].size();
   for (uint32_t l = 0; l < MAX_LANE_NUM; l++) {
      lanes_[l] = nullptr;
   }
   for (uint32_t sid = 0; sid < shardNum_; sid++) {
      for (uint32_t rid = 0; rid < replicaNum_; rid++) {
         std::string fullName =
             config_["site"]["server"][sid][rid].as<std::string>();
         std::string thisServerName = fullName.substr(0, fullName.find(':'));
         std::string portName = fullName.substr(fullName.find(':') + 1);
         std::string ip = config_["host"][thisServerName].as<std::string>();
         serverAddrs_[sid][rid] = ip + ":" + portName;
      }
   }
}

void TigaCommunicator::Connect() {
   if (laneNum_.load() == 0) {
      AddLane();
   }
}

uint32_t TigaCommunicator::AddLane() {
   std::lock_guard<std::mutex> lock(laneMtx_);
   uint32_t l = laneNum_.load();
   CHECK(l < MAX_LANE_NUM) << "too many lanes";
   Lane* lane = new Lane();
   lane->rpcPoll_ = new PollMgr(1);
   for (uint32_t sid = 0; sid < shardNum_; sid++) {
      for (uint32_t rid = 0; rid < replicaNum_; rid++) {
         rrr::Client* cli = new rrr::Client(lane->rpcPoll_);
         int ret = -1;
         LOG(INFO) << "Connect lane=" << l << " to sid=" << sid
                   << "\trid=" << rid << ":" << serverAddrs_[sid][rid];
         do {
            ret = cli->connect(serverAddrs_[sid][rid].c_str());
            if (ret != 0) {
               usleep(100000);
            }
         } while (ret != 0);
         lane->proxies_[sid][rid] = new TigaProxy(cli);
      }
   }
   lanes_[l].store(lane);
   laneNum_.store(l + 1);
   LOG(INFO) << "All Connected (lane " << l << ")";
   return l;
}

uint32_t TigaCommunicator::LaneNum() { return laneNum_.load(); }

TigaProxy* TigaCommunicator::ProxyAt(const uint32_t shardId,
                                     const uint32_t replicaId,
                                     const uint32_t lane) {
   return lanes_[lane].load(std::memory_order_acquire)
       ->proxies_[shardId][replicaId];
}

YAML::Node TigaCommunicator::Config() { return config_; }
TigaCommunicator::~TigaCommunicator() {}
