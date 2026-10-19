#include "TigaCommunicator.h"
#include <thread>
#include <vector>

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
   uint32_t l;
   {
      std::lock_guard<std::mutex> lock(laneMtx_);
      l = laneReserved_++;
   }
   CHECK(l < MAX_LANE_NUM) << "too many lanes";
   Lane* lane = new Lane();
   lane->rpcPoll_ = new PollMgr(1);
   // Each connection takes a round trip, which adds up over the replicas
   // (and the lanes) when they are connected one after the other
   std::vector<std::thread> connecting;
   for (uint32_t sid = 0; sid < shardNum_; sid++) {
      for (uint32_t rid = 0; rid < replicaNum_; rid++) {
         connecting.emplace_back([this, lane, l, sid, rid]() {
            rrr::Client* cli = new rrr::Client(lane->rpcPoll_);
            int ret = -1;
            VLOG(1) << "Connect lane=" << l << " to sid=" << sid
                    << "\trid=" << rid << ":" << serverAddrs_[sid][rid];
            do {
               ret = cli->connect(serverAddrs_[sid][rid].c_str());
               if (ret != 0) {
                  usleep(100000);
               }
            } while (ret != 0);
            lane->proxies_[sid][rid] = new TigaProxy(cli);
         });
      }
   }
   for (std::thread& t : connecting) {
      t.join();
   }
   lanes_[l].store(lane, std::memory_order_release);
   laneNum_.fetch_add(1);
   // One line per process; the other lanes (one per client thread) only with --v=1
   if (l == 0) {
      LOG(INFO) << "All Connected (lane " << l << ")";
   } else {
      VLOG(1) << "All Connected (lane " << l << ")";
   }
   return l;
}

uint32_t TigaCommunicator::LaneNum() { return laneNum_.load(); }

void TigaCommunicator::WaitLane(const uint32_t l) {
   while (lanes_[l].load(std::memory_order_acquire) == nullptr) {
      usleep(1000);
   }
}

TigaProxy* TigaCommunicator::ProxyAt(const uint32_t shardId,
                                     const uint32_t replicaId,
                                     const uint32_t lane) {
   return lanes_[lane].load(std::memory_order_acquire)
       ->proxies_[shardId][replicaId];
}

YAML::Node TigaCommunicator::Config() { return config_; }
TigaCommunicator::~TigaCommunicator() {}
