#include "ycsb_client.h"
#include "TigaEntity/TigaCoordinator.h"
#include "TigaService/TigaCommunicator.h"
#include "TxnGenerator/TxnGenerator.h"
#include <future>
#include <mutex>
#include <memory>
#include <chrono>
#include <atomic>
#include <unistd.h>
#include <functional>
#include <ctime>

class TigaYcsbTxnGenerator : public TxnGenerator {
  public:
    TigaYcsbTxnGenerator(uint32_t shardNum, uint32_t replicaNum, const YAML::Node& config)
        : TxnGenerator(shardNum, replicaNum, config) {}
    
    virtual ~TigaYcsbTxnGenerator() {}
    
    std::string RTTI() override { return "TigaYcsbTxnGenerator"; }
    void GetTxnReq(ClientRequest *req, uint32_t reqId, uint32_t cid) override {}
};

class TigaYcsbClient : public BaseYcsbClient {
public:
    YAML::Node config_;
    TigaCommunicator* comm_;
    GlobalInfo* info_;
    TigaCoordinator* coord_;
    TigaYcsbTxnGenerator* txnGen_;
    uint32_t shardNum_;
    uint32_t replicaNum_;

    static std::atomic<uint32_t> nextCoordinatorId_;

    TigaYcsbClient(const std::string& configPath) {
        config_ = YAML::LoadFile(configPath);
        shardNum_ = config_["site"]["server"].size();
        replicaNum_ = config_["site"]["server"][0].size();

        char hostname[256];
        if (gethostname(hostname, sizeof(hostname)) != 0) {
            strcpy(hostname, "unknown");
        }
        uint32_t hostHash = std::hash<std::string>{}(hostname) & 0xFF;
        uint32_t timeSec = static_cast<uint32_t>(time(NULL)) & 0xFFFF;
        uint32_t coordId = (hostHash << 24) | (timeSec << 8) | (nextCoordinatorId_.fetch_add(1) & 0xFF);

        comm_ = new TigaCommunicator(coordId, config_);
        comm_->Connect();
        info_ = new GlobalInfo(coordId, shardNum_, replicaNum_, 400000, 60000, 10000, comm_);
        
        coord_ = new TigaCoordinator(coordId, config_);
        coord_->SetGlobalInfo(info_);
        
        txnGen_ = new TigaYcsbTxnGenerator(shardNum_, replicaNum_, config_);
    }

    ~TigaYcsbClient() override {
        delete txnGen_;
        delete coord_;
        delete info_;
        delete comm_;
    }

    int execute(uint32_t txnType, const std::string& key, JNIEnv* env, jobject jmap) override {
        int32_t int_key = hashKey(key);

        ClientRequest req;
        req.cmd_.clientId_ = info_->coordinatorId_;
        req.cmd_.reqId_ = info_->nextRequestIdByProxy_.fetch_add(1);
        req.cmd_.txnType_ = txnType;
        req.cmd_.ws_[int_key].set_i32(0);
        req.targetShards_.insert(int_key % shardNum_);

        auto promise = std::make_shared<std::promise<ClientReply>>();
        auto future = promise->get_future();

        req.callback_ = [promise](const ClientReply& rep) {
            promise->set_value(rep);
        };

        coord_->DoOne(req, txnGen_);

        auto status = future.wait_for(std::chrono::seconds(5));
        if (status == std::future_status::timeout) {
            return -1; // Transaction timed out, YCSB will record as error
        }
        ClientReply reply = future.get();

        if (txnType == 1 && jmap) { // Read
            int32_t val = 0;
            auto it = reply.result_.find(int_key);
            if (it != reply.result_.end()) {
                val = it->second.get_i32();
            }
            for (int i = 0; i < 10; ++i) {
                populateJavaMap(env, jmap, "field" + std::to_string(i), "value" + std::to_string(val));
            }
        }

        return 0;
    }
};

std::atomic<uint32_t> TigaYcsbClient::nextCoordinatorId_{0};

BaseYcsbClient* createTigaClient(const std::string& configPath) {
    return new TigaYcsbClient(configPath);
}
