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

    bool NeedDisPatch(const ClientRequest &req) override {
        return req.cmd_.txnType_ == 4; // YCSB_SWAP
    }

    void GetInquireKeys(const uint32_t txnType,
                        std::map<int32_t, mdb::Value>* existing,
                        std::map<int32_t, mdb::Value>* input) override {
        if (txnType == 4) { // YCSB_SWAP
            input->clear();
            for (auto& kv : *existing) {
                (*input)[kv.first] = mdb::Value();
            }
        }
    }
};

namespace {

void extractJavaMap(JNIEnv* env, jobject jmap, std::map<std::string, std::string>& cppMap) {
    if (!jmap) return;
    jclass mapClass = env->GetObjectClass(jmap);
    jmethodID entrySetMethod = env->GetMethodID(mapClass, "entrySet", "()Ljava/util/Set;");
    jobject entrySet = env->CallObjectMethod(jmap, entrySetMethod);
    
    jclass setClass = env->GetObjectClass(entrySet);
    jmethodID iteratorMethod = env->GetMethodID(setClass, "iterator", "()Ljava/util/Iterator;");
    jobject iterator = env->CallObjectMethod(entrySet, iteratorMethod);
    
    jclass iteratorClass = env->GetObjectClass(iterator);
    jmethodID hasNextMethod = env->GetMethodID(iteratorClass, "hasNext", "()Z");
    jmethodID nextMethod = env->GetMethodID(iteratorClass, "next", "()Ljava/lang/Object;");
    
    jclass entryClass = nullptr;
    jmethodID getKeyMethod = nullptr;
    jmethodID getValueMethod = nullptr;
    
    while (env->CallBooleanMethod(iterator, hasNextMethod)) {
        jobject entry = env->CallObjectMethod(iterator, nextMethod);
        if (!entryClass) {
            entryClass = env->GetObjectClass(entry);
            getKeyMethod = env->GetMethodID(entryClass, "getKey", "()Ljava/lang/Object;");
            getValueMethod = env->GetMethodID(entryClass, "getValue", "()Ljava/lang/Object;");
        }
        
        jstring jkey = (jstring)env->CallObjectMethod(entry, getKeyMethod);
        jstring jval = (jstring)env->CallObjectMethod(entry, getValueMethod);
        
        const char* keyStr = env->GetStringUTFChars(jkey, nullptr);
        const char* valStr = env->GetStringUTFChars(jval, nullptr);
        
        cppMap[keyStr] = valStr;
        
        env->ReleaseStringUTFChars(jkey, keyStr);
        env->ReleaseStringUTFChars(jval, valStr);
        
        env->DeleteLocalRef(entry);
        env->DeleteLocalRef(jkey);
        env->DeleteLocalRef(jval);
    }
    if (entryClass) env->DeleteLocalRef(entryClass);
    env->DeleteLocalRef(iteratorClass);
    env->DeleteLocalRef(setClass);
    env->DeleteLocalRef(mapClass);
    env->DeleteLocalRef(iterator);
    env->DeleteLocalRef(entrySet);
}

void extractJavaSet(JNIEnv* env, jobject jset, std::vector<std::string>& cppSet) {
    if (!jset) return;
    jclass setClass = env->GetObjectClass(jset);
    jmethodID iteratorMethod = env->GetMethodID(setClass, "iterator", "()Ljava/util/Iterator;");
    jobject iterator = env->CallObjectMethod(jset, iteratorMethod);
    
    jclass iteratorClass = env->GetObjectClass(iterator);
    jmethodID hasNextMethod = env->GetMethodID(iteratorClass, "hasNext", "()Z");
    jmethodID nextMethod = env->GetMethodID(iteratorClass, "next", "()Ljava/lang/Object;");
    
    while (env->CallBooleanMethod(iterator, hasNextMethod)) {
        jstring jitem = (jstring)env->CallObjectMethod(iterator, nextMethod);
        const char* itemStr = env->GetStringUTFChars(jitem, nullptr);
        cppSet.push_back(itemStr);
        env->ReleaseStringUTFChars(jitem, itemStr);
        env->DeleteLocalRef(jitem);
    }
    env->DeleteLocalRef(iteratorClass);
    env->DeleteLocalRef(setClass);
    env->DeleteLocalRef(iterator);
}

int getFieldId(const std::string& fieldName) {
    if (fieldName.rfind("field", 0) == 0) {
        try {
            return std::stoi(fieldName.substr(5));
        } catch (...) {
            return 0;
        }
    }
    return 0;
}

std::string serializeMap(const std::map<std::string, std::string>& m) {
    std::string ret = "";
    for (auto const& [k, v] : m) {
        ret += k + ":" + v + "|";
    }
    return ret;
}

void deserializeMap(const std::string& str, std::map<std::string, std::string>& m) {
    size_t last = 0;
    size_t next = 0;
    while ((next = str.find('|', last)) != std::string::npos) {
        std::string pair = str.substr(last, next - last);
        size_t colon = pair.find(':');
        if (colon != std::string::npos) {
            m[pair.substr(0, colon)] = pair.substr(colon + 1);
        }
        last = next + 1;
    }
}

} // namespace

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
        delete info_;
        delete coord_;
        delete comm_;
    }

    int execute(uint32_t txnType, const std::string& key, JNIEnv* env, jobject jfields, jobject jmap) override {
        int32_t record_id = hashKey(key);
        int32_t int_key = record_id;

        ClientRequest req;
        req.cmd_.clientId_ = info_->coordinatorId_;
        req.cmd_.reqId_ = info_->nextRequestIdByProxy_.fetch_add(1);
        req.cmd_.txnType_ = txnType;

        if (txnType == 1) { // Read
            req.cmd_.ws_[int_key].set_i32(0);
            req.targetShards_.insert(record_id % shardNum_);
        } else if (txnType == 2 || txnType == 3) { // Update / Insert
            std::map<std::string, std::string> cppMap;
            extractJavaMap(env, jmap, cppMap);
            std::string payload = serializeMap(cppMap);
            req.cmd_.ws_[int_key].set_str(payload);
            req.targetShards_.insert(record_id % shardNum_);
        }

        static std::atomic<uint64_t> execCnt{0};
        uint64_t curCnt = execCnt.fetch_add(1);
        if (curCnt < 20) {
            LOG(INFO) << "[YCSB-CLIENT] execute #" << curCnt 
                      << " txnType=" << txnType 
                      << " key=" << key 
                      << " record_id=" << record_id 
                      << " targetShard=" << (record_id % shardNum_)
                      << " reqId=" << req.cmd_.reqId_;
        }

        auto promise = std::make_shared<std::promise<ClientReply>>();
        auto future = promise->get_future();

        req.callback_ = [promise, curCnt](const ClientReply& rep) {
            if (curCnt < 20) {
                LOG(INFO) << "[YCSB-CLIENT] callback invoked #" << curCnt;
            }
            promise->set_value(rep);
        };

        coord_->DoOne(req, txnGen_);

        ClientReply reply = future.get();
        if (curCnt < 20) {
            LOG(INFO) << "[YCSB-CLIENT] future resolved #" << curCnt;
        }

        if (txnType == 1 && jmap) { // Read
            std::string rowStr = "";
            auto it = reply.result_.find(int_key);
            if (it != reply.result_.end()) {
                rowStr = it->second.get_str();
            }
            std::map<std::string, std::string> rowMap;
            deserializeMap(rowStr, rowMap);

            std::vector<std::string> fields;
            extractJavaSet(env, jfields, fields);
            for (const auto& field : fields) {
                std::string val = "";
                auto valIt = rowMap.find(field);
                if (valIt != rowMap.end()) {
                    val = valIt->second;
                }
                populateJavaMap(env, jmap, field, val);
            }
        }

        return 0;
    }

    int transfer(const std::string& key1, const std::string& key2, const std::string& field, JNIEnv* env) override {
        int32_t rec1 = hashKey(key1);
        int32_t rec2 = hashKey(key2);

        ClientRequest req;
        req.cmd_.clientId_ = info_->coordinatorId_;
        req.cmd_.reqId_ = info_->nextRequestIdByProxy_.fetch_add(1);
        req.cmd_.txnType_ = 2; // YCSB_UPDATE

        req.cmd_.ws_[rec1].set_str("transfer");
        req.cmd_.ws_[rec2].set_str("transfer");

        req.targetShards_.insert(rec1 % shardNum_);
        req.targetShards_.insert(rec2 % shardNum_);

        auto promise = std::make_shared<std::promise<ClientReply>>();
        auto future = promise->get_future();

        req.callback_ = [promise](const ClientReply& rep) {
            promise->set_value(rep);
        };

        coord_->DoOne(req, txnGen_);
        ClientReply reply = future.get();
        return 0;
    }

    int swap(const std::vector<std::string>& keys, const std::string& field, JNIEnv* env) override {
        ClientRequest req;
        req.cmd_.clientId_ = info_->coordinatorId_;
        req.cmd_.reqId_ = info_->nextRequestIdByProxy_.fetch_add(1);
        req.cmd_.txnType_ = 4; // YCSB_SWAP

        for (const auto& key : keys) {
            int32_t int_key = hashKey(key);
            req.cmd_.ws_[int_key].set_str("");
            req.targetShards_.insert(int_key % shardNum_);
        }

        static std::atomic<uint64_t> swapCnt{0};
        uint64_t curCnt = swapCnt.fetch_add(1);
        if (curCnt < 20) {
            LOG(INFO) << "[YCSB-CLIENT] swap #" << curCnt
                      << " keyCount=" << keys.size()
                      << " targetShardsCount=" << req.targetShards_.size()
                      << " reqId=" << req.cmd_.reqId_;
        }

        auto promise = std::make_shared<std::promise<ClientReply>>();
        auto future = promise->get_future();

        req.callback_ = [promise, curCnt](const ClientReply& rep) {
            if (curCnt < 20) {
                LOG(INFO) << "[YCSB-CLIENT] swap callback invoked #" << curCnt;
            }
            promise->set_value(rep);
        };

        coord_->DoOne(req, txnGen_);

        ClientReply reply = future.get();
        if (curCnt < 20) {
            LOG(INFO) << "[YCSB-CLIENT] swap future resolved #" << curCnt;
        }

        return 0;
    }
};

std::atomic<uint32_t> TigaYcsbClient::nextCoordinatorId_{0};

BaseYcsbClient* createTigaClient(const std::string& configPath) {
    return new TigaYcsbClient(configPath);
}
