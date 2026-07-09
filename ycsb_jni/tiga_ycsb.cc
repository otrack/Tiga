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
        int32_t int_key = hashKey(key);

        ClientRequest req;
        req.cmd_.clientId_ = info_->coordinatorId_;
        req.cmd_.reqId_ = info_->nextRequestIdByProxy_.fetch_add(1);
        req.cmd_.txnType_ = txnType;

        if (txnType == 1) { // Read
            std::vector<std::string> fields;
            extractJavaSet(env, jfields, fields);
            for (const auto& field : fields) {
                int fieldId = getFieldId(field);
                int32_t cell_key = int_key * 100 + fieldId;
                req.cmd_.ws_[cell_key].set_i32(0);
                req.targetShards_.insert(int_key % shardNum_);
            }
        } else if (txnType == 2 || txnType == 3) { // Update / Insert
            std::map<std::string, std::string> cppMap;
            extractJavaMap(env, jmap, cppMap);
            for (auto const& [field, val] : cppMap) {
                int fieldId = getFieldId(field);
                int32_t cell_key = int_key * 100 + fieldId;
                req.cmd_.ws_[cell_key].set_str(val);
                req.targetShards_.insert(int_key % shardNum_);
            }
        }

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
            std::vector<std::string> fields;
            extractJavaSet(env, jfields, fields);
            for (const auto& field : fields) {
                int fieldId = getFieldId(field);
                int32_t cell_key = int_key * 100 + fieldId;
                std::string val = "";
                auto it = reply.result_.find(cell_key);
                if (it != reply.result_.end()) {
                    val = it->second.get_str();
                }
                populateJavaMap(env, jmap, field, val);
            }
        }

        return 0;
    }
};

std::atomic<uint32_t> TigaYcsbClient::nextCoordinatorId_{0};

BaseYcsbClient* createTigaClient(const std::string& configPath) {
    return new TigaYcsbClient(configPath);
}
