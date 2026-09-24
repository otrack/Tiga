#include "ycsb_client.h"
#include "TigaEntity/TigaCoordinator.h"
#include "TigaService/TigaCommunicator.h"
#include "TxnGenerator/TxnGenerator.h"
#include <future>
#include <mutex>
#include <memory>
#include <chrono>
#include <atomic>
#include <thread>
#include <random>
#include <algorithm>
#include <unistd.h>
#include <cstdio>
#include <functional>
#include <ctime>
#include <cmath>

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
                (*input)[kv.first] = kv.second;
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

        txnGen_ = new TigaYcsbTxnGenerator(shardNum_, replicaNum_, config_);
    }

    ~TigaYcsbClient() override {
        stopPump_.store(true);
        if (pumpThread_.joinable()) {
            pumpThread_.join();
        }
        // Stop daemon/inquiry threads first: they reference comm_ (ProxyAt).
        info_->Shutdown();
        // Destroy the communicator (joins its poller thread), so no late RPC
        // replies can invoke callbacks that touch info_/coordinators afterwards.
        delete comm_;
        // Only now is it safe to free the objects those callbacks referenced.
        delete info_;
        for (TigaCoordinator* c : poolCreated_) {
            delete c;
        }
        delete txnGen_;
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

        ClientReply reply;
        if (!Submit(req, &reply)) {
            LOG(WARNING) << "[YCSB-CLIENT] execute timeout #" << curCnt;
            return -1;
        }
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

        ClientReply reply;
        if (!Submit(req, &reply)) {
            LOG(WARNING) << "[YCSB-CLIENT] transfer timeout";
            return -1;
        }
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

        ClientReply reply;
        if (!Submit(req, &reply)) {
            LOG(WARNING) << "[YCSB-CLIENT] swap timeout #" << curCnt;
            return -1;
        }
        if (curCnt < 20) {
            LOG(INFO) << "[YCSB-CLIENT] swap future resolved #" << curCnt;
        }

        return 0;
    }

    int runSwapOpenLoop(uint32_t rate, uint32_t maxOutstanding, uint32_t runSec,
                        uint32_t recordCount, uint32_t swapSize) override;

    int setOpenLoopArrival(int mode) override;

  private:
    struct OpenLoopSample {
        uint64_t sendUs_;
        uint64_t commitUs_;
        int32_t boundUs_;
        uint32_t repSlow_;
        uint32_t nonSerial_;
    };

    std::mutex poolMtx_;
    std::vector<TigaCoordinator*> poolCreated_;
    std::vector<TigaCoordinator*> poolFree_;
    std::atomic<int32_t> outstanding_{0};
    std::vector<OpenLoopSample> samples_;
    std::mutex samplesMtx_;
    std::thread pumpThread_;
    std::atomic<bool> stopPump_{false};
    std::atomic<uint64_t> pumpElapsedUs_{0};
    std::mt19937_64 rng_{static_cast<uint64_t>(
        std::chrono::high_resolution_clock::now().time_since_epoch().count())};
    uint32_t openLoopRecordCount_ = 2000005;
    uint32_t openLoopSwapSize_ = 3;

    static std::string BuildKeyName(uint64_t n) {
        return "user" + std::to_string(n);
    }

    TigaCoordinator* PopCoordinator();
    void PushCoordinator(TigaCoordinator* coo);
    bool Submit(ClientRequest& req, ClientReply* reply);
    void OpenLoopDispatch(TigaCoordinator* coo);
    void OpenLoopRequestDone(TigaCoordinator* coo);
    void RunOpenLoop(uint32_t rate, uint32_t maxOutstanding, uint32_t runSec);
    void RunOpenLoopDeterministic(uint32_t rate, uint32_t maxOutstanding,
                                  uint32_t runSec);
    void RunOpenLoopPoisson(uint32_t rate, uint32_t maxOutstanding,
                            uint32_t runSec);
    void FinishOpenLoop();
    void PrintOpenLoopSummary();

    int openLoopArrivalMode_ = 0;
};

std::atomic<uint32_t> TigaYcsbClient::nextCoordinatorId_{0};

namespace {

// YCSB creates one DB (hence one native client) per worker thread.  A
// TigaYcsbClient owns a communicator, a GlobalInfo and their threads, so all
// the handles of a JVM share a single one; each in-flight request gets its own
// pooled TigaCoordinator instead.
std::mutex sharedMtx;
TigaYcsbClient* sharedClient = nullptr;
std::string sharedConfigPath;
uint32_t sharedRefs = 0;

class TigaYcsbClientRef : public BaseYcsbClient {
  public:
    explicit TigaYcsbClientRef(TigaYcsbClient* client) : client_(client) {}

    ~TigaYcsbClientRef() override {
        std::lock_guard<std::mutex> lock(sharedMtx);
        if (--sharedRefs == 0) {
            delete sharedClient;
            sharedClient = nullptr;
        }
    }

    int execute(uint32_t txnType, const std::string& key, JNIEnv* env, jobject jfields, jobject jmap) override {
        return client_->execute(txnType, key, env, jfields, jmap);
    }

    int transfer(const std::string& key1, const std::string& key2, const std::string& field, JNIEnv* env) override {
        return client_->transfer(key1, key2, field, env);
    }

    int swap(const std::vector<std::string>& keys, const std::string& field, JNIEnv* env) override {
        return client_->swap(keys, field, env);
    }

    int runSwapOpenLoop(uint32_t rate, uint32_t maxOutstanding, uint32_t runSec,
                        uint32_t recordCount, uint32_t swapSize) override {
        return client_->runSwapOpenLoop(rate, maxOutstanding, runSec, recordCount, swapSize);
    }

    int setOpenLoopArrival(int mode) override {
        return client_->setOpenLoopArrival(mode);
    }

  private:
    TigaYcsbClient* client_;
};

} // namespace

BaseYcsbClient* createTigaClient(const std::string& configPath) {
    std::lock_guard<std::mutex> lock(sharedMtx);
    if (sharedClient == nullptr) {
        sharedClient = new TigaYcsbClient(configPath);
        sharedConfigPath = configPath;
    } else if (configPath != sharedConfigPath) {
        LOG(WARNING) << "[YCSB-CLIENT] ignoring config " << configPath
                     << ", sharing the client built from " << sharedConfigPath;
    }
    sharedRefs++;
    return new TigaYcsbClientRef(sharedClient);
}

void TigaYcsbClient::PushCoordinator(TigaCoordinator* coo) {
    std::lock_guard<std::mutex> lock(poolMtx_);
    poolFree_.push_back(coo);
}

// Runs req on a pooled coordinator and waits for its reply.  On timeout the
// coordinator is not returned to the pool: a late commit would otherwise fire
// the callback of whichever request reuses it.  It is freed at teardown.
bool TigaYcsbClient::Submit(ClientRequest& req, ClientReply* reply) {
    auto promise = std::make_shared<std::promise<ClientReply>>();
    auto future = promise->get_future();
    req.callback_ = [promise](const ClientReply& rep) {
        try { promise->set_value(rep); } catch (const std::future_error&) {}
    };

    TigaCoordinator* coo = PopCoordinator();
    coo->DoOne(req, txnGen_);

    if (future.wait_for(std::chrono::seconds(30)) == std::future_status::timeout) {
        return false;
    }
    *reply = future.get();
    PushCoordinator(coo);
    return true;
}

TigaCoordinator* TigaYcsbClient::PopCoordinator() {
    std::lock_guard<std::mutex> lock(poolMtx_);
    if (!poolFree_.empty()) {
        TigaCoordinator* c = poolFree_.back();
        poolFree_.pop_back();
        return c;
    }
    TigaCoordinator* c = new TigaCoordinator(info_->coordinatorId_, config_);
    c->SetGlobalInfo(info_);
    poolCreated_.push_back(c);
    return c;
}

void TigaYcsbClient::OpenLoopDispatch(TigaCoordinator* coo) {
    ClientRequest req;
    req.cmd_.clientId_ = info_->coordinatorId_;
    req.cmd_.reqId_ = info_->nextRequestIdByProxy_.fetch_add(1);
    req.cmd_.txnType_ = 4;  // YCSB_SWAP

    std::vector<uint64_t> keyNums;
    keyNums.reserve(openLoopSwapSize_);
    while (keyNums.size() < openLoopSwapSize_) {
        uint64_t n = rng_() % openLoopRecordCount_;
        bool duplicate = false;
        for (auto& k : keyNums) {
            if (k == n) { duplicate = true; break; }
        }
        if (!duplicate) keyNums.push_back(n);
    }
    for (uint64_t n : keyNums) {
        int32_t int_key = hashKey(BuildKeyName(n));
        req.cmd_.ws_[int_key].set_str("");
        req.targetShards_.insert(int_key % shardNum_);
    }

    outstanding_.fetch_add(1);
    req.callback_ = [this, coo](const ClientReply&) {
        this->OpenLoopRequestDone(coo);
    };
    coo->DoOne(req, txnGen_);
}

void TigaYcsbClient::OpenLoopRequestDone(TigaCoordinator* coo) {
    uint64_t commitUs = GetMicrosecondTimestamp();
    outstanding_.fetch_sub(1);
    {
        std::lock_guard<std::mutex> lock(samplesMtx_);
        samples_.push_back({coo->sendTime_, commitUs,
                            coo->reqInProcess_.bound_,
                            (uint32_t)coo->detectReplicationInconsistency_,
                            (uint32_t)coo->detectNonSerial_});
    }
    PushCoordinator(coo);
}

void TigaYcsbClient::RunOpenLoop(uint32_t rate, uint32_t maxOutstanding,
                                 uint32_t runSec) {
    if (openLoopArrivalMode_ != 0) {
        RunOpenLoopPoisson(rate, maxOutstanding, runSec);
    } else {
        RunOpenLoopDeterministic(rate, maxOutstanding, runSec);
    }
}

// Arrival mode 0: greedy dispatch while the running average (txnCnt/elapsed)
// stays below rate, then idle.  Spacing is deterministic-ish at the mean rate
// with coefficient of variation ~0.
void TigaYcsbClient::RunOpenLoopDeterministic(uint32_t rate,
                                              uint32_t maxOutstanding,
                                              uint32_t runSec) {
    uint64_t startTime = GetMicrosecondTimestamp();
    uint64_t endTime = startTime + runSec * 1000ul * 1000ul;
    uint64_t txnCnt = 0;
    double tps = 0;
    while (true) {
        if (stopPump_.load()) break;
        if (info_->serverSignal_ != CSTATUS_RUN) {
            while (info_->serverSignal_ != CSTATUS_RUN && !stopPump_.load()) {
                usleep(10000);
            }
            startTime = GetMicrosecondTimestamp();
            endTime = startTime + runSec * 1000ul * 1000ul;
            txnCnt = 0;
            tps = 0;
        }
        uint64_t elapsed = 0;
        while (tps < rate && !stopPump_.load()) {
            if ((uint32_t)outstanding_.load() < maxOutstanding) {
                TigaCoordinator* coo = PopCoordinator();
                if (coo != NULL) {
                    OpenLoopDispatch(coo);
                    txnCnt++;
                } else {
                    usleep(50);
                }
            } else {
                usleep(50);
            }
            elapsed = GetMicrosecondTimestamp() - startTime;
            tps = elapsed > 0 ? (double)(txnCnt * 1000000.0) / elapsed : 0;
        }
        uint64_t nowTime = GetMicrosecondTimestamp();
        elapsed = nowTime - startTime;
        tps = elapsed > 0 ? (double)(txnCnt * 1000000.0) / elapsed : 0;
        if (nowTime >= endTime) break;
    }
    pumpElapsedUs_.store(GetMicrosecondTimestamp() - startTime);
    FinishOpenLoop();
}

// Arrival mode 1: poisson process with exponential inter-arrival times of mean
// 1/rate.  maxOutstanding still caps concurrency, but now acts as a
// back-pressure valve: a due arrival whose slot is taken simply waits (the
// schedule is never skipped), which is where open-loop burstiness shows up as
// latency inflation and output clipping.
void TigaYcsbClient::RunOpenLoopPoisson(uint32_t rate, uint32_t maxOutstanding,
                                        uint32_t runSec) {
    auto interArrivalUs = [this, rate]() -> uint64_t {
        double lambdaPerUs = (double)rate / 1000000.0;
        std::uniform_real_distribution<double> dist(0.0, 1.0);
        double u = std::nextafter(dist(rng_), 1.0);
        double dtUs = -std::log(u) / lambdaPerUs;
        if (dtUs < 1.0) dtUs = 1.0;
        return (uint64_t)(dtUs + 0.5);
    };

    uint64_t startTime = GetMicrosecondTimestamp();
    uint64_t endTime = startTime + runSec * 1000ul * 1000ul;
    uint64_t nextArrivalUs = startTime + interArrivalUs();
    while (true) {
        if (stopPump_.load()) break;
        if (info_->serverSignal_ != CSTATUS_RUN) {
            while (info_->serverSignal_ != CSTATUS_RUN && !stopPump_.load()) {
                usleep(10000);
            }
            startTime = GetMicrosecondTimestamp();
            endTime = startTime + runSec * 1000ul * 1000ul;
            nextArrivalUs = startTime + interArrivalUs();
        }
        uint64_t nowUs = GetMicrosecondTimestamp();
        while (nowUs >= nextArrivalUs && nowUs < endTime && !stopPump_.load()) {
            if ((uint32_t)outstanding_.load() < maxOutstanding) {
                TigaCoordinator* coo = PopCoordinator();
                if (coo != NULL) {
                    OpenLoopDispatch(coo);
                    nextArrivalUs += interArrivalUs();
                } else {
                    usleep(50);
                }
            } else {
                usleep(50);
            }
            nowUs = GetMicrosecondTimestamp();
        }
        if (nowUs >= endTime) break;
        uint64_t waitUs = nextArrivalUs - nowUs;
        if (waitUs > 50) {
            usleep((useconds_t)std::min<uint64_t>(waitUs, 5000));
        }
    }
    pumpElapsedUs_.store(GetMicrosecondTimestamp() - startTime);
    FinishOpenLoop();
}

void TigaYcsbClient::FinishOpenLoop() {
    uint64_t drainStart = GetMicrosecondTimestamp();
    while (outstanding_.load() > 0) {
        if (stopPump_.load() &&
            GetMicrosecondTimestamp() - drainStart > 10 * 1000ul * 1000ul) {
            break;
        }
        usleep(1000);
    }
    PrintOpenLoopSummary();
}

void TigaYcsbClient::PrintOpenLoopSummary() {
    std::vector<OpenLoopSample> samples;
    {
        std::lock_guard<std::mutex> lock(samplesMtx_);
        samples.swap(samples_);
    }
    auto latOf = [](const OpenLoopSample& s) {
        return s.commitUs_ - s.sendUs_;
    };
    std::sort(samples.begin(), samples.end(),
              [&](const OpenLoopSample& a, const OpenLoopSample& b) {
                  return latOf(a) < latOf(b);
              });

    size_t n = samples.size();
    uint64_t sumUs = 0;
    uint32_t repSlowCnt = 0;
    uint32_t nonSerialCnt = 0;
    for (auto& s : samples) {
        sumUs += latOf(s);
        repSlowCnt += s.repSlow_;
        nonSerialCnt += s.nonSerial_;
    }
    uint64_t elapsedUs = pumpElapsedUs_.load();
    if (elapsedUs == 0) elapsedUs = 1;

    auto pct = [&](size_t rank) -> uint64_t {
        return n ? latOf(samples[std::min(rank, n - 1)]) : 0;
    };
    fprintf(stdout,
            "[OL-SUMMARY] completed=%zu tput_hz=%.2f p50=%llu p90=%llu "
            "p99=%llu p99_9=%llu max=%llu avg=%.1f repSlow=%u nonSerial=%u\n",
            n, (double)n * 1000000.0 / elapsedUs,
            (unsigned long long)pct(n * 50 / 100),
            (unsigned long long)pct(n * 90 / 100),
            (unsigned long long)pct(n * 99 / 100),
            (unsigned long long)pct(n * 999 / 1000),
            (unsigned long long)(n ? latOf(samples[n - 1]) : 0),
            n ? (double)sumUs / n : 0.0, repSlowCnt, nonSerialCnt);
    for (size_t i = 0; i < n; i++) {
        fprintf(stdout, "[OL] %llu %d %u %u\n",
                (unsigned long long)latOf(samples[i]), (int)samples[i].boundUs_,
                (unsigned)samples[i].repSlow_, (unsigned)samples[i].nonSerial_);
    }
    fflush(stdout);
}

int TigaYcsbClient::setOpenLoopArrival(int mode) {
    openLoopArrivalMode_ = mode;
    return 0;
}

int TigaYcsbClient::runSwapOpenLoop(uint32_t rate, uint32_t maxOutstanding,
                                    uint32_t runSec, uint32_t recordCount,
                                    uint32_t swapSize) {
    if (maxOutstanding == 0) maxOutstanding = std::max((uint32_t)1, rate * 2);
    if (swapSize < 2) swapSize = 3;
    if (recordCount < 2) recordCount = 2000005;
    openLoopRecordCount_ = recordCount;
    openLoopSwapSize_ = swapSize;
    if (pumpThread_.joinable()) {
        LOG(WARNING) << "[OPENLOOP] pump already running";
        return 0;
    }
    LOG(INFO) << "[OPENLOOP] rate=" << rate
              << " maxOutstanding=" << maxOutstanding
              << " runSec=" << runSec << " recordCount=" << recordCount
              << " swapSize=" << swapSize
              << " arrivalMode=" << openLoopArrivalMode_;
    pumpThread_ = std::thread([this, rate, maxOutstanding, runSec]() {
        this->RunOpenLoop(rate, maxOutstanding, runSec);
    });
    return 0;
}
