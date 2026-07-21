#include "ycsb_client.h"
#include "ncc/janus/src/deptran/coordinator.h"
#include "ncc/janus/src/deptran/frame.h"
#include "ncc/janus/src/deptran/workload.h"
#include "ncc/janus/src/deptran/procedure.h"
#include "ncc/janus/src/deptran/config.h"
#include "ncc/janus/src/deptran/sharding.h"
#include "ncc/janus/src/deptran/client_worker.h"
#include <future>
#include <mutex>
#include <memory>
#include <chrono>
#include <atomic>
#include <unistd.h>
#include <functional>
#include <ctime>
#include <map>
#include <vector>
#include <string>
#include <cstring>

std::vector<std::unique_ptr<janus::ClientWorker>> client_workers_g;

namespace janus {

class YcsbChopper : public TxData {
public:
    YcsbChopper() {}
    virtual ~YcsbChopper() {}

    virtual void Init(TxRequest &req) override {
        ws_init_ = req.input_;
        ws_ = req.input_;
        type_ = req.tx_type_;
        callback_ = req.callback_;
        max_try_ = req.n_try_;
        n_try_ = 1;
        commit_.store(true);

        innid_t piece_type = type_ * 10;
        GetWorkspace(piece_type).keys_ = {0, 1}; 
        n_pieces_dispatchable_ = 1;

        output_size_ = {{piece_type, (type_ == 1 ? 11 : 0)}};
        p_types_ = {{piece_type, piece_type}};
        
        std::string table_name = "usertable";
        sss_->GetPartition(table_name, req.input_[0], sharding_[piece_type]);
        partition_ids_.insert(sharding_[piece_type]);
        std::cerr << "[DEBUG JNI] YcsbChopper Init piece_type=" << piece_type << " target_partition=" << sharding_[piece_type] << std::endl;
        
        status_ = {{piece_type, DISPATCHABLE}};
        n_pieces_all_ = 1;
    }

    virtual bool HandleOutput(int pi, int res, std::map<int32_t, Value> &output) override {
        return false;
    }

    virtual bool IsReadOnly() override {
        return type_ == 1; 
    }

    virtual void Reset() override {
        TxData::Reset();
        ws_ = ws_init_;
        innid_t piece_type = type_ * 10;
        GetWorkspace(piece_type);
        for (auto& pair : status_) {
            pair.second = DISPATCHABLE;
        }
        commit_.store(true);
        partition_ids_.clear();
        partition_ids_.insert(sharding_[piece_type]);
        n_pieces_dispatchable_ = 1;
        n_try_++;
    }
};

class YcsbWorkload : public Workload {
public:
    YcsbWorkload(Config* config) : Workload(config) {}
    virtual ~YcsbWorkload() {}

    virtual void GetTxRequest(TxRequest* req, uint32_t cid) override {
        verify(0); // Not called in JNI mode
    }

    virtual void RegisterPrecedures() override {
        RegP(1, 10,
             {}, {}, {}, {"usertable", {0}}, DF_NO,
             PROC {
                 verify(cmd.input.size() >= 1);
                 std::string table_name = "usertable";
                 auto tbl = tx.GetTable(table_name);
                 mdb::MultiBlob buf(1);
                 buf[0] = cmd.input[0].get_blob();
                 mdb::Row *r = tx.Query(tbl, buf);
                 if (r) {
                     for (int col = 1; col <= 10; ++col) {
                         Value val;
                         tx.ReadColumn(r, col, &val, TXN_BYPASS);
                         output[col] = val;
                     }
                     *res = SUCCESS;
                 } else {
                     *res = REJECT;
                 }
             }
        );

        RegP(2, 20,
             {}, {}, {}, {"usertable", {0}}, DF_REAL,
             PROC {
                 verify(cmd.input.size() >= 2);
                 std::string table_name = "usertable";
                 auto tbl = tx.GetTable(table_name);
                 mdb::MultiBlob buf(1);
                 buf[0] = cmd.input[0].get_blob();
                 mdb::Row *r = tx.Query(tbl, buf);
                 if (r) {
                     std::string payload = cmd.input[1].get_str();
                     size_t last = 0;
                     size_t next = 0;
                     while ((next = payload.find('|', last)) != std::string::npos) {
                         std::string pair = payload.substr(last, next - last);
                         size_t colon = pair.find(':');
                         if (colon != std::string::npos) {
                             std::string col_name = pair.substr(0, colon);
                             std::string col_val = pair.substr(colon + 1);
                             if (col_name.rfind("field", 0) == 0) {
                                 int col_num = std::stoi(col_name.substr(5));
                                 int col_id = col_num + 1; 
                                 tx.WriteColumn(r, col_id, Value(col_val), TXN_DEFERRED);
                             }
                         }
                         last = next + 1;
                     }
                     *res = SUCCESS;
                 } else {
                     *res = REJECT;
                 }
             }
        );

        RegP(3, 30,
             {}, {}, {}, {"usertable", {0}}, DF_REAL,
             PROC {
                 verify(cmd.input.size() >= 2);
                 std::string table_name = "usertable";
                 auto tbl = tx.GetTable(table_name);
                 
                 mdb::MultiBlob buf(1);
                 buf[0] = cmd.input[0].get_blob();
                 mdb::Row *r_exist = tx.Query(tbl, buf);
                 if (r_exist) {
                     *res = SUCCESS;
                     return;
                 }

                 std::string payload = cmd.input[1].get_str();
                 std::map<int, std::string> fieldVals;
                 size_t last = 0;
                 size_t next = 0;
                 while ((next = payload.find('|', last)) != std::string::npos) {
                     std::string pair = payload.substr(last, next - last);
                     size_t colon = pair.find(':');
                     if (colon != std::string::npos) {
                         std::string col_name = pair.substr(0, colon);
                         std::string col_val = pair.substr(colon + 1);
                         if (col_name.rfind("field", 0) == 0) {
                             int col_num = std::stoi(col_name.substr(5));
                             fieldVals[col_num + 1] = col_val;
                         }
                     }
                     last = next + 1;
                 }

        std::vector<Value> row_data(11);
                 row_data[0] = cmd.input[0]; 
                 for (int col = 1; col <= 10; ++col) {
                     auto it = fieldVals.find(col);
                     row_data[col] = Value(it != fieldVals.end() ? it->second : "");
                 }

                 mdb::Row* r = nullptr;
                 CREATE_ROW(tbl->schema(), row_data);
                 tx.InsertRow(tbl, r);
                 *res = SUCCESS;
             }
        );
    }
};

} // namespace janus

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

std::string serializeMap(const std::map<std::string, std::string>& m) {
    std::string ret = "";
    for (auto const& [k, v] : m) {
        ret += k + ":" + v + "|";
    }
    return ret;
}

} // namespace

class JanusYcsbClient : public BaseYcsbClient {
public:
    janus::Config* config_;
    janus::Frame* frame_;
    rrr::PollMgr* poll_mgr_;
    janus::Communicator* commo_;
    std::shared_ptr<janus::TxnRegistry> txn_reg_;
    janus::Workload* workload_;
    janus::Coordinator* coord_;
    uint32_t shardNum_;
    uint32_t replicaNum_;

    static std::atomic<uint32_t> nextCoordinatorId_;
    static std::once_flag config_once_;

    JanusYcsbClient(const std::string& configPath) {
        std::call_once(config_once_, [&configPath]() {
            std::vector<std::string> args = {
                "ycsb",
                "-f", configPath,
                "-P", "janus-lan-proxy-0000"
            };
            std::vector<char*> argv;
            for (const auto& arg : args) {
                argv.push_back(const_cast<char*>(arg.c_str()));
            }
            int argc = argv.size();

            int ret = janus::Config::CreateConfig(argc, argv.data());
            verify(ret == SUCCESS);
        });

        config_ = janus::Config::GetConfig();
        shardNum_ = config_->GetNumPartition();
        replicaNum_ = config_->GetPartitionSize(0);

        frame_ = janus::Frame::GetFrame(config_->tx_proto_);
        poll_mgr_ = new rrr::PollMgr(1);

        commo_ = frame_->CreateCommo(poll_mgr_);

        txn_reg_ = std::make_shared<janus::TxnRegistry>();
        workload_ = new janus::YcsbWorkload(config_);
        workload_->txn_reg_ = txn_reg_;
        workload_->RegisterPrecedures();

        char hostname[256];
        if (gethostname(hostname, sizeof(hostname)) != 0) {
            strcpy(hostname, "unknown");
        }
        uint32_t hostHash = std::hash<std::string>{}(hostname) & 0xFF;
        uint32_t timeSec = static_cast<uint32_t>(time(NULL)) & 0xFFFF;
        uint32_t client_id = nextCoordinatorId_.fetch_add(1) & 0xFF;
        uint32_t coo_id = (hostHash << 24) | (timeSec << 8) | client_id;

        auto client_infos = config_->GetMyClients();
        verify(!client_infos.empty());
        auto& my_site = client_infos[0];

        commo_->loc_id_ = my_site.locale_id;
        coord_ = frame_->CreateCoordinator(coo_id, config_, config_->benchmark_, nullptr, client_id, txn_reg_);
        coord_->loc_id_ = my_site.locale_id;
        coord_->commo_ = commo_;
    }

    ~JanusYcsbClient() override {
        delete coord_;
        delete workload_;
        delete commo_;
        poll_mgr_->release();
    }

    int execute(uint32_t txnType, const std::string& key, JNIEnv* env, jobject jfields, jobject jmap) override {
        std::cerr << "[DEBUG JNI] execute called type=" << txnType << " key=" << key << std::endl;
        janus::TxRequest req;
        req.tx_type_ = txnType;
        req.n_try_ = 20;

        req.input_[0] = janus::Value(key);

        if (txnType == 2 || txnType == 3) { // Update or Insert
            std::map<std::string, std::string> cppMap;
            extractJavaMap(env, jmap, cppMap);
            std::string payload = serializeMap(cppMap);
            std::cerr << "[DEBUG JNI] payload length=" << payload.length() << std::endl;
            req.input_[1] = janus::Value(payload);
        }

        auto promise = std::make_shared<std::promise<janus::TxReply>>();
        auto future = promise->get_future();

        req.callback_ = [promise](janus::TxReply& rep) {
            std::cerr << "[DEBUG JNI] callback triggered res=" << rep.res_ << std::endl;
            promise->set_value(rep);
        };

        std::cerr << "[DEBUG JNI] calling DoTxAsync..." << std::endl;
        coord_->DoTxAsync(req);

        std::cerr << "[DEBUG JNI] waiting for future..." << std::endl;
        janus::TxReply reply = future.get();
        std::cerr << "[DEBUG JNI] transaction done res=" << reply.res_ << std::endl;

        if (reply.res_ != SUCCESS) {
            return -1;
        }

        if (txnType == 1 && jmap) { // Read
            for (const auto& [col, val] : reply.output_) {
                int col_num = col - 1;
                std::string field_name = "field" + std::to_string(col_num);
                std::string field_val = (val.get_kind() == janus::Value::STR) ? val.get_str() : "";
                populateJavaMap(env, jmap, field_name, field_val);
            }
        }

        return 0;
    }
};

std::atomic<uint32_t> JanusYcsbClient::nextCoordinatorId_{0};
std::once_flag JanusYcsbClient::config_once_;

BaseYcsbClient* createJanusClient(const std::string& configPath) {
    return new JanusYcsbClient(configPath);
}
