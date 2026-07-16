#include "ycsb.h"
#include "deptran/sharding.h"
#include "deptran/constants.h"
#include "deptran/config.h"

namespace janus {

YcsbChopper::YcsbChopper() {}

YcsbChopper::~YcsbChopper() {}

void YcsbChopper::Init(TxRequest &req) {
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
    
    status_ = {{piece_type, DISPATCHABLE}};
    n_pieces_all_ = 1;
}

bool YcsbChopper::HandleOutput(int pi, int res, std::map<int32_t, Value> &output) {
    return false;
}

bool YcsbChopper::IsReadOnly() {
    return type_ == 1; 
}

void YcsbChopper::Reset() {
    TxData::Reset();
    for (auto& pair : status_) {
        pair.second = DISPATCHABLE;
    }
    commit_.store(true);
    partition_ids_.clear();
    n_pieces_dispatchable_ = 1;
    n_try_++;
}

YcsbWorkload::YcsbWorkload(Config* config) : Workload(config) {}

YcsbWorkload::~YcsbWorkload() {}

void YcsbWorkload::GetTxRequest(TxRequest* req, uint32_t cid) {
    verify(0); 
}

void YcsbWorkload::RegisterPrecedures() {
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
                 row_data[col] = Value(fieldVals[col]);
             }

             mdb::Row* r = nullptr;
             CREATE_ROW(tbl->schema(), row_data);
             tx.InsertRow(tbl, r);
             *res = SUCCESS;
         }
    );
}

} // namespace janus
