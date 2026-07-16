#pragma once

#include "deptran/__dep__.h"
#include "deptran/procedure.h"
#include "deptran/workload.h"

namespace janus {

class YcsbChopper : public TxData {
public:
    YcsbChopper();
    virtual ~YcsbChopper();

    virtual void Init(TxRequest &req) override;
    virtual bool HandleOutput(int pi, int res, std::map<int32_t, Value> &output) override;
    virtual bool IsReadOnly() override;
    virtual void Reset() override;
};

class YcsbWorkload : public Workload {
public:
    YcsbWorkload(Config* config);
    virtual ~YcsbWorkload();

    virtual void GetTxRequest(TxRequest* req, uint32_t cid) override;
    virtual void RegisterPrecedures() override;
};

} // namespace janus
