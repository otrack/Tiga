# Janus Protocol & YCSB Integration Status

## 1. Overview
This document summarizes the work performed, bug fixes implemented, and current status of integrating the **Janus** protocol (under `ncc/janus`) with the YCSB benchmark harness (`0track/ycsb`).

---

## 2. Completed Work & Bug Fixes

### A. JNI Binding & Linkage Architecture
- **Bazel Build Targets**:
  - Maintained separate Bazel `cc_binary` targets (`//ycsb_jni:libtigaycsb.so` and `//ycsb_jni:libjanusycsb.so`) in `/home/otrack/Implementation/Tiga/ycsb_jni/BUILD`.
  - *Rationale*: Prevents C++ symbol collisions between Janus's internal `rrr` RPC framework (`ncc/janus/src/rrr`) and Tiga's top-level `rrrLib`.
- **Dynamic Java/JNI Library Selection**:
  - Updated `YcsbClient.java` in YCSB to dynamically select and extract either `libjanusycsb.so` (for `mode=janus`) or `libtigaycsb.so` (for `tiga`/`calvin`/`detock`) from Maven JAR resources at runtime.
- **JAR Resource Packaging**:
  - Native libraries (`libjanusycsb.so` and `libtigaycsb.so`) are packaged directly into `tiga-binding-0.18.0-SNAPSHOT.jar` under `src/main/resources/`, removing the need for manual file copying across Docker build stages.

### B. Linker & Symbol Resolution Fixes
- **`UnsatisfiedLinkError: undefined symbol: client_workers_g`**:
  - *Root Cause*: Janus's `benchmark_control_rpc.cc` referenced `extern vector<unique_ptr<ClientWorker>> client_workers_g;` in the global namespace (matching `s_main.cc`).
  - *Fix*: Defined `std::vector<std::unique_ptr<janus::ClientWorker>> client_workers_g;` in the global scope (outside `namespace janus`) in `ycsb_jni/janus_ycsb.cc`.

### C. Janus Core & Memory Safety Fixes
- **Partition Metadata & Workspace Initialization**:
  - Added `partition_ids_.insert(sharding_[piece_type])` to `YcsbChopper::Init` and `YcsbChopper::Reset` in both `ncc/janus/src/bench/ycsb/ycsb.cc` and `ycsb_jni/janus_ycsb.cc`.
  - Used `GetWorkspace(piece_type)` in `YcsbChopper::Init` and `Reset()` to properly link piece inputs (`inputs_[piece_type]`) to transaction request workspaces before RPC dispatch.
- **`mdb::Value` Move/Copy Safety**:
  - Patched `Value(Value&& o)`, `operator=(Value&& that)`, `Value(const Value& o)`, `operator=(const Value& o)`, `set_str`, and `~Value()` in `ncc/janus/src/memdb/value.h`.
  - *Fix*: Ensured `o.k_` is set to `UNKNOWN` when moving string pointers, and added `nullptr` guards to prevent dereferencing dangling or null `p_str_` pointers during RPC value unmarshaling.
- **Safe Proxy Lookups in `Communicator`**:
  - Updated `Communicator::NearestProxyForPartition` in `ncc/janus/src/deptran/communicator.cc` to use modulo indexing (`loc_id_ % partition_proxies.size()`) instead of strict array bounds assertion.
  - Initialized `commo_->loc_id_ = my_site.locale_id` in `JanusYcsbClient`'s constructor.

---

## 3. Current State of the Game

### Operational Status
- ✅ **Dynamic JNI Loading**: `libjanusycsb.so` extracts and loads cleanly in YCSB without `UnsatisfiedLinkError`.
- ✅ **Server Connection Setup**: Janus client connects to all database server nodes (`database-node1:10000`, `database-node2:20000`, `database-node3:30000`).
- ✅ **RPC Dispatch Initiation**: `DoTxAsync` creates `YcsbChopper` instances, sets up transaction IDs, and successfully initiates RPC dispatch over `TroadCommo`.

### Remaining Issue
- ⚠️ **RPC Response Unmarshaling / Future Evaluation**:
  - When asynchronous transaction futures (`future.get()`) resolve upon receiving RPC replies from Janus servers, `std::length_error basic_string::_M_create` is thrown inside the coroutine callback thread during RPC output unmarshaling in `TroadCommo`.

---

## 4. Next Steps (If Janus Integration is Revisited)
1. **Trace `TroadCommo` RPC Reply Marshaling**:
   Inspect `proxy->async_JanusDispatch` and `MarshalDeputy` unmarshaling in `ncc/janus/src/deptran/troad/commo.cc` to ensure `TxnOutput` and `RccGraph` stream operators (`operator>>`) correctly deserialize custom YCSB output maps.
2. **Coroutine Stack Depth**:
   Verify boost coroutine stack allocation sizes in `rrr::Coroutine::BoostRunWrapper` under JNI threads.
