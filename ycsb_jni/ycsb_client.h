#ifndef YCSB_CLIENT_H
#define YCSB_CLIENT_H

#include <string>
#include <cstdint>
#include <jni.h>

class BaseYcsbClient {
public:
    virtual ~BaseYcsbClient() {}
    virtual int execute(uint32_t txnType, const std::string& key, JNIEnv* env, jobject jfields, jobject jmap) = 0;
    virtual int transfer(const std::string& key1, const std::string& key2, const std::string& field, JNIEnv* env) { return -1; }
};

BaseYcsbClient* createTigaClient(const std::string& configPath);
BaseYcsbClient* createCalvinClient(const std::string& configPath);
BaseYcsbClient* createDetockClient(const std::string& configPath);
BaseYcsbClient* createJanusClient(const std::string& configPath);

int32_t hashKey(const std::string& key);
void populateJavaMap(JNIEnv* env, jobject jmap, const std::string& field, const std::string& value);

#endif // YCSB_CLIENT_H
