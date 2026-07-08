#ifndef YCSB_CLIENT_H
#define YCSB_CLIENT_H

#include <string>
#include <cstdint>
#include <jni.h>

class BaseYcsbClient {
public:
    virtual ~BaseYcsbClient() {}
    virtual int execute(uint32_t txnType, const std::string& key, JNIEnv* env, jobject jmap) = 0;
};

BaseYcsbClient* createTigaClient(const std::string& configPath);
BaseYcsbClient* createCalvinClient(const std::string& configPath);
BaseYcsbClient* createDetockClient(const std::string& configPath);

int32_t hashKey(const std::string& key);
void populateJavaMap(JNIEnv* env, jobject jmap, const std::string& field, const std::string& value);

#endif // YCSB_CLIENT_H
