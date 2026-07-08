#include "com_tiga_ycsb_YcsbClient.h"
#include "ycsb_client.h"
#include <string>
#include <algorithm>
#include <cstdint>

int32_t hashKey(const std::string& key) {
    std::hash<std::string> hasher;
    return static_cast<int32_t>((hasher(key) & 0x7FFFFFFF) % 2000005);
}

void populateJavaMap(JNIEnv* env, jobject jmap, const std::string& field, const std::string& value) {
    jclass mapClass = env->GetObjectClass(jmap);
    jmethodID putMethod = env->GetMethodID(mapClass, "put",
        "(Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;");
    
    jstring jkey = env->NewStringUTF(field.c_str());
    jstring jval = env->NewStringUTF(value.c_str());
    env->CallObjectMethod(jmap, putMethod, jkey, jval);
    
    env->DeleteLocalRef(jkey);
    env->DeleteLocalRef(jval);
}

static std::string jstring2string(JNIEnv* env, jstring jstr) {
    if (!jstr) return "";
    const char* str = env->GetStringUTFChars(jstr, nullptr);
    std::string cppstr(str);
    env->ReleaseStringUTFChars(jstr, str);
    return cppstr;
}

JNIEXPORT jlong JNICALL Java_com_tiga_ycsb_YcsbClient_initClient(JNIEnv *env, jobject obj, jstring jconfigPath, jstring jmode) {
    std::string configPath = jstring2string(env, jconfigPath);
    std::string mode = jstring2string(env, jmode);
    
    std::transform(mode.begin(), mode.end(), mode.begin(), ::tolower);

    try {
        BaseYcsbClient* client = nullptr;
        if (mode == "tiga") {
            client = createTigaClient(configPath);
        } else if (mode == "calvin") {
            client = createCalvinClient(configPath);
        } else if (mode == "detock") {
            client = createDetockClient(configPath);
        }
        return reinterpret_cast<jlong>(client);
    } catch (const std::exception& e) {
        return 0;
    }
}

JNIEXPORT void JNICALL Java_com_tiga_ycsb_YcsbClient_closeClient(JNIEnv *env, jobject obj, jlong handle) {
    BaseYcsbClient* client = reinterpret_cast<BaseYcsbClient*>(handle);
    if (client) {
        delete client;
    }
}

static jint runClientTxn(JNIEnv *env, jobject obj, jstring jkey, jobject jmap, uint32_t txnType) {
    jclass thisClass = env->GetObjectClass(obj);
    jfieldID fid = env->GetFieldID(thisClass, "clientHandle", "J");
    jlong handle = env->GetLongField(obj, fid);
    BaseYcsbClient* client = reinterpret_cast<BaseYcsbClient*>(handle);
    if (!client) return -1;

    std::string key = jstring2string(env, jkey);
    return client->execute(txnType, key, env, jmap);
}

JNIEXPORT jint JNICALL Java_com_tiga_ycsb_YcsbClient_read(JNIEnv *env, jobject obj, jstring jkey, jobject jmap) {
    return runClientTxn(env, obj, jkey, jmap, 1);
}

JNIEXPORT jint JNICALL Java_com_tiga_ycsb_YcsbClient_update(JNIEnv *env, jobject obj, jstring jkey, jobject jmap) {
    return runClientTxn(env, obj, jkey, jmap, 2);
}

JNIEXPORT jint JNICALL Java_com_tiga_ycsb_YcsbClient_insert(JNIEnv *env, jobject obj, jstring jkey, jobject jmap) {
    return runClientTxn(env, obj, jkey, jmap, 3);
}
