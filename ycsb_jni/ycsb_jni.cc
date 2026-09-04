#include "com_tiga_ycsb_YcsbClient.h"
#include "ycsb_client.h"
#include <string>
#include <algorithm>
#include <cstdint>

static jfieldID g_fid_clientHandle = nullptr;
static jmethodID g_mid_mapPut = nullptr;

int32_t hashKey(const std::string& key) {
    std::hash<std::string> hasher;
    return static_cast<int32_t>((hasher(key) & 0x7FFFFFFF) % 2000005);
}

void populateJavaMap(JNIEnv* env, jobject jmap, const std::string& field, const std::string& value) {
    if (!g_mid_mapPut) {
        jclass mapClass = env->GetObjectClass(jmap);
        g_mid_mapPut = env->GetMethodID(mapClass, "put",
            "(Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;");
        env->DeleteLocalRef(mapClass);
    }
    
    jstring jkey = env->NewStringUTF(field.c_str());
    jstring jval = env->NewStringUTF(value.c_str());
    env->CallObjectMethod(jmap, g_mid_mapPut, jkey, jval);
    
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

    if (!g_fid_clientHandle) {
        jclass thisClass = env->GetObjectClass(obj);
        g_fid_clientHandle = env->GetFieldID(thisClass, "clientHandle", "J");
        env->DeleteLocalRef(thisClass);
    }

    try {
        BaseYcsbClient* client = nullptr;
#ifdef BUILD_JANUS_JNI
        if (mode == "janus") {
            client = createJanusClient(configPath);
        }
#else
        if (mode == "tiga") {
            client = createTigaClient(configPath);
        } else if (mode == "calvin") {
            client = createCalvinClient(configPath);
        } else if (mode == "detock") {
            client = createDetockClient(configPath);
        }
#endif
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

static jint runClientTxn(JNIEnv *env, jobject obj, jstring jkey, jobject jfields, jobject jmap, uint32_t txnType) {
    if (!g_fid_clientHandle) {
        jclass thisClass = env->GetObjectClass(obj);
        g_fid_clientHandle = env->GetFieldID(thisClass, "clientHandle", "J");
        env->DeleteLocalRef(thisClass);
    }

    jlong handle = env->GetLongField(obj, g_fid_clientHandle);
    BaseYcsbClient* client = reinterpret_cast<BaseYcsbClient*>(handle);
    if (!client) return -1;

    std::string key = jstring2string(env, jkey);
    return client->execute(txnType, key, env, jfields, jmap);
}

JNIEXPORT jint JNICALL Java_com_tiga_ycsb_YcsbClient_read(JNIEnv *env, jobject obj, jstring jkey, jobject jfields, jobject jmap) {
    return runClientTxn(env, obj, jkey, jfields, jmap, 1);
}

JNIEXPORT jint JNICALL Java_com_tiga_ycsb_YcsbClient_update(JNIEnv *env, jobject obj, jstring jkey, jobject jmap) {
    return runClientTxn(env, obj, jkey, nullptr, jmap, 2);
}

JNIEXPORT jint JNICALL Java_com_tiga_ycsb_YcsbClient_insert(JNIEnv *env, jobject obj, jstring jkey, jobject jmap) {
    return runClientTxn(env, obj, jkey, nullptr, jmap, 3);
}

JNIEXPORT jint JNICALL Java_com_tiga_ycsb_YcsbClient_transfer(JNIEnv *env, jobject obj, jstring jkey1, jstring jkey2, jstring jfield) {
    if (!g_fid_clientHandle) {
        jclass thisClass = env->GetObjectClass(obj);
        g_fid_clientHandle = env->GetFieldID(thisClass, "clientHandle", "J");
        env->DeleteLocalRef(thisClass);
    }

    jlong handle = env->GetLongField(obj, g_fid_clientHandle);
    BaseYcsbClient* client = reinterpret_cast<BaseYcsbClient*>(handle);
    if (!client) return -1;

    std::string key1 = jstring2string(env, jkey1);
    std::string key2 = jstring2string(env, jkey2);
    std::string field = jstring2string(env, jfield);

    return client->transfer(key1, key2, field, env);
}

JNIEXPORT jint JNICALL Java_com_tiga_ycsb_YcsbClient_swap(JNIEnv *env, jobject obj, jobjectArray jkeys, jstring jfield) {
    if (!g_fid_clientHandle) {
        jclass thisClass = env->GetObjectClass(obj);
        g_fid_clientHandle = env->GetFieldID(thisClass, "clientHandle", "J");
        env->DeleteLocalRef(thisClass);
    }

    jlong handle = env->GetLongField(obj, g_fid_clientHandle);
    BaseYcsbClient* client = reinterpret_cast<BaseYcsbClient*>(handle);
    if (!client) return -1;

    std::string field = jstring2string(env, jfield);

    jsize len = env->GetArrayLength(jkeys);
    std::vector<std::string> keys;
    keys.reserve(len);
    for (jsize i = 0; i < len; i++) {
        jstring jkey = (jstring)env->GetObjectArrayElement(jkeys, i);
        keys.push_back(jstring2string(env, jkey));
        env->DeleteLocalRef(jkey);
    }

    return client->swap(keys, field, env);
}
