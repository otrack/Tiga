package com.tiga.ycsb;

import java.util.Map;
import java.util.Set;

public class YcsbClient {
    static {
        try {
            System.loadLibrary("tigaycsb");
        } catch (UnsatisfiedLinkError e) {
            System.err.println("Failed to load native library tigaycsb: " + e.getMessage());
        }
    }

    // Pointer to the underlying C++ wrapper object (stored as a long)
    private final long clientHandle;

    public YcsbClient(String configPath, String mode) {
        this.clientHandle = initClient(configPath, mode);
        if (this.clientHandle == 0) {
            throw new RuntimeException("Failed to initialize native " + mode + " client");
        }
    }

    public void close() {
        if (clientHandle != 0) {
            closeClient(clientHandle);
        }
    }

    // Native API mappings
    private native long initClient(String configPath, String mode);
    private native void closeClient(long handle);

    public native int read(String key, Set<String> fields, Map<String, String> result);
    public native int update(String key, Map<String, String> values);
    public native int insert(String key, Map<String, String> values);
    public native int transfer(String key1, String key2, String field);
    public native int swap(String[] keys, String field);
}
