package com.tiga.ycsb;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class YcsbClient {
  private static final Set<String> LOADED_LIBRARIES = new HashSet<>();

  private static synchronized void ensureLibraryLoaded(String mode) {
    String libShortName = "janus".equalsIgnoreCase(mode) ? "janusycsb" : "tigaycsb";
    if (LOADED_LIBRARIES.contains(libShortName)) {
      return;
    }
    try {
      System.loadLibrary(libShortName);
    } catch (UnsatisfiedLinkError e) {
      try {
        loadFromJar(libShortName);
      } catch (Exception ex) {
        System.err.println("Failed to load native library " + libShortName + ": " + e.getMessage());
        System.err.println("Failed to load native library from JAR resources: " + ex.getMessage());
        throw new RuntimeException(ex);
      }
    }
    LOADED_LIBRARIES.add(libShortName);
  }

  private static void loadFromJar(String libShortName) throws Exception {
    String resourceName = "lib" + libShortName + ".so";
    java.io.InputStream in = YcsbClient.class.getClassLoader().getResourceAsStream(resourceName);
    if (in == null) {
      in = YcsbClient.class.getResourceAsStream("/" + resourceName);
    }
    if (in == null && Thread.currentThread().getContextClassLoader() != null) {
      in = Thread.currentThread().getContextClassLoader().getResourceAsStream(resourceName);
    }
    if (in == null) {
      throw new java.io.FileNotFoundException("Library " + resourceName + " not found in JAR resources");
    }
    java.io.File tempFile = java.io.File.createTempFile("lib" + libShortName, ".so");
    tempFile.deleteOnExit();
    try (java.io.FileOutputStream out = new java.io.FileOutputStream(tempFile)) {
      byte[] buffer = new byte[8192];
      int read;
      while ((read = in.read(buffer)) != -1) {
        out.write(buffer, 0, read);
      }
    }
    System.load(tempFile.getAbsolutePath());
  }

  // Pointer to the underlying C++ wrapper object (stored as a long)
  private final long clientHandle;

  public YcsbClient(String configPath, String mode) {
    ensureLibraryLoaded(mode);
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
