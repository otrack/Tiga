load("@rules_cc//cc:defs.bzl", "cc_library", "cc_binary")

cc_library(
    name = "janus_ycsb_lib",
    srcs = [
        "janus_ycsb.cc",
    ],
    hdrs = [
        "ycsb_client.h",
        "com_tiga_ycsb_YcsbClient.h",
        "include/jni.h",
        "include/jni_md.h",
    ],
    includes = [
        "include",
    ],
    deps = [
        "//ncc/janus:janus_lib",
    ],
)

cc_library(
    name = "other_ycsb_lib",
    srcs = [
        "tiga_ycsb.cc",
        "calvin_ycsb.cc",
        "detock_ycsb.cc",
    ],
    hdrs = [
        "ycsb_client.h",
        "com_tiga_ycsb_YcsbClient.h",
        "include/jni.h",
        "include/jni_md.h",
    ],
    includes = [
        "include",
    ],
    deps = [
        "//TigaEntity:TigaCoordinator",
        "//CalvinEntity:CalvinCoordinator",
        "//DetockEntity:DetockCoordinator",
        "//rrr:rrrLib",
    ],
)

cc_binary(
    name = "libtigaycsb.so",
    srcs = [
        "ycsb_jni.cc",
        "ycsb_client.h",
        "com_tiga_ycsb_YcsbClient.h",
        "include/jni.h",
        "include/jni_md.h",
    ],
    includes = [
        "include",
    ],
    linkshared = True,
    deps = [
        ":other_ycsb_lib",
    ],
    linkopts = [
        "-L/usr/local/lib",
        "-pthread",
        "-lgflags",
        "-lglog",
        "-lcrypto",
        "-lyaml-cpp",
        "-lboost_filesystem",
        "-lboost_coroutine",
        "-lboost_context"
    ],
)

cc_binary(
    name = "libjanusycsb.so",
    srcs = [
        "ycsb_jni.cc",
        "ycsb_client.h",
        "com_tiga_ycsb_YcsbClient.h",
        "include/jni.h",
        "include/jni_md.h",
    ],
    includes = [
        "include",
    ],
    copts = [
        "-DBUILD_JANUS_JNI",
    ],
    linkshared = True,
    deps = [
        ":janus_ycsb_lib",
    ],
    linkopts = [
        "-L/usr/local/lib",
        "-pthread",
        "-lgflags",
        "-lglog",
        "-lcrypto",
        "-lyaml-cpp",
        "-lboost_filesystem",
        "-lboost_coroutine",
        "-lboost_context"
    ],
)
