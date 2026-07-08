load("@rules_cc//cc:defs.bzl", "cc_library", "cc_binary")

cc_binary(
    name = "libtigaycsb.so",
    srcs = [
        "ycsb_jni.cc",
        "tiga_ycsb.cc",
        "calvin_ycsb.cc",
        "detock_ycsb.cc",
        "ycsb_client.h",
        "com_tiga_ycsb_YcsbClient.h",
        "include/jni.h",
        "include/jni_md.h",
    ],
    includes = [
        "include",
    ],
    copts = [
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
    linkshared = True,
    deps = [
        "//TigaEntity:TigaCoordinator",
        "//CalvinEntity:CalvinCoordinator",
        "//DetockEntity:DetockCoordinator",
        "//rrr:rrrLib",
    ],
)
