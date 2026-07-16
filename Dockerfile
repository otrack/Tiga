FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive

# Install lightweight runtime libraries only
RUN apt-get update && apt-get install -y \
    libgflags2.2 \
    libgoogle-glog0v6 \
    libyaml-cpp0.8 \
    libboost-system1.83.0 \
    libboost-filesystem1.83.0 \
    libboost-thread1.83.0 \
    libboost-coroutine1.83.0 \
    libboost-context1.83.0 \
    libssl3 \
    iproute2 \
    && rm -rf /var/lib/apt/lists/*

# Copy precompiled binaries from host's bazel-bin
COPY bazel-bin/TigaEntity/TigaServer /usr/local/bin/
COPY bazel-bin/CalvinEntity/CalvinServer /usr/local/bin/
COPY bazel-bin/DetockEntity/DetockServer /usr/local/bin/
COPY bazel-bin/ycsb_jni/libtigaycsb.so /usr/local/lib/
COPY bazel-bin/ycsb_jni/libjanusycsb.so /usr/local/lib/
COPY bazel-bin/ncc/janus/deptran_server /usr/local/bin/

WORKDIR /app

# Copy configuration files and run script
RUN mkdir -p /app/config
COPY config-*.yml tpcc.yml /app/config/
COPY entrypoint.sh /app/

RUN chmod +x /app/entrypoint.sh

ENTRYPOINT ["/app/entrypoint.sh"]
