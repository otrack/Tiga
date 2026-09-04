# Stage 1: Build stage
FROM ubuntu:24.04 AS builder

ENV DEBIAN_FRONTEND=noninteractive

# Install build tools and development libraries
RUN apt-get update && apt-get install -y \
    curl \
    gnupg \
    g++ \
    make \
    python3 \
    libgflags-dev \
    libgoogle-glog-dev \
    libyaml-cpp-dev \
    libboost-dev \
    libboost-system-dev \
    libboost-filesystem-dev \
    libboost-thread-dev \
    libboost-coroutine-dev \
    libboost-context-dev \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Bazelisk as bazel
RUN curl -fsSL https://github.com/bazelbuild/bazelisk/releases/latest/download/bazelisk-linux-amd64 -o /usr/local/bin/bazel && \
    chmod +x /usr/local/bin/bazel

WORKDIR /build

# Copy the source code
COPY . .

# Build targets inside the container (using a cache mount for incremental Bazel builds)
# We copy the built binaries out of the bazel-bin symlink target into a real folder (/build/out) before the cache is unmounted
RUN --mount=type=cache,target=/root/.cache/bazel \
    bazel build //TigaEntity:TigaServer //CalvinEntity:CalvinServer //DetockEntity:DetockServer //ycsb_jni:libtigaycsb.so //ycsb_jni:libjanusycsb.so //ncc/janus:deptran_server && \
    mkdir -p /build/out/TigaEntity /build/out/CalvinEntity /build/out/DetockEntity /build/out/ycsb_jni /build/out/ncc/janus && \
    cp -L bazel-bin/TigaEntity/TigaServer /build/out/TigaEntity/ && \
    cp -L bazel-bin/CalvinEntity/CalvinServer /build/out/CalvinEntity/ && \
    cp -L bazel-bin/DetockEntity/DetockServer /build/out/DetockEntity/ && \
    cp -L bazel-bin/ycsb_jni/libtigaycsb.so /build/out/ycsb_jni/ && \
    cp -L bazel-bin/ycsb_jni/libjanusycsb.so /build/out/ycsb_jni/ && \
    cp -L bazel-bin/ncc/janus/deptran_server /build/out/ncc/janus/

# Stage 2: Runtime stage
FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive

# Install runtime libraries
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

# Copy compiled binaries and libraries from the build stage (from the real /build/out folder)
COPY --from=builder /build/out/TigaEntity/TigaServer /usr/local/bin/
COPY --from=builder /build/out/CalvinEntity/CalvinServer /usr/local/bin/
COPY --from=builder /build/out/DetockEntity/DetockServer /usr/local/bin/
COPY --from=builder /build/out/ycsb_jni/libtigaycsb.so /usr/local/lib/
COPY --from=builder /build/out/ycsb_jni/libjanusycsb.so /usr/local/lib/
COPY --from=builder /build/out/ncc/janus/deptran_server /usr/local/bin/
COPY --from=builder /build/ycsb_jni/YcsbClient.java /usr/local/share/java/com/tiga/ycsb/YcsbClient.java

WORKDIR /app

# Copy configuration files and run script
RUN mkdir -p /app/config
COPY config-*.yml tpcc.yml /app/config/
COPY entrypoint.sh /app/

RUN chmod +x /app/entrypoint.sh

ENTRYPOINT ["/app/entrypoint.sh"]
