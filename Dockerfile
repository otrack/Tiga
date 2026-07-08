# =========================================================
# Stage 1: Build Tiga, Calvin, and Detock
# =========================================================
FROM ubuntu:24.04 AS builder

ENV DEBIAN_FRONTEND=noninteractive

# Install compilation toolchain and packages required for build
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    gnupg \
    unzip \
    git \
    libgflags-dev \
    libgoogle-glog-dev \
    libyaml-cpp-dev \
    libboost-all-dev \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Bazelisk as the default bazel executable
RUN curl -L https://github.com/bazelbuild/bazelisk/releases/latest/download/bazelisk-linux-amd64 -o /usr/local/bin/bazel && \
    chmod +x /usr/local/bin/bazel

WORKDIR /build

# Copy codebase
COPY . .

# Build Calvin, Detock, Tiga servers and JNI client library
RUN bazel build //TigaEntity:TigaServer \
                //CalvinEntity:CalvinServer \
                //DetockEntity:DetockServer \
                //ycsb_jni:libtigaycsb.so

# =========================================================
# Stage 2: Runtime Image
# =========================================================
FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive

# Install lightweight runtime libraries only
RUN apt-get update && apt-get install -y \
    libgflags2.2 \
    libgoogle-glog0v6 \
    libyaml-cpp0.8 \
    libboost-filesystem1.83.0 \
    libboost-thread1.83.0 \
    libboost-coroutine1.83.0 \
    libboost-context1.83.0 \
    libssl3 \
    iproute2 \
    && rm -rf /var/lib/apt/lists/*

# Copy compiled binaries from builder stage
COPY --from=builder /build/bazel-bin/TigaEntity/TigaServer /usr/local/bin/
COPY --from=builder /build/bazel-bin/CalvinEntity/CalvinServer /usr/local/bin/
COPY --from=builder /build/bazel-bin/DetockEntity/DetockServer /usr/local/bin/
COPY --from=builder /build/bazel-bin/ycsb_jni/libtigaycsb.so /usr/local/lib/

WORKDIR /app

# Copy configuration files and run script
RUN mkdir -p /app/config
COPY config-*.yml tpcc.yml /app/config/
COPY entrypoint.sh /app/

RUN chmod +x /app/entrypoint.sh

ENTRYPOINT ["/app/entrypoint.sh"]
