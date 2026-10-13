# The image is built incrementally: only the Bazel TARGETS are compiled, and
# every other binary is taken from PREV_IMAGE, usually the previous build of
# this image.  By default, only the Tiga server and its YCSB client library are
# rebuilt, as the YCSB benchmarks only change those:
#
#   docker build -t 0track/tiga-suite .
#
# After changing the Calvin, Detock or Janus (ncc) servers, build them too, or
# rebuild everything without a previous image:
#
#   docker build -t 0track/tiga-suite --build-arg PREV_IMAGE=scratch \
#     --build-arg TARGETS="//TigaEntity:TigaServer //CalvinEntity:CalvinServer //DetockEntity:DetockServer //ycsb_jni:libtigaycsb.so //ycsb_jni:libjanusycsb.so //ncc/janus:deptran_server" .
#
# Bazel keeps its cache in a BuildKit cache mount, so a rebuild only
# recompiles what changed, unless that cache has been pruned
# (docker builder prune).
ARG PREV_IMAGE=0track/tiga-suite:latest
ARG TARGETS="//TigaEntity:TigaServer //ycsb_jni:libtigaycsb.so"

FROM ${PREV_IMAGE} AS prev

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

# Build the targets inside the container (using a cache mount for incremental
# Bazel builds), and copy the binaries out of the bazel-bin symlink into a real
# folder (/build/out) before the cache is unmounted.  The Bazelisk download is
# cached as well.
ARG TARGETS
RUN --mount=type=cache,target=/root/.cache/bazel \
    --mount=type=cache,target=/root/.cache/bazelisk \
    bazel build ${TARGETS} && \
    mkdir -p /build/out && \
    for t in ${TARGETS}; do \
        p=${t#//}; cp -L "bazel-bin/${p%%:*}/${p#*:}" /build/out/; \
    done

# Stage 2: the binaries of the image: those of the previous image, replaced by
# the ones just built
FROM ubuntu:24.04 AS artifacts
RUN --mount=from=prev,target=/prev \
    mkdir -p /out && \
    for f in bin/TigaServer bin/CalvinServer bin/DetockServer bin/deptran_server \
             lib/libtigaycsb.so lib/libjanusycsb.so; do \
        if [ -f "/prev/usr/local/$f" ]; then cp "/prev/usr/local/$f" /out/; fi; \
    done
COPY --from=builder /build/out/ /out/
RUN for f in TigaServer CalvinServer DetockServer deptran_server libtigaycsb.so libjanusycsb.so; do \
        if [ ! -f "/out/$f" ]; then \
            echo "No $f: add its target to TARGETS, or set PREV_IMAGE to an image that has it" >&2; exit 1; \
        fi; \
    done

# Stage 3: Runtime stage
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

# Copy the binaries and libraries
COPY --from=artifacts /out/TigaServer /out/CalvinServer /out/DetockServer /out/deptran_server /usr/local/bin/
COPY --from=artifacts /out/libtigaycsb.so /out/libjanusycsb.so /usr/local/lib/
COPY --from=builder /build/ycsb_jni/YcsbClient.java /usr/local/share/java/com/tiga/ycsb/YcsbClient.java

WORKDIR /app

# Copy configuration files and run script
RUN mkdir -p /app/config
COPY config-*.yml tpcc.yml /app/config/
COPY entrypoint.sh /app/

RUN chmod +x /app/entrypoint.sh

ENTRYPOINT ["/app/entrypoint.sh"]
