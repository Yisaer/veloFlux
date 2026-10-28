# Keep the build and smoke-test userlands on the same explicit glibc baseline.
ARG BASE_IMAGE=almalinux:8.10@sha256:8b469a3a78515e8a18ea8fc727e6a3679e1d0c5ba6f58d5b30be3d0d9b86cffe
FROM ${BASE_IMAGE} AS runtime
# Use the canonical 8.10 repositories instead of a separately hosted mirrorlist.
RUN sed -i -e '/^mirrorlist=/d' \
        -e 's|^# baseurl=https://repo.almalinux.org/almalinux/$releasever/|baseurl=https://repo.almalinux.org/almalinux/8.10/|' \
        /etc/yum.repos.d/almalinux*.repo \
    && dnf install -y ca-certificates libgcc python3 \
    && dnf clean all \
    && test "$(getconf GNU_LIBC_VERSION)" = 'glibc 2.28'

FROM runtime AS video-runtime
RUN dnf install -y gstreamer1 gstreamer1-plugins-base && dnf clean all

FROM video-runtime AS builder
RUN dnf install -y gcc-toolset-14-gcc gcc-toolset-14-gcc-c++ make git curl \
        binutils m4 pkgconf-pkg-config unzip xz cmake \
        gstreamer1-devel gstreamer1-plugins-base-devel \
    && dnf clean all
ENV PATH=/opt/rust/bin:/opt/rh/gcc-toolset-14/root/usr/bin:/usr/local/bin:/usr/bin:/bin
ARG TARGETARCH
# Pin the native toolchain and protoc, including their upstream archive checksums.
RUN set -eu; \
    case "$TARGETARCH" in \
      amd64) rust_arch=x86_64; proto_arch=x86_64; \
        rust_sha=88f28fa9af20594179f85d6df67078dfd6fa93e2f6da5e1e9b0ac4997988ca4f; \
        proto_sha=44a6b498e996b845edef83864734c0e52f42197e85c9d567af55f4e3ff09d755 ;; \
      arm64) rust_arch=aarch64; proto_arch=aarch_64; \
        rust_sha=9a7a2c336b4787f1b72f6bab7c35d5b7af2fd03cbd39b4fc721466a70d402a7d; \
        proto_sha=220aaf1d56327388acd30f869d7ee097bf8066c891bd0c9f72b64365e6bad73f ;; \
      *) echo "unsupported build architecture: $TARGETARCH" >&2; exit 1 ;; \
    esac; \
    rust_target="$rust_arch-unknown-linux-gnu"; \
    cd /tmp; \
    curl -fsSL --retry 3 "https://static.rust-lang.org/dist/rust-1.97.1-$rust_target.tar.xz" -o rust.tar.xz; \
    echo "$rust_sha  rust.tar.xz" | sha256sum -c -; \
    tar -xf rust.tar.xz; \
    ./rust-1.97.1-"$rust_target"/install.sh --prefix=/opt/rust \
        --components="rustc,cargo,rust-std-$rust_target" --disable-ldconfig; \
    curl -fsSL --retry 3 "https://github.com/protocolbuffers/protobuf/releases/download/v3.20.3/protoc-3.20.3-linux-$proto_arch.zip" -o protoc.zip; \
    echo "$proto_sha  protoc.zip" | sha256sum -c -; \
    unzip -q protoc.zip -d /opt/protoc; \
    ln -s /opt/protoc/bin/protoc /usr/local/bin/protoc; \
    rm -rf /tmp/rust* /tmp/protoc.zip; \
    rustc --version; cargo --version; protoc --version
WORKDIR /workspace
