#!/usr/bin/env bash
set -euo pipefail

readonly srt_repository="https://github.com/Haivision/srt.git"
readonly srt_tag="v1.5.7"
readonly srt_commit="899348d8318eb9a3c5a5b6ec43c4a1114288773a"

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
target_dir="${CARGO_TARGET_DIR:-$project_root/target}"
if [[ "$target_dir" != /* ]]; then
    target_dir="$project_root/$target_dir"
fi
srt_root="$target_dir/libsrt"
srt_source="$srt_root/source-$srt_commit"
srt_build="$srt_root/build-$srt_commit"
srt_prefix="$srt_root/install-$srt_commit"

if [[ ! -d "$srt_source/.git" ]]; then
    mkdir -p "$srt_root"
    git clone --filter=blob:none --no-checkout "$srt_repository" "$srt_source"
fi

current_commit="$(git -C "$srt_source" rev-parse HEAD)"
if [[ "$current_commit" != "$srt_commit" ]]; then
    git -C "$srt_source" fetch --depth 1 origin "$srt_commit"
    git -C "$srt_source" checkout --detach "$srt_commit"
fi
actual_commit="$(git -C "$srt_source" rev-parse HEAD)"
if [[ "$actual_commit" != "$srt_commit" ]]; then
    printf 'Expected libsrt %s (%s), got %s\n' "$srt_tag" "$srt_commit" "$actual_commit" >&2
    exit 1
fi

cmake -S "$srt_source" -B "$srt_build" \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_PREFIX="$srt_prefix" \
    -DENABLE_APPS=OFF \
    -DENABLE_SHARED=OFF \
    -DENABLE_STATIC=ON \
    -DENABLE_UNITTESTS=OFF \
    -DENABLE_ENCRYPTION=ON \
    -DUSE_ENCLIB=openssl
cmake --build "$srt_build" --parallel
cmake --install "$srt_build"

export PKG_CONFIG_PATH="$srt_prefix/lib/pkgconfig:$srt_prefix/lib64/pkgconfig${PKG_CONFIG_PATH:+:$PKG_CONFIG_PATH}"
unset PKG_CONFIG_ALL_STATIC
pkg-config --atleast-version=1.5 srt

case "$(uname -s)" in
    Linux)
        srt_link_flags="-C link-arg=-lstdc++ -C link-arg=-lssl -C link-arg=-lcrypto"
        ;;
    Darwin)
        srt_link_flags="-C link-arg=-lssl -C link-arg=-lcrypto"
        ;;
    *)
        printf 'Static SRT linking is not configured for %s\n' "$(uname -s)" >&2
        exit 1
        ;;
esac
export RUSTFLAGS="${RUSTFLAGS:+$RUSTFLAGS }$srt_link_flags"

cd "$project_root"
exec cargo "$@"
