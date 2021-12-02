#!/usr/bin/env bash
set -eux

# Used to copy LLVM and LLD into artiq/compiler while building wheels. This
# means that Linux wheels will automatically have these dependencies built in,
# rather than users having to install them themselves.

# This is intended for Debian Stretch.

apt update
apt install -y apt-transport-https wget

echo "deb http://apt.llvm.org/stretch/ llvm-toolchain-stretch-11 main" \
    >> /etc/apt/sources.list
echo "deb-src http://apt.llvm.org/stretch/ llvm-toolchain-stretch-11 main" \
    >> /etc/apt/sources.list
wget -O - https://apt.llvm.org/llvm-snapshot.gpg.key | apt-key add -
apt update
apt install -y llvm-11 lld-11

function copy_bin {
    local name="$1"
    cp /usr/lib/llvm-11/bin/"$name" artiq/compiler/"$name"
}

copy_bin llvm-strip
copy_bin llvm-addr2line
copy_bin llvm-cxxfilt

cp /usr/bin/ld.lld-11 artiq/compiler/ld.lld
