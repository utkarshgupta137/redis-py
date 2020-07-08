#!/usr/bin/env bash

source /build_tools/redis_vars.sh

pushd /tmp

# create a clean directory for redis
rm -rf $REDIS_DIR
mkdir -p $REDIS_BIN_DIR
mkdir -p $REDIS_CONF_DIR
mkdir -p $REDIS_SAVE_DIR

# download, unpack and build redis
mkdir -p $REDIS_DOWNLOAD_DIR
cd $REDIS_DOWNLOAD_DIR
rm -f $REDIS_PACKAGE
rm -rf $REDIS_BUILD_DIR
wget http://download.redis.io/releases/$REDIS_PACKAGE
tar zxvf $REDIS_PACKAGE
cd $REDIS_BUILD_DIR
make
cp src/redis-server $REDIS_DIR/bin
cp src/redis-cli $REDIS_DIR/bin
cp src/redis-sentinel $REDIS_DIR/bin

popd
