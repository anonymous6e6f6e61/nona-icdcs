#!/bin/bash
# execute in ROOT/scripts

cd ..

# download dependencies for kcat
sudo apt-get update
sudo apt-get install -y gcc g++ make librdkafka-dev pkg-config libzstd-dev zlib1g-dev unzip wget libssl-dev cmake libcurl4-openssl-dev

# download and unzip kcat repo
wget https://github.com/edenhill/kcat/archive/refs/tags/1.7.0.zip -O kcat.zip
unzip kcat.zip

# replace bootstrap.sh with custom file
cp scripts/bootstrap_kcat.sh kcat-1.7.0/bootstrap.sh

# install kcat
cd kcat-1.7.0
bash bootstrap.sh
sudo make install