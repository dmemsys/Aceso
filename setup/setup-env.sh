#!/bin/bash

mode="$1"
ubuntu_version=$(lsb_release -r -s)

if [ $ubuntu_version == "18.04" ]; then
  wget https://content.mellanox.com/ofed/MLNX_OFED-4.9-5.1.0.0/MLNX_OFED_LINUX-4.9-5.1.0.0-ubuntu18.04-x86_64.tgz
  mv MLNX_OFED_LINUX-4.9-5.1.0.0-ubuntu18.04-x86_64.tgz ofed.tgz
elif [ $ubuntu_version == "20.04" ]; then
  wget https://content.mellanox.com/ofed/MLNX_OFED-4.9-5.1.0.0/MLNX_OFED_LINUX-4.9-5.1.0.0-ubuntu20.04-x86_64.tgz
  mv MLNX_OFED_LINUX-4.9-5.1.0.0-ubuntu20.04-x86_64.tgz ofed.tgz
else
  echo "Wrong ubuntu distribution for $mode!"
  exit 0
fi
echo $mode $ubuntu_version $ofed_fid

sudo apt update -y

# install anaconda
mkdir install
mv ofed.tgz install

cd install
if [ ! -f "./anaconda-install.sh" ]; then
  wget https://repo.anaconda.com/archive/Anaconda3-2022.05-Linux-x86_64.sh -O anaconda-install.sh
fi
if [ ! -d "$HOME/anaconda3" ]; then
  chmod +x anaconda-install.sh
  ./anaconda-install.sh -b
  export PATH=$PATH:$HOME/anaconda3/bin
  # add conda to path
  echo PATH=$PATH:$HOME/anaconda3/bin >> $HOME/.bashrc
  conda init
  source ~/.bashrc
  # activate base
fi
conda activate base
cd ..

pip install gdown
sudo apt install memcached -y
sudo apt-get install libmemcached-dev -y
sudo apt-get install libpapi-dev -y
sudo apt-get install hugepages -y
sudo apt install libtbb-dev libboost-all-dev -y
sudo apt install nasm -y

# install ofed
cd install
if [ ! -d "./ofed" ]; then
  tar zxf ofed.tgz
  mv MLNX* ofed
fi
cd ofed
sudo ./mlnxofedinstall --force
if [ $mode == "scalestore" ]; then
  sudo /etc/init.d/openibd restart
fi
cd ..

# install isa-l
git clone https://github.com/intel/isa-l.git
cd isa-l
./autogen.sh
./configure --prefix=/usr --libdir=/usr/lib
make
sudo make install
cd ..

# install cmake
mkdir -p cmake
cd cmake
if [ ! -f cmake-3.16.8.tar.gz ]; then
  wget https://cmake.org/files/v3.16/cmake-3.16.8.tar.gz
fi
if [ ! -d "./cmake-3.16.8" ]; then
  tar zxf cmake-3.16.8.tar.gz
  cd cmake-3.16.8 && sudo ./configure && sudo make clean && make -j 4 && sudo make install
fi
cd ..

# install cityhash
git clone https://github.com/google/cityhash.git
cd cityhash
sudo ./configure
sudo make all check CXXFLAGS="-g -O3"
sudo make install
cd ..

# install memcached
sudo apt-get -y --force-yes install memcached libmemcached-dev

# install boost
wget https://jaist.dl.sourceforge.net/project/boost/boost/1.53.0/boost_1_53_0.zip
unzip boost_1_53_0.zip
cd boost_1_53_0
./bootstrap.sh
./b2 install --with-system --with-coroutine --layout=versioned threading=multi
sudo apt-get -y --force-yes install libboost-all-dev
cd ..

# install lz4
git clone https://github.com/lz4/lz4.git
cd lz4
make
sudo make install
cd ..

# install tbb
git clone https://github.com/wjakob/tbb.git
cd tbb/build
cmake ..
make -j
sudo make install
ldconfig
cd ../..

# install openjdk-8
sudo apt-get -y --force-yes install openjdk-8-jdk

# install gtest
if [ ! -d "/usr/src/gtest" ]; then
  sudo apt install -y libgtest-dev
fi
cd /usr/src/gtest
sudo cmake .
sudo make
sudo make install

sudo su
chsh -s /bin/bash