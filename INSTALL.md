# Build and Installation

## nCloud Components

### Software Pre-requisites

The recommended platforms are [Ubuntu 22.04 LTS][ubuntu2204] 64-bit Server, with the following software installed.

- General
  - CMake, version 3.13 or above
  - `g++`, version 11.3
  - Boost libraries (`libboost-filesystem-dev`, `libboost-system-dev`, `libboost-timer-dev`), version 1.74.0
  - libevent, version 2.1.12
  - OpenSSL (`libssl-dev`), version 3.0.2
  - Glib-2.0, version 2.72.4
  - nlohmann json (`libnlohmann-json3-dev`), version 3.10.5
- Coding-related
  - Netwide Assembler (`nasm`), v2.11.01 or above, for [Intel(R) Intelligent Storage Acceleration Library](https://github.com/01org/isa-l/blob/master/README.md)
  - `autoconf`
  - `libtool`
  - `pkg-config`
- Metadata-related
  - `redis-server`: Redis server version 6.1.0
- Report-related
  - `libjson-c-dev`
- Container-related
  - APR (`libapr1-dev`), version 1.7.0, for Aliyun
  - APR-Util (`libaprutil1-dev`), version 1.6.1, for Aliyun
  - Cpprest (`libcpprest-dev`), version 2.10.18, for Azure
  - Boost libraries (`libboost-log-dev`, `libboost-random-dev`, `libboost-locale-dev`, `libboost-regex-dev`), version 1.74.0, for Azure
  - `libxml2-dev`, version 2.9.13, for Azure
  - `uuid-dev`, version 2.37.2, for Azure
  - Curl (`libcurl-ocaml-dev`), version 0.9.2, for AWS
  - Unzip (`unzip`) for AWS

To install all the software on the platform,

```bash
sudo apt update
sudo apt install -y cmake g++ libssl-dev libboost-filesystem-dev libboost-system-dev libboost-timer-dev libboost-log-dev libboost-random-dev libboost-locale-dev libboost-regex-dev autoconf libtool nasm pkg-config libevent-dev uuid-dev redis-server redis-tools libxml2-dev libcpprest-dev libaprutil1-dev libapr1-dev libglib2.0-dev libjson-c-dev unzip curl nlohmann-json3-dev libcurl-ocaml-dev
```

### Configure Build Environment

Prepare the build environment,

1. Create a build directory
   
   ```bash
   mkdir build
   ```

2. Move into the build directory
   
   ```bash
   cd build
   ```

3. Setup the build environment using CMake
   
   - For development:

     ```bash
     cmake ..
     ```

   - For package build and release:

     ```bash
     cmake -DCMAKE_BUILD_TYPE=Release ..
     ```

### Build

The key entities `agent`, `proxy`, `ncloud-reporter` are built by default in the `bin` folder,

Build all entities 

```bash
make
```

Optionally, to build only one of the entities, e.g., `agent`

```bash
make agent
```

## Metadata Store Setup (Redis)

Start Redis Service

```bash
sudo service redis-server start
```

## Platform-dependent Package Build

nCloud uses the packaging tool in `CMake` (`CPack`) to pack all necessary libraries and binaries into an installer to avoid compiling source code from scratch on every server to deploy. Note that this packaging is platform-dependent, i.e., packages can only be installed on the same OS platform where they were built. Currently, the supported platform is Ubuntu 22.04 64-bit servers in form of Debain packages (`.deb`).

### Build packages

To build the packages, 

1. Switch the build type to "Release".
   
   ```bash
   cmake -DCMAKE_BUILD_TYPE=Release ..
   ```

2. Build the packages.
   
   ```bash
   make package
   ```

The following packages are built:

* `ncloud-<version>-amd64-proxy.deb`: `proxy` and `ncloud-reporter`

* `ncloud-<version>-amd64-agent.deb`: `agent`

* `ncloud-<version>-amd64-utils.deb`: `ncloud-reporter`

* `ncloud-<version>-amd64-full.deb`: all the entities built 

Remarks: Binaries compiled under the build type "Release" with `-DNDEBUG` flag defined and `-g` flag removed, and hence are smaller and generate less log messages. It is recommended to use `-DCMAKE_BUILD_TYPE=Debug` (the default mode) for development and debugging purpose.

### Package Installation

To install the packages,

1. Update the list of packages on the system.
   
   ```bash
   sudo apt update
   ```

2. Install a package, e.g., `ncloud-1.0-amd64-proxy.deb`.
   
   ```bash
   sudo dpkg -i ncloud-1.0-amd64-proxy.deb
   sudo apt install -f -y
   ```

## Samba Installation

- Download the package from the release section.

- Extract the package
  
  ```bash
  tar zxf ncloud-cifs.tar.gz
  ```

- Move the folder `samba/` under `/usr/local/`
  
  ```bash
  sudo mv samba /usr/local/
  ```

- Create the SMB share folder
  
  ```bash
  sudo mkdir -p /smb/ncloud && sudo chmod 777 /smb/ncloud
  ```

- Run `install.sh` under the folder `scripts`
  
  ```bash
  cd scripts && sudo bash install.sh
  ```

- Start the SMB service
  
  ```bash
  sudo service ncloud-cifs start
  ```

- Add a Samba user, e.g., `ncloud`, with password, e.g., `ncloud`. Note the user must already exist in the system.
  
  ```bash
  sudo /usr/local/samba/bin/pdbedit -a ncloud
  ```
  
  Enter the password twice

[ubuntu2204]: http://releases.ubuntu.com/22.04/

[compile-aws-prob-1-code]: https://github.com/aws/aws-sdk-cpp/blob/master/aws-cpp-sdk-core-tests/utils/FileSystemUtilsTest.cpp#L271

[package-installation]: docs/release-doc/source/install.rst
