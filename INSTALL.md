# Build and Installation

## Nexoedge Components

### Software Pre-requisites

The recommended platforms are [Ubuntu 22.04 LTS][ubuntu2204] 64-bit Server, with the following software installed.

- General
  - CMake, version 3.13 or above
  - `g++`, version 11.3
  - Boost libraries (`libboost-filesystem-dev`, `libboost-system-dev`, `libboost-timer-dev`), version 1.74.0
  - libevent, version 2.1.12
  - OpenSSL (`libssl-dev`), version 3.0.2
  - Glib-2.0, version 2.72.4
  - nlohmann json (`nlohmann-json3-dev`), version 3.10.5
  - libsodium (`libsodium-dev`), version 1.0.16
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
  - Boost libraries (`libboost-log-dev`, `libboost-random-dev`, `libboost-locale-dev`, `libboost-regex-dev`), version 1.74.0, for Azure
  - `libxml2-dev`, version 2.9.13, for Azure
  - `uuid-dev`, version 2.37.2, for Azure
  - Curl (`libcurl-ocaml-dev`), version 0.9.2, for AWS
  - Unzip (`unzip`) for AWS

To install all the software on the platform,

```bash
sudo apt update
sudo apt install -y cmake g++ libssl-dev libboost-filesystem-dev libboost-system-dev libboost-timer-dev libboost-log-dev libboost-random-dev libboost-locale-dev libboost-regex-dev autoconf libtool nasm pkg-config libevent-dev uuid-dev redis-server redis-tools libxml2-dev libcpprest-dev libaprutil1-dev libapr1-dev libglib2.0-dev libjson-c-dev unzip curl nlohmann-json3-dev libcurl-ocaml-dev libsodium-dev
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

3. Setup the build environment using CMake.
   
   - For development:

     ```bash
     cmake ..
     ```

   - For package build and release:

     ```bash
     cmake -DCMAKE_BUILD_TYPE=Release ..
     ```

### Build

The key entities `agent`, `proxy`, and `ncloud-reporter` are built by default under the `bin` folder, as well as the utility `ncloud-curve-keypair-generator`

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

Nexoedge uses the packaging tool in `CMake` (`CPack`) to pack all necessary libraries and binaries into an installer to avoid compiling source code from scratch on every server to deploy. Note that this packaging is platform-dependent, i.e., packages can only be installed on the same OS platform where they were built. Currently, the supported platform is Ubuntu 22.04 64-bit servers in form of Debain packages (`.deb`).

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

* `nexoedge-<version>-amd64-proxy.deb`: `proxy` and `ncloud-reporter`

* `nexoedge-<version>-amd64-agent.deb`: `agent`

* `nexoedge-<version>-amd64-utils.deb`: `ncloud-reporter`

* `nexoedge-<version>-amd64-full.deb`: all the entities built 

Remarks: Binaries compiled under the build type "Release" with `-DNDEBUG` flag defined and `-g` flag removed, and hence are smaller and generate less log messages. It is recommended to use `-DCMAKE_BUILD_TYPE=Debug` (the default mode) for development and debugging purpose.

### Package Installation

To install the packages,

1. Update the list of packages on the system.
   
   ```bash
   sudo apt update
   ```

2. Install a package, e.g., `nexoedge-1.0-amd64-proxy.deb`.
   
   ```bash
   sudo dpkg -i nexoedge-1.0-amd64-proxy.deb
   sudo apt install -f -y
   ```

### Secure Proxy-Agent Network Communication

Nexoedge leverages the CURVE mechanism in ZeroMQ to provide secure communication between agents and proxies. Following are the steps to enable secure communication.

1. Generate a key pair for proxies and another key pair for agents using the provided tool `ncloud-curve-keypair-generator`
   - For package installation,
     ```bash
     ncloud-curve-keypair-generator 
     ```
   - For source code compilation,
     ```bash
     bin/ncloud-curve-keypair-generator 
     ```

1. Copy the key files to the working directories of agents and proxies (where the binaries are run from, e.g., `/usr/lib/ncloud/current` for an installed package)
   - For agents: (i) the agent public key (`agent_pkey`), (ii) the agent secret key (`agent_skey`), and (iii) the proxy public key (`proxy_pkey`)
   - For proxies: (i) the proxy public key (`proxy_pkey`), (ii) the proxy secret key (`proxy_skey`), and (iii) the agent public key (`agent_pkey`)

1. Update the general configuration as follows
   - For both agents and proxies,
     - Set `[network] > use_curve` to 1
   - For agents,
     - Set `[network] > agent_curve_public_key` to `agent_pkey`
     - Set `[network] > agent_curve_secret_key` to `agent_skey`
     - Set `[network] > proxy_curve_public_key` to `proxy_pkey`
   - For proxies,
     - Set `[network] > proxy_curve_public_key` to `proxy_pkey`
     - Set `[network] > proxy_curve_secret_key` to `proxy_skey`
     - Set `[network] > agent_curve_public_key` to `agent_pkey`


## Samba Installation

- Download the package from the release section.

- Extract the package
  
  ```bash
  tar zxf nexoedge-cifs.tar.gz
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


## Docker Build

See [the guide](./docker/Readme.rst) for details.


[ubuntu2204]: http://releases.ubuntu.com/22.04/

[compile-aws-prob-1-code]: https://github.com/aws/aws-sdk-cpp/blob/master/aws-cpp-sdk-core-tests/utils/FileSystemUtilsTest.cpp#L271

[package-installation]: docs/release-doc/source/install.rst
