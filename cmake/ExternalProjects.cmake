## ISA-L
ExternalProject_Add (
    "isa-l" 
    PREFIX "third-party/isa-l"
    EXCLUDE_FROM_ALL true
    URL ${PROJECT_THIRD_PARTY_LIB_DIR}/isa-l-2.31.1.tar.gz
    URL_MD5 851eb1b98da53c4dc8b94e0119106f03
    UPDATE_COMMAND ""
    BUILD_IN_SOURCE 1
    CONFIGURE_COMMAND ./autogen.sh COMMAND ./configure ${THIRD_PARTY_CONFIG} --enable-shared=false --enable-static=true
    BUILD_COMMAND make
    INSTALL_DIR ""
    INSTALL_COMMAND make install
)

## zeromq 
ExternalProject_Add (
    "zero-mq"
    PREFIX "third-party/zeromq"
    EXCLUDE_FROM_ALL true
    URL ${PROJECT_THIRD_PARTY_LIB_DIR}/zeromq-4.3.5.tar.gz
    URL_MD5 cfb80c6ad694cacc2f7c99bf44e8b686
    UPDATE_COMMAND ""
    CMAKE_ARGS
        -DCMAKE_INSTALL_PREFIX=${PROJECT_BINARY_DIR}
        -DBUILD_SHARED=ON
        -DBUILD_STATIC=ON
        -DBUILD_TESTS=OFF
        -DCMAKE_BUILD_TYPE=Release
        -DENABLE_CURVE=ON
        -DWITH_LIBSODIUM=ON
    INSTALL_DIR ""
    INSTALL_COMMAND make install
)

## glog
ExternalProject_Add (
    "google-log"
    PREFIX "third-party/glog"
    EXCLUDE_FROM_ALL true
    URL ${PROJECT_THIRD_PARTY_LIB_DIR}/glog-0.7.1.tar.gz
    URL_MD5 128e2995cc33d794ff24f785a3060346
    UPDATE_COMMAND ""
    CMAKE_ARGS
        -DCMAKE_BUILD_TYPE=Release
        -DCMAKE_INSTALL_PREFIX=${PROJECT_BINARY_DIR}
        -DWITH_GFLAGS=OFF
        -DGLOG_EXPORT=ON
)

## hiredis
ExternalProject_Add (
    "hiredis-cli" 
    PREFIX "third-party/hiredis"
    EXCLUDE_FROM_ALL true
    URL ${PROJECT_THIRD_PARTY_LIB_DIR}/hiredis-1.2.0.tar.gz
    URL_MD5 119767d178cfa79718a80c83e0d0e849 
    UPDATE_COMMAND ""
    BUILD_IN_SOURCE 1
    CONFIGURE_COMMAND ""
    BUILD_COMMAND make USE_SSL=1 libhiredis.a libhiredis_ssl.a
    INSTALL_DIR ""
    INSTALL_COMMAND 
        mkdir -p ${THIRD_PARTY_INCLUDE_CONFIG}/hiredis/adapters ${THIRD_PARTY_LIB_CONFIG} 
        COMMAND cp ${PROJECT_BINARY_DIR}/third-party/hiredis/src/hiredis-cli/hiredis.h 
            ${PROJECT_BINARY_DIR}/third-party/hiredis/src/hiredis-cli/hiredis_ssl.h
            ${PROJECT_BINARY_DIR}/third-party/hiredis/src/hiredis-cli/read.h
            ${PROJECT_BINARY_DIR}/third-party/hiredis/src/hiredis-cli/sds.h
            ${PROJECT_BINARY_DIR}/third-party/hiredis/src/hiredis-cli/async.h
            ${PROJECT_BINARY_DIR}/third-party/hiredis/src/hiredis-cli/alloc.h
            ${THIRD_PARTY_INCLUDE_CONFIG}/hiredis
        COMMAND cp ${PROJECT_BINARY_DIR}/third-party/hiredis/src/hiredis-cli/adapters/libevent.h
            ${THIRD_PARTY_INCLUDE_CONFIG}/hiredis/adapters
        COMMAND cp ${PROJECT_BINARY_DIR}/third-party/hiredis/src/hiredis-cli/libhiredis.a
            ${PROJECT_BINARY_DIR}/third-party/hiredis/src/hiredis-cli/libhiredis_ssl.a
            ${THIRD_PARTY_LIB_CONFIG}/
)

## cpp-netlib
ExternalProject_Add (
    "cpp-netlib"
    PREFIX "third-party/cpp-netlib"
    EXCLUDE_FROM_ALL true
    URL ${PROJECT_THIRD_PARTY_LIB_DIR}/cpp-netlib-0.13.0.tar.gz
    URL_MD5 002b0922bc7028d585c4975db748399d
    PATCH_COMMAND patch -p1 < ${PROJECT_THIRD_PARTY_LIB_DIR}/patch/cpp-netlib-0.13.0.patch
    CMAKE_ARGS -DCMAKE_INSTALL_PREFIX=${PROJECT_BINARY_DIR}
        -DCMAKE_CXX_STANDARD=17
        -DCMAKE_CXX_FLAGS=-I${THIRD_PARTY_INCLUDE_CONFIG}
        -DCMAKE_PREFIX_PATH=${PROJECT_BINARY_DIR}
        -DCMAKE_BUILD_TYPE=Release
)

## aliyun object storage sdk
ExternalProject_Add (
    "mxml-lib"
    PREFIX "third-party/mxml"
    EXCLUDE_FROM_ALL true
    URL ${PROJECT_THIRD_PARTY_LIB_DIR}/mxml-2.8.tar.gz
    URL_MD5 ab6442726a3ad646e6b58682df041cdb
    BUILD_IN_SOURCE 1
    UPDATE_COMMAND ""
    CONFIGURE_COMMAND ./configure --prefix=${PROJECT_BINARY_DIR}
    BUILD_COMMAND make
    INSTALL_COMMAND make install
)

ExternalProject_Add (
    "aliyun-oss-sdk"
    PREFIX "third-party/aliyun-oss-sdk"
    DEPENDS mxml-lib cpp-netlib
    EXCLUDE_FROM_ALL true
    PATCH_COMMAND patch -p0 < ${PROJECT_THIRD_PARTY_LIB_DIR}/patch/aliyun-oss-c-sdk-3.10.0.patch
    URL ${PROJECT_THIRD_PARTY_LIB_DIR}/aliyun-oss-c-sdk-3.10.0.tar.gz
    URL_MD5 aeba4ccef9b1c092aca3c87746492b65
    UPDATE_COMMAND ""
    CMAKE_ARGS 
        -DCMAKE_INSTALL_PREFIX=${PROJECT_BINARY_DIR}
        -DCMAKE_BUILD_TYPE=Release
)

## aws core and s3 sdk
ExternalProject_Add (
    "aws-sdk"
    PREFIX "third-party/aws-sdk"
    EXCLUDE_FROM_ALL true
    URL ${PROJECT_THIRD_PARTY_LIB_DIR}/aws-sdk-cpp-1.11.5.tar.gz
    URL_MD5 53a36820c5ef8891e79cc0612c7934e8
    PATCH_COMMAND "./prefetch_crt_dependency.sh"
    CMAKE_ARGS 
        -DCMAKE_INSTALL_PREFIX=${PROJECT_BINARY_DIR}
        -DENABLE_TESTING=OFF
        -DBUILD_ONLY=s3
        -DCMAKE_BUILD_TYPE=Release
)

## azure storage sdk
ExternalProject_Add (
    "cpprest-sdk"
    PREFIX "third-party/cpprest-sdk"
    EXCLUDE_FROM_ALL true
    URL ${PROJECT_THIRD_PARTY_LIB_DIR}/cpprest-sdk-2.10.18.tar.gz
    URL_MD5 c4cd1d36aa3156026bd396a21a4fdb74
    PATCH_COMMAND patch -p0 < ${PROJECT_THIRD_PARTY_LIB_DIR}/patch/cpprestsdk-2.10.18.patch
    CMAKE_ARGS
        -DCMAKE_INSTALL_PREFIX=${PROJECT_BINARY_DIR}
        -DCMAKE_BUILD_TYPE=Release
	-DBUILD_TESTS=OFF
	-DBUILD_SAMPLES=OFF
)

ExternalProject_Add (
    "azure-storage-sdk"
    DEPENDS cpprest-sdk
    PREFIX "third-party/azure-sdk"
    EXCLUDE_FROM_ALL true
    URL ${PROJECT_THIRD_PARTY_LIB_DIR}/azure-storage-cpp-7.5.0.tar.gz
    URL_MD5 fed50779b589f2da29d62c533a29c474
    BINARY_DIR "third-party/azure-sdk/src/azure-storage-sdk/Microsoft.WindowsAzure.Storage"
    PATCH_COMMAND patch -p0 < ${PROJECT_THIRD_PARTY_LIB_DIR}/patch/azure-storage-cpp-sdk-7.5.0.patch
    CONFIGURE_COMMAND 
        cmake 
        -DCMAKE_INSTALL_PREFIX=${PROJECT_BINARY_DIR}
        -DCASABLANCA_DIR=${PROJECT_BINARY_DIR}
        -DCMAKE_BUILD_TYPE=Release
        -DBUILD_SHARED_LIBS=ON
	-DBUILD_TESTS=OFF
	-DBUILD_SAMPLES=OFF
        .
)

# for aliyun sdk
include_directories ( /usr/include/apr-1.0 ) # ubuntu
include_directories ( /usr/include/apr-1 ) # fedora

