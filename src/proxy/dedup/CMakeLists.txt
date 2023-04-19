cmake_minimum_required( VERSION 3.5 )

project( nCloud_Dedup )

set( CMAKE_CXX_STANDARD 11 )

find_package( OpenSSL REQUIRED )

# deduplication module
include_directories( include )
file( GLOB ncloud_dedup_src metastore/*.cc fingerprint/*.cc chunking/*.cc impl/*.cc )
#add_library( ncloud_dedup SHARED EXCLUDE_FROM_ALL ${ncloud_dedup_src} )
add_library( ncloud_dedup EXCLUDE_FROM_ALL ${ncloud_dedup_src} )
target_link_libraries( ncloud_dedup leveldb OpenSSL::Crypto glog )

# test program
add_executable( ncloud_dedup_test dedup_test.cc )
target_link_libraries( ncloud_dedup_test ncloud_dedup )
