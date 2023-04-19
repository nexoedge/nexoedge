####################
## Coding schemes ##
####################

file( GLOB_RECURSE ncloud_coding_source coding/*.cc )
add_library( ncloud_code STATIC EXCLUDE_FROM_ALL ${ncloud_coding_source} )
add_dependencies( ncloud_code google-log isa-l )
target_link_libraries( ncloud_code isal glog OpenSSL::SSL OpenSSL::Crypto )


###################
## Configuration ##
###################

add_library( ncloud_config STATIC config.cc define.cc )
add_dependencies( ncloud_config google-log )
target_link_libraries( ncloud_config glog )


###############
## Benchmark ##
###############

file( GLOB ncloud_benchmark_source benchmark/*.cc )
add_library( ncloud_benchmark STATIC ${ncloud_benchmark_source} )
add_dependencies( ncloud_benchmark google-log )
target_link_libraries( ncloud_benchmark glog )


############
## Common ##
############

file( GLOB ncloud_common_source *.cc ../ds/*.cc )
add_library( ncloud_common STATIC ${ncloud_common_source} )
add_dependencies( ncloud_common zero-mq google-log )
target_link_libraries( ncloud_common ncloud_benchmark ncloud_config ncloud_dedup curl glog )

