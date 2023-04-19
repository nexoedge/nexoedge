#####################
## Proxy Metastore ##
#####################

file( GLOB metastore_source metastore/*.cc )
add_library( ncloud_metastore STATIC EXCLUDE_FROM_ALL ${metastore_source} )
add_dependencies( ncloud_metastore hiredis-cli )
target_link_libraries( ncloud_metastore ncloud_common hiredis event )

###################
## Proxy Staging ##
###################

file( GLOB_RECURSE staging_source staging/*.cc )
add_library( ncloud_staging STATIC EXCLUDE_FROM_ALL ${staging_source} )
target_link_libraries( ncloud_staging ncloud_common )

#########################
## Proxy Deduplication ##
#########################

# deduplication module
file( GLOB ncloud_dedup_src dedup/metastore/*.cc dedup/fingerprint/*.cc dedup/chunking/*.cc dedup/impl/*.cc )
add_library( ncloud_dedup STATIC EXCLUDE_FROM_ALL ${ncloud_dedup_src} )
add_dependencies( ncloud_dedup google-log )
target_link_libraries( ncloud_dedup OpenSSL::Crypto glog )

###########
## Proxy ##
###########

file( GLOB proxy_source *.cc interfaces/*.cc )
add_library( ncloud_proxy STATIC EXCLUDE_FROM_ALL ${proxy_source} )
add_dependencies( ncloud_proxy zero-mq hiredis-cli )
target_link_libraries( ncloud_proxy ncloud_code ncloud_common ncloud_metastore ncloud_dedup ncloud_staging zmq event OpenSSL::SSL )

################
## Proxy main ##
################

include ( ${PROJECT_SOURCE_DIR}/cmake/InstallFunc.cmake )

if ( BUILD_PROXY )
    add_executable( proxy proxy_main.cc )
else ()
    add_executable( proxy EXCLUDE_FROM_ALL proxy_main.cc )
endif ( BUILD_PROXY )
add_dependencies( proxy ncloud_proxy )
target_link_libraries( proxy ncloud_common ncloud_proxy )

##################
## Installation ##
##################

function ( ncloud_build_proxy component )
    add_ncloud_install_target( proxy ${component} )
    add_ncloud_install_target( ncloud-reporter ${component} )
    add_ncloud_install_target( ncloud_zmq_client ${component} )
    add_ncloud_sample_config( ${component} )
    add_ncloud_install_service( ${PROJECT_SOURCE_DIR}/scripts/package/ncloud-proxy.service ${component} )
    add_ncloud_install_libs( "lib(zmq|hiredis|isal|glog)" ${component} )
endfunction ( ncloud_build_proxy )

set( proxy_package_depends_list
    " libssl3 (>= 3.0.2) "
    " uuid-runtime (>= 2.37.2) "
    " redis-server (>= 5:6.0.16) "
    " libevent-2.1-7 (>= 2.1.12) "
    " libcurl4 (>= 7.81.0) "
)

list( 
    APPEND
    proxy_package_depends_list
    ${utils_package_depends_list}
    ${common_package_depends_list}
)
list( REMOVE_DUPLICATES proxy_package_depends_list )
string( REPLACE ";" "," proxy_package_depends "${proxy_package_depends_list}" )

if ( BUILD_PROXY )
    ncloud_build_proxy( proxy )
    set( CPACK_DEBIAN_PROXY_PACKAGE_DEPENDS ${proxy_package_depends} PARENT_SCOPE )
    set( CPACK_DEBIAN_PROXY_PACKAGE_CONTROL_EXTRA ${package_extra_controls} PARENT_SCOPE )
endif ( BUILD_PROXY )

# for compiling full package
set( proxy_package_depends_list ${proxy_package_depends_list} PARENT_SCOPE )
