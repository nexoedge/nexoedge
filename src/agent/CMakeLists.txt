include ( ${PROJECT_SOURCE_DIR}/cmake/InstallFunc.cmake )

########################
## Storage containers ##
########################

set( container_deps
        aliyun-oss-sdk
        aws-sdk
        azure-storage-sdk
)
set( container_libs
        aws-cpp-sdk-core
        aws-cpp-sdk-s3
        aws-c-event-stream
        aws-c-common
        aws-checksums
        azurestorage
        oss_c_sdk_static
        cpprest
        boost_system
        OpenSSL::SSL
        OpenSSL::Crypto 
        apr-1
        aprutil-1
        mxml
        curl
        boost_thread
)
set( container_compile_flags -Wno-reorder -Wno-unknown-pragmas )

file( GLOB_RECURSE container_source container/*.cc )

add_library( ncloud_container STATIC EXCLUDE_FROM_ALL ${container_source} )
add_dependencies( ncloud_container ${container_deps} )
target_compile_options( ncloud_container PUBLIC ${container_compile_flags} )
target_link_libraries( ncloud_container PUBLIC glog ${container_libs} )


###########
## Agent ##
###########

file( GLOB agent_source *.cc )
list( REMOVE_ITEM agent_source agent_main.cc )

add_library( ncloud_agent STATIC EXCLUDE_FROM_ALL ${agent_source} )
add_dependencies( ncloud_agent zero-mq google-log )
target_link_libraries( ncloud_agent ncloud_code ncloud_common ncloud_container zmq )


################
## Agent Main ##
################

if ( BUILD_AGENT )
    add_executable( agent agent_main.cc )
else ( BUILD_AGENT )
    add_executable( agent EXCLUDE_FROM_ALL agent_main.cc )
endif ( BUILD_AGENT )
add_dependencies( agent ${ALLDEPS} google-log zero-mq )
target_link_libraries( agent ncloud_agent glog zmq )


##################
## Installation ##
##################

function ( ncloud_build_agent component )
    add_ncloud_install_target( agent ${component} )
    add_ncloud_sample_config( ${component} )
    add_ncloud_install_service( ${PROJECT_SOURCE_DIR}/scripts/package/ncloud-agent.service ${component} )
    add_ncloud_install_libs( 
        "lib(zmq|isal|glog|aws|azurestorage|oss_c_sdk|cppnetlib|cpprest|mxml)+.*"
        ${component} 
    )
endfunction ( ncloud_build_agent )

set( agent_package_depends_list
    " libxml2 (>= 2.9.13) "
    " libaprutil1 (>= 1.6.1)"
    " libapr1 (>= 1.7.0) "
    " libcurl4 (>= 7.81.0) "
    " libglib2.0-0 (>= 2.72.4) "
)

list(
    APPEND
    agent_package_depends_list
    ${common_package_depends_list}
)
string( REPLACE ";" "," agent_package_depends "${agent_package_depends_list}" )

if ( BUILD_AGENT )
    ncloud_build_agent( agent )
    set( CPACK_DEBIAN_AGENT_PACKAGE_DEPENDS ${agent_package_depends} PARENT_SCOPE )
    set( CPACK_DEBIAN_AGENT_PACKAGE_CONTROL_EXTRA ${package_extra_controls} PARENT_SCOPE )
endif ( BUILD_AGENT )

# for full package
set( agent_package_depends_list ${agent_package_depends_list} PARENT_SCOPE )
