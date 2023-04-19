################
## ZMQ client ##
################

file( GLOB client_zmq_source c/zmq_interface.c )

if ( BUILD_CLIENTS OR BUILD_UTILS OR BUILD_PROXY )
    add_library( ncloud_zmq_client SHARED ${client_zmq_source} )
else ()
    add_library( ncloud_zmq_client EXCLUDE_FROM_ALL SHARED ${client_zmq_source} )
endif ( BUILD_CLIENTS OR BUILD_UTILS OR BUILD_PROXY )
add_dependencies( ncloud_zmq_client zero-mq )
target_link_libraries( ncloud_zmq_client zmq )
set_target_properties( ncloud_zmq_client PROPERTIES OUTPUT_NAME ncloud_zmq )

set( zmq_client_main_source c/zmq_client.c )
if ( BUILD_CLIENTS )
    add_executable( zmq_client ${zmq_client_main_source} )
else ()
    add_executable( zmq_client EXCLUDE_FROM_ALL  ${zmq_client_main_source} )
endif ( BUILD_CLIENTS )
target_link_libraries( zmq_client ncloud_zmq_client )


##################
## Installation ##
##################

include ( ${PROJECT_SOURCE_DIR}/cmake/InstallFunc.cmake )

function ( ncloud_build_client component )
    add_ncloud_install_target( ncloud_zmq_client ${component} )
    add_ncloud_sample_config( ${component} )
    add_ncloud_install_libs( "lib(zmq|glog)" ${component} )
endfunction ( ncloud_build_client )

set( clients_package_depends_list
    " libevent-2.1-7 (>= 2.1.12) "
    " libgflags2.2 (>= 2.2.2 ) "
)

list(
    APPEND
    clients_package_depends_list
    ${common_package_depends_list}
)
string( REPLACE ";" "," clients_package_depends "${clients_package_depends_list}" )

if ( BUILD_CLIENTS )
    ncloud_build_client( clients )
    set( CPACK_DEBIAN_CLIENTS_PACKAGE_DEPENDS ${clients_package_depends} PARENT_SCOPE )
    set( CPACK_DEBIAN_CLIENTS_PACKAGE_CONTROL_EXTRA ${package_extra_controls} PARENT_SCOPE )
endif ( BUILD_CLIENTS )

# for full package
set( clients_package_depends_list ${clients_package_depends_list} PARENT_SCOPE )
