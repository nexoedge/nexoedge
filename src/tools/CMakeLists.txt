##################
## ZMQ reporter ##
##################

if ( BUILD_UTILS OR BUILD_PROXY )
    add_executable( ncloud-reporter zmq_reporter.c )
else ( BUILD_UTILS OR BUILD_PROXY )
    add_executable( ncloud-reporter EXCLUDE_FROM_ALL zmq_reporter.c )
endif ( BUILD_UTILS OR BUILD_PROXY )
add_dependencies( ncloud-reporter hiredis-cli )
target_link_libraries( ncloud-reporter ncloud_zmq_client ${GLIB2_LIBRARIES} hiredis json-c )


##################
## Installation ##
##################

include ( ${PROJECT_SOURCE_DIR}/cmake/InstallFunc.cmake )

function ( ncloud_build_utils component )
    add_ncloud_install_target( ncloud-reporter ${component} )
    add_ncloud_install_target( ncloud_zmq_client ${component} )
    add_ncloud_sample_config( ${component} )
    add_ncloud_install_libs( "lib(zmq|hiredis|glog)" ${component} )
endfunction ( ncloud_build_utils )

set( utils_package_depends_list
    " libjson-c5 (>= 0.15) "
    " libglib2.0-0 (>= 2.72.4) "
)

list(
    APPEND
    utils_package_depends_list
    ${common_package_depends_list}
)

string( REPLACE ";" "," utils_package_depends "${utils_package_depends_list}" )

if ( BUILD_UTILS )
    ncloud_build_utils ( utils )
    set( CPACK_DEBIAN_UTILS_PACKAGE_DEPENDS ${utils_package_depends} PARENT_SCOPE )
    set( CPACK_DEBIAN_UTILS_PACKAGE_CONTROL_EXTRA ${package_extra_controls} PARENT_SCOPE )
endif ( BUILD_UTILS )

# for full package
set( utils_package_depends_list ${utils_package_depends_list} PARENT_SCOPE )
