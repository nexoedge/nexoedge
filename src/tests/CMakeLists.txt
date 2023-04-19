##################
# Coding schemes #
##################

add_executable( coding_test EXCLUDE_FROM_ALL common/coding_test.cc )
target_link_libraries( coding_test ncloud_code ncloud_config )

################
# Coordinators #
################

file( GLOB_RECURSE coordinator_source ${PROJECT_SOURCE_DIR}/src/*/coordinator.cc ${PROJECT_SOURCE_DIR}/src/agent/container_manager.cc )
add_executable( coordinator_test EXCLUDE_FROM_ALL common/coordinator_test.cc ${coordinator_source} )
add_dependencies( coordinator_test zero-mq google-log )
target_link_libraries( coordinator_test ncloud_code ncloud_common ncloud_container glog zmq )

######################
# Storage containers #
######################

add_executable( container_test EXCLUDE_FROM_ALL agent/container_test.cc )
target_link_libraries( container_test ncloud_container ncloud_config )

#########
# Agent #
#########

add_executable( agent_test EXCLUDE_FROM_ALL agent/agent_test.cc )
target_link_libraries( agent_test ncloud_code ncloud_common ncloud_container ncloud_agent )

##############
# ZMQ Client #
##############

add_executable( zmq_client_test EXCLUDE_FROM_ALL client/zmq_client_test.c )
target_link_libraries( zmq_client_test ncloud_zmq_client ${GLIB2_LIBRARIES} )

#############
# Metastore #
#############
add_executable( metastore_test EXCLUDE_FROM_ALL proxy/metastore_test.cc )
add_dependencies( metastore_test google-log )
target_link_libraries( metastore_test ncloud_metastore glog )


#######################
# Collection of tests #
#######################
set ( ncloud_unit_tests coding_test container_test coordinator_test agent_test zmq_client_test )
add_custom_target( tests )
add_dependencies( tests ${ncloud_unit_tests} )

