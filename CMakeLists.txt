cmake_minimum_required( VERSION 3.13 )

project( ncloud )

#####################
### CMAKE modules ###
#####################

include( ExternalProject )


#####################
### Build Options ###
#####################

# build type (default: debug)
if ( NOT CMAKE_BUILD_TYPE ) 
    set ( CMAKE_BUILD_TYPE debug )
endif()

# build options
option( BUILD_PROXY "Build nCloud Proxy" ON )
option( BUILD_AGENT "Build nCloud Agent" ON )
option( BUILD_CLIENTS "Build nCloud Clients" OFF )
option( BUILD_UTILS "Build nCloud Utils" ON )


#######################
### Compile Options ###
#######################

## Global compile flags
set ( CMAKE_CXX_STANDARD 17 )
set ( CMAKE_CXX_FLAGS "-Wall -Werror" )
set ( CMAKE_CXX_FLAGS_DEBUG "-g -O0" )
set ( CMAKE_CXX_FLAGS_RELEASE "-DNDEBUG -O2" )
## Use address sanitizer
# set ( CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fno-omit-frame-pointer -fsanitize=address" )
# set ( CMAKE_LINKER_FLAGS_DEBUG "${CMAKE_LINKER_FLAGS_DEBUG} -fno-omit-frame-pointer -fsanitize=address" )


############################
### Package Dependencies ###
############################

## Global dependencies, link flags
set ( ALLDEPS google-log zero-mq )
# NOTE! always have isal before fmsr, otherwise, isal will fail to generate matrix
link_libraries( boost_filesystem boost_system boost_timer glog pthread )

# TODO add library checking here
## Required for all
find_package( OpenSSL REQUIRED )
if( OPENSSL_FOUND )
    set( OPENSSL_USE_STATIC_LIBS TRUE )
    message( STATUS "Using OpenSSL ${OPENSSL_VERSION}" )
endif()
find_package( Threads REQUIRED )
find_package( Boost REQUIRED COMPONENTS filesystem system timer )
## Optional (for some components only)
find_package( Boost COMPONENTS thread log random locale regex )
find_package( LibXml2 REQUIRED )
find_package( PkgConfig REQUIRED )
pkg_check_modules( GLIB2 REQUIRED glib-2.0 )
include_directories ( ${GLIB2_INCLUDE_DIRS} )
link_directories ( ${GLIB2_LIBRARY_DIRS} )

# figure out the library and os versions
set( BOOST_VERSION "${Boost_MAJOR_VERSION}.${Boost_MINOR_VERSION}.${Boost_SUBMINOR_VERSION}" )


#################################
### nCloud Output Directories ###
#################################

## Setup for ncloud library and archive output
set ( CMAKE_RUNTIME_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR}/bin )
set ( CMAKE_LIBRARY_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR}/lib )
set ( CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR}/lib )


#############################
### Third-party libraries ###
#############################

## Setup third-party library
set ( PROJECT_THIRD_PARTY_LIB_DIR ${PROJECT_SOURCE_DIR}/third-party )
set ( THIRD_PARTY_INCLUDE_CONFIG ${PROJECT_BINARY_DIR}/include )
set ( THIRD_PARTY_LIB_CONFIG ${CMAKE_LIBRARY_OUTPUT_DIRECTORY} )
set ( THIRD_PARTY_BIN_CONFIG ${PROJECT_BINARY_DIR}/bin/third-party )
set ( THIRD_PARTY_MAN_CONFIG ${PROJECT_BINARY_DIR}/man/third-party )
set ( THIRD_PARTY_CONFIG --includedir=${THIRD_PARTY_INCLUDE_CONFIG} --libdir=${THIRD_PARTY_LIB_CONFIG} --bindir=${THIRD_PARTY_BIN_CONFIG} --mandir=${THIRD_PARTY_MAN_CONFIG} )

link_directories ( ${THIRD_PARTY_LIB_CONFIG} )
include_directories ( ${THIRD_PARTY_INCLUDE_CONFIG} )
include_directories ( ${PROJECT_THIRD_PARTY_LIB_DIR}/include )

# Third party libraries as external projects
include( cmake/ExternalProjects.cmake )


####################
###  COMPONENTS  ###
####################

add_subdirectory( src/common )

add_subdirectory( src/client )
add_subdirectory( src/tools )

add_subdirectory( src/agent )
add_subdirectory( src/proxy )


####################
###  UNIT TESTS  ###
####################

add_subdirectory( src/tests )


####################
###   INSTALL    ###
####################

include ( cmake/InstallFunc.cmake )

list(
    APPEND 
    full_package_depends_list
    ${agent_package_depends_list}
    ${proxy_package_depends_list}
    ${clients_package_depends_list}
    ${utils_package_depends_list}
)

list( REMOVE_DUPLICATES full_package_depends_list )
string( REPLACE ";" "," full_package_depends "${full_package_depends_list}" )

## full
if ( BUILD_AGENT AND BUILD_PROXY AND BUILD_UTILS )
    ncloud_build_proxy( full )
    ncloud_build_agent( full )
    ncloud_build_utils ( full )
    set( CPACK_DEBIAN_FULL_PACKAGE_DEPENDS ${full_package_depends} )
    set( CPACK_DEBIAN_FULL_PACKAGE_CONTROL_EXTRA ${package_extra_controls} )
endif ( BUILD_AGENT AND BUILD_PROXY AND BUILD_UTILS )

## force component-based install
add_ncloud_sample_config( full )


####################
###    PACKAGE   ###
####################

set( CPACK_GENERATOR DEB )
execute_process( COMMAND 
    dpkg --print-architecture
    OUTPUT_VARIABLE CPACK_SYSTEM_NAME
    OUTPUT_STRIP_TRAILING_WHITESPACE   
)
set( CPACK_PACKAGE_NAME "ncloud" )
set( CPACK_PACKAGE_VERSION "1.0" )
set( CPACK_PACKAGE_CONTACT "CUHK ADSLab" )
set( CPACK_PACKAGE_DESCRIPTION "nCloud, store and distribute your data over multiple clouds" )
set( CPACK_PACKAGE_DESCRIPTION_SUMMARY "Store and distribute your data over multiple clouds" )
set( CPACK_DEB_COMPONENT_INSTALL 1 )
set( CPACK_DEBIAN_PACKAGE_CONTROL_EXTRA ${package_extra_controls} )

include( CPack )
