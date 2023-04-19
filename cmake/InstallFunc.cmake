######################
## Common functions ##
######################

function( add_ncloud_install_target target component )
    install(
            TARGETS ${target}
            CONFIGURATIONS Release 
            COMPONENT ${component}
            RUNTIME DESTINATION bin
            LIBRARY DESTINATION lib
            ARCHIVE DESTINATION lib
    )
endfunction( add_ncloud_install_target )

function( add_ncloud_install_libs shared_lib_regex component )
    install(
            DIRECTORY ${PROJECT_BINARY_DIR}/lib/
            DESTINATION lib
            CONFIGURATIONS Release
            COMPONENT ${component}
            FILES_MATCHING 
                REGEX "${shared_lib_regex}.so*"
                PATTERN "aws-*" EXCLUDE
                PATTERN "libncloud*" EXCLUDE
                PATTERN "cmake" EXCLUDE
                PATTERN "pkgconfig" EXCLUDE
    )
endfunction( add_ncloud_install_libs )

function( add_ncloud_install_service service component )
    install(
            FILES ${service}
            DESTINATION lib/ncloud
            CONFIGURATIONS Release 
            COMPONENT ${component}
            PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE
    )
endfunction( add_ncloud_install_service )

function( add_ncloud_sample_config component )
    install(
            DIRECTORY ${PROJECT_SOURCE_DIR}/sample
            DESTINATION lib/ncloud
            CONFIGURATIONS Release 
            COMPONENT ${component}
    )
endfunction( add_ncloud_sample_config )


#########################
## Common dependencies ##
#########################

set(
    package_extra_controls
    ${CMAKE_SOURCE_DIR}/scripts/package/postinst
    ${CMAKE_SOURCE_DIR}/scripts/package/prerm
)

set( common_package_depends_list
    " libboost-filesystem${BOOST_VERSION} (>= ${BOOST_VERSION}) "
    " libboost-system${BOOST_VERSION} (>= ${BOOST_VERSION}) "
    " libboost-timer${BOOST_VERSION} (>= ${BOOST_VERSION}) "
    " libboost-log${BOOST_VERSION} (>= ${BOOST_VERSION}) "
    " libboost-random${BOOST_VERSION} (>= ${BOOST_VERSION}) "
    " libboost-locale${BOOST_VERSION} (>= ${BOOST_VERSION}) "
    " libboost-regex${BOOST_VERSION} (>= ${BOOST_VERSION}) "
)

