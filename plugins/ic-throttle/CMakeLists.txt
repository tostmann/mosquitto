include_directories(${mosquitto_SOURCE_DIR} ${mosquitto_SOURCE_DIR}/include ${mosquitto_SOURCE_DIR}/deps
			${OPENSSL_INCLUDE_DIR} ${STDBOOL_H_PATH} ${STDINT_H_PATH})

add_library(ic-throttle MODULE ic-throttle.c)
set_target_properties(ic-throttle PROPERTIES
	POSITION_INDEPENDENT_CODE 1
)
set_target_properties(ic-throttle PROPERTIES PREFIX "")

# Don't install, these are example plugins only.
#install(TARGETS ic-throttle RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}" LIBRARY DESTINATION "${CMAKE_INSTALL_LIBDIR}")
