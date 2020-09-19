option (USE_INTERNAL_POCO_LIBRARY "Use internal Poco library" ON)

if (USE_INTERNAL_POCO_LIBRARY)
    set (LIBRARY_DIR ${ClickHouse_SOURCE_DIR}/contrib/poco)
else ()
    find_path (ROOT_DIR NAMES Foundation/include/Poco/Poco.h include/Poco/Poco.h)
    if (NOT ROOT_DIR)
        message (${RECONFIGURE_MESSAGE_LEVEL} "Can't find system poco")
    endif()
endif ()
