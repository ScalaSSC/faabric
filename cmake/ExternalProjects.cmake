include(FindGit)
find_package(Git)
include (ExternalProject)
include (FetchContent)
find_package (Threads REQUIRED)

# Find conan-generated package descriptions
list(PREPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_BINARY_DIR})
list(PREPEND CMAKE_PREFIX_PATH ${CMAKE_CURRENT_BINARY_DIR})

find_program(CONAN_CMD conan)
if(NOT CONAN_CMD)
    message(FATAL_ERROR "Please install Conan 2.x and add it to your PATH.")
endif()

set(CONANFILE ${CMAKE_SOURCE_DIR}/faabric/conanfile.txt)
set(CONAN_OUTPUT_DIR ${CMAKE_CURRENT_BINARY_DIR})

execute_process(
    COMMAND ${CONAN_CMD} install ${CONANFILE} 
    --output-folder=${CONAN_OUTPUT_DIR} 
    --build=missing 
    -s build_type=${CMAKE_BUILD_TYPE} 
    -s compiler.cppstd=20
    RESULT_VARIABLE CONAN_RESULT
)

if(NOT CONAN_RESULT EQUAL 0)
        message(FATAL_ERROR "Conan install failed with exit code: ${CONAN_RESULT}")
endif()

include(${CMAKE_CURRENT_BINARY_DIR}/conan_toolchain.cmake)

find_package(absl REQUIRED)
find_package(Boost 1.80.0 REQUIRED)
find_package(Catch2 REQUIRED)
find_package(flatbuffers REQUIRED)
find_package(fmt REQUIRED)
find_package(hiredis REQUIRED)
# 27/01/2023 - Pin OpenSSL to a specific version to avoid incompatibilities
# with the system's (i.e. Ubuntu 22.04) OpenSSL
find_package(OpenSSL 3.0.2 REQUIRED)
find_package(Protobuf 3.20.0 REQUIRED)
find_package(readerwriterqueue REQUIRED)
find_package(spdlog REQUIRED)
find_package(ZLIB REQUIRED)
find_package(RapidJSON REQUIRED)

# --------------------------------
# Fetch content dependencies
# --------------------------------

# zstd (Conan version not customizable enough)
set(ZSTD_BUILD_CONTRIB OFF CACHE INTERNAL "")
set(ZSTD_BUILD_CONTRIB OFF CACHE INTERNAL "")
set(ZSTD_BUILD_PROGRAMS OFF CACHE INTERNAL "")
set(ZSTD_BUILD_SHARED ON CACHE INTERNAL "")
set(ZSTD_BUILD_STATIC ON CACHE INTERNAL "")
set(ZSTD_BUILD_TESTS OFF CACHE INTERNAL "")
# This means zstd doesn't use threading internally,
# not that it can't be used in a multithreaded context
set(ZSTD_MULTITHREAD_SUPPORT OFF CACHE INTERNAL "")
set(ZSTD_LEGACY_SUPPORT OFF CACHE INTERNAL "")
set(ZSTD_ZLIB_SUPPORT OFF CACHE INTERNAL "")
set(ZSTD_LZMA_SUPPORT OFF CACHE INTERNAL "")
set(ZSTD_LZ4_SUPPORT OFF CACHE INTERNAL "")

# nng (Conan version out of date)
set(NNG_TESTS OFF CACHE INTERNAL "")

FetchContent_Declare(zstd_ext
    GIT_REPOSITORY "https://github.com/facebook/zstd"
    GIT_TAG "v1.5.2"
    SOURCE_SUBDIR "build/cmake"
)
FetchContent_Declare(nng_ext
    GIT_REPOSITORY "https://github.com/nanomsg/nng"
    # NNG tagged version 1.7.1
    GIT_TAG "ec4b5722fba105e3b944e3dc0f6b63c941748b3f"
)

FetchContent_MakeAvailable(zstd_ext)
# Work around zstd not declaring its targets properly
target_include_directories(libzstd_static SYSTEM INTERFACE $<BUILD_INTERFACE:${zstd_ext_SOURCE_DIR}/lib>)
target_include_directories(libzstd_shared SYSTEM INTERFACE $<BUILD_INTERFACE:${zstd_ext_SOURCE_DIR}/lib>)
add_library(zstd::libzstd_static ALIAS libzstd_static)
add_library(zstd::libzstd_shared ALIAS libzstd_shared)

FetchContent_MakeAvailable(nng_ext)
add_library(nng::nng ALIAS nng)

# Group all external dependencies into a convenient virtual CMake library
add_library(faabric_common_dependencies INTERFACE)
target_include_directories(faabric_common_dependencies INTERFACE
    ${FAABRIC_INCLUDE_DIR}
)
target_link_libraries(faabric_common_dependencies INTERFACE
    absl::algorithm_container
    absl::btree
    absl::flat_hash_set
    absl::flat_hash_map
    absl::strings
    # Boost::Boost
    Boost::system
    flatbuffers::flatbuffers
    hiredis::hiredis
    nng::nng
    protobuf::libprotobuf
    readerwriterqueue::readerwriterqueue
    spdlog::spdlog
    Threads::Threads
    zstd::libzstd_static
    rapidjson
)
target_compile_definitions(faabric_common_dependencies INTERFACE
    FMT_DEPRECATED= # Suppress warnings about use of deprecated api by spdlog
)
add_library(faabric::common_dependencies ALIAS faabric_common_dependencies)
