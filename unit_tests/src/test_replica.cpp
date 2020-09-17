#include "catch.hpp"

#include "client_connection.hpp"
#include "dstream.hpp"
#include "irods_at_scope_exit.hpp"
#include "replica.hpp"
#include "resource_administration.hpp"
#include "rodsClient.h"
#include "transport/default_transport.hpp"
#include "unit_test_utils.hpp"

#include <chrono>
#include <iostream>
#include <string_view>
#include <thread>

namespace fs = irods::experimental::filesystem;
namespace io = irods::experimental::io;
namespace replica = irods::experimental::replica;

// IMPORTANT NOTE REGARDING THE CLIENT_CONNECTION OBJECTS
// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
// Connection Pools do not work well with the resource administration library
// because the resource manager within the agents do not see any changes to the
// resource hierarchies. The only way around this is to spawn a new agent by
// creating a new connection to the iRODS server.

TEST_CASE("replica", "[replica]")
{
    using namespace std::chrono_literals;
    using std::chrono::system_clock;
    using std::chrono::time_point_cast;

    load_client_api_plugins();

    irods::experimental::client_connection setup_comm;
    RcComm& setup_conn = static_cast<RcComm&>(setup_comm);

    rodsEnv env;
    _getRodsEnv(env);

    const auto sandbox = fs::path{env.rodsHome} / "test_replica";

    if (!fs::client::exists(setup_conn, sandbox)) {
        REQUIRE(fs::client::create_collection(setup_conn, sandbox));
    }

    irods::at_scope_exit remove_sandbox{[&sandbox] {
        irods::experimental::client_connection conn;
        REQUIRE(fs::client::remove_all(conn, sandbox, fs::remove_options::no_trash));
    }};

    const auto target_object = sandbox / "target_object";

    std::string_view expected_checksum = "sha2:z4DNiu1ILV0VJ9fccvzv+E5jJlkoSER9LcCw6H38mpA=";

    std::string_view object_content = "testing";

    {
        io::client::default_transport tp{setup_conn};
        io::odstream{tp, target_object} << object_content;
    }

    std::this_thread::sleep_for(2s);

    const std::string_view resc_name = "unit_test_ufs";

    {
        // create resource.
        irods::experimental::client_connection conn;
        REQUIRE(unit_test_utils::add_ufs_resource(conn, resc_name, "unit_test_vault"));
    }

    {
        // replicate replica.
        irods::experimental::client_connection conn;
        REQUIRE(unit_test_utils::replicate_data_object(conn, target_object.c_str(), resc_name));
    }

    irods::at_scope_exit clean_up{[&target_object, &resc_name] {
        namespace ix = irods::experimental;
        ix::client_connection conn;
        fs::client::remove(conn, target_object, fs::remove_options::no_trash);
        ix::administration::client::remove_resource(conn, resc_name);
    }};

    SECTION("perform library operations using replica number")
    {
        irods::experimental::client_connection comm;
        RcComm& conn = static_cast<RcComm&>(comm);

        // the replica number of the second replica.
        const auto second_replica = 1;

        // size
        REQUIRE(!replica::is_replica_empty(conn, target_object, second_replica));
        REQUIRE(object_content.length() == replica::replica_size(conn, target_object, second_replica));

        // checksum
        REQUIRE(replica::replica_checksum(conn, target_object, second_replica) == expected_checksum);

        // mtime (access)
        const auto old_mtime = replica::last_write_time(conn, target_object, 0);

        // show that the mtime of each replica is different.
        REQUIRE(old_mtime != replica::last_write_time(conn, target_object, second_replica));

        // mtime (modification)
        replica::last_write_time(conn, target_object, second_replica, old_mtime);
        REQUIRE(old_mtime == replica::last_write_time(conn, target_object, second_replica));
    }

    SECTION("perform library operations using resource name")
    {
        irods::experimental::client_connection comm;
        RcComm& conn = static_cast<RcComm&>(comm);

        // size
        REQUIRE(!replica::is_replica_empty(conn, target_object, resc_name));
        REQUIRE(object_content.length() == replica::replica_size(conn, target_object, resc_name));

        // checksum
        REQUIRE(replica::replica_checksum(conn, target_object, resc_name) == expected_checksum);

        // mtime (access)
        const auto old_mtime = replica::last_write_time(conn, target_object, 0);

        // show that the mtime of each replica is different.
        REQUIRE(old_mtime != replica::last_write_time(conn, target_object, resc_name));

        // mtime (modification)
        replica::last_write_time(conn, target_object, resc_name, old_mtime);
        REQUIRE(old_mtime == replica::last_write_time(conn, target_object, resc_name));
    }
}

