#include "catalog.hpp"
#include "catalog_utilities.hpp"
#include "irods_exception.hpp"
#include "irods_logger.hpp"
#include "rs_finalize_data_object.hpp"

#include "fmt/format.h"
#include "json.hpp"
#include "nanodbc/nanodbc.h"

#include <string_view>

namespace {

    using log   = irods::experimental::log;
    using json  = nlohmann::json;

    namespace ic    = irods::experimental::catalog;

    auto set_replica_state(
        nanodbc::connection& _db_conn,
        std::string_view _data_id,
        json& _before,
        json& _after) -> void
    {
#if 0
        nanodbc::statement statement{_db_conn};

        // prepare SQL statement string
        std::string s{"update R_DATA_MAIN set"};

        for (auto&& c : ic::r_data_main::columns) {
            s += fmt::format(" {} = ?", c);
            if (ic::r_data_main::columns.back() != c) {
                s+= ",";
            }
        }

        s += " where data_id = ? and data_repl_num = ?";

        log::server::debug("statement:[{}]", s);

        prepare(statement, s);

        // apply bind variables
        std::size_t i = 0;
        for (; i < ic::r_data_main::columns.size(); ++i) {
            const auto& key   = ic::r_data_main::columns[i];
            const auto& value = _after.at(key).get<std::string>();

            log::server::debug("binding [{}] to [{}] at [{}]", key, value, i);

            statement.bind(i, value.c_str());
        }

        log::server::debug("binding data_id:[{}] at [{}]", _data_id, i);
        statement.bind(i++, _data_id.data());
        log::server::debug("binding data_repl_num:[{}] at [{}]", _before.at("data_repl_num").get<std::string>(), i);
        statement.bind(i,   _before.at("data_repl_num").get<std::string>().c_str());

        execute(statement);
#else
        nanodbc::statement statement{_db_conn};

        prepare(statement, "update R_DATA_MAIN set"
                           " data_is_dirty = ? "
                           " where data_id = ?");

        statement.bind(0, _after.at("data_is_dirty").get<std::string>().c_str());
        statement.bind(1, _data_id.data());

        log::server::debug("binding data_is_dirty:[{}] at [0]", _after.at("data_is_dirty").get<std::string>());
        log::server::debug("binding data_id:[{}] at [1]", _data_id);

        execute(statement);
#endif
    } // set_replica_state

auto map_replica_statuses(json _input) -> std::map<std::string, std::string>
{
    std::map<std::string, std::string> m;

    const auto& statuses = _input.at("statuses");

    log::server::debug("number of statuses found: [{}]", statuses.size());

    for (json::size_type i = 0; i < statuses.size(); ++i) {
        const auto number = statuses[i].at("replica_number").get<std::string>();
        const auto status = statuses[i].at("replica_status").get<std::string>();

        log::server::debug("adding map[{}] = [{}]", number, status);

        m[number] = status;
    }

    return m;
} // map_replica_statuses

} // anonymous namespace

auto rs_finalize_data_object(
    rsComm_t* _comm,
    bytesBuf_t* _input,
    bytesBuf_t** _output) -> int
{

    namespace ic = irods::experimental::catalog;

    // TODO: redirect to catalog provider

    json input;

    try {
        input = json::parse(std::string(static_cast<const char*>(_input->buf), _input->len));
    }
    catch (const json::parse_error& e) {
        log::api::error({{"log_message", "Failed to parse input into JSON"},
                         {"error_message", e.what()}});

        //const auto err_info = make_error_object(json{}, 0, e.what());
        //*_output = to_bytes_buffer(err_info.dump());

        return INPUT_ARG_NOT_WELL_FORMED_ERR;
    }

    std::string data_id;
    json replicas;

    try {
        data_id = input.at("data_id").get<std::string>();
        replicas = input.at("replicas");
    }
    catch (const std::exception& e) {
        //*_output = to_bytes_buffer(make_error_object(json{}, 0, e.what()).dump());
        return SYS_INVALID_INPUT_PARAM;
    }

    nanodbc::connection db_conn;

    try {
        std::tie(std::ignore, db_conn) = ic::new_database_connection();
    }
    catch (const std::exception& e) {
        log::server::error(e.what());
        return SYS_CONFIG_FILE_ERR;
    }

    return ic::execute_transaction(db_conn, [&](auto& _trans) -> int
    {
        try {
            for (auto&& r : replicas) {
                log::server::debug("setting status for replica before:[{}],after:[{}]", r.at("before").dump(), r.at("after").dump());
                set_replica_state(db_conn, data_id, r.at("before"), r.at("after"));
            }

            _trans.commit();

            return 0;
        }   
        catch (const irods::exception& e) {
            log::server::error(e.what());
            return e.code();
        }
        catch (const std::exception& e) {
            log::server::error(e.what());
            return SYS_INTERNAL_ERR;
        }       
    });

} // rs_finalize_data_object

