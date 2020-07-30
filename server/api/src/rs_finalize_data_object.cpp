#include "catalog.hpp"
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

auto update_replica_status(
    nanodbc::connection& _db_conn,
    std::string_view _data_id,
    std::string_view _repl_num,
    std::string_view _status) -> void
{
    nanodbc::statement s{_db_conn};

    log::server::debug("updating data_id [{}] repl_num [{}] with status [{}]", _data_id, _repl_num, _status);

    prepare(s, "update R_DATA_MAIN set "
               "data_is_dirty = ? where "
               "data_id = ? and data_repl_num = ?");

    s.bind(0, _status.data());
    s.bind(1, _data_id.data());
    s.bind(2, _repl_num.data());

    execute(s);
} // update_replica_status

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
    std::map<std::string, std::string> replica_statuses;

    log::server::debug("grabbing info from parsed JSON");

    try {
        data_id = input.at("data_id").get<std::string>();
        log::server::debug("found data_id:[{}]", data_id);
        replica_statuses = map_replica_statuses(input);
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
            for (auto&& r : replica_statuses) {
                update_replica_status(db_conn, data_id, r.first, r.second);
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

