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
        const json& _before,
        const json& _after) -> void
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

        log::database::debug("statement:[{}]", s);

        prepare(statement, s);

        // apply bind variables
        std::size_t i = 0;
        for (; i < ic::r_data_main::columns.size(); ++i) {
            // TODO: don't do this -- need a copy of the column names
            const auto& key   = ic::r_data_main::columns[i];
            const auto& value = _after.at(key).get<std::string>();

            log::database::trace("binding [{}] to [{}] at [{}]", key, value, i);

            // TODO: don't do this
            statement.bind(i, value.c_str());
        }

        log::database::trace("binding data_id:[{}] at [{}]", _data_id, i);
        statement.bind(i++, _data_id.data());
        log::database::trace("binding data_repl_num:[{}] at [{}]", _before.at("data_repl_num").get<std::string>(), i);
        statement.bind(i,   _before.at("data_repl_num").get<std::string>().c_str());

        execute(statement);
#else
            nanodbc::statement statement{_db_conn};

            // prepare statement...
            const std::string s{"update R_DATA_MAIN set"
                " data_id = ?"
                ",coll_id = ?"
                ",data_name = ?"
                ",data_repl_num = ?"
                ",data_version = ?"
                ",data_type_name = ?"
                ",data_size = ?"
                //",resc_group_name = ?"
                //",resc_name = ?"
                ",data_path = ?"
                ",data_owner_name = ?"
                ",data_owner_zone = ?"
                ",data_is_dirty = ?"
                ",data_status = ?"
                ",data_checksum = ?"
                ",data_expiry_ts = ?"
                ",data_map_id = ?"
                ",data_mode = ?"
                ",r_comment = ?"
                ",create_ts = ?"
                ",modify_ts = ?"
                //",resc_hier = ?"
                ",resc_id = ?"
                " where resc_id = ? and data_id = ?"};

            log::database::debug("[{}:{}] - statement:[{}]", __FUNCTION__, __LINE__, s);

            prepare(statement, s);

            // set columns...
            const auto data_id          = std::stoll(_after.at("data_id").get<std::string>());
            const auto coll_id          = std::stoll(_after.at("coll_id").get<std::string>());
            const auto data_name        = _after.at("data_name").get<std::string>();
            const auto data_repl_num    = std::stoi(_after.at("data_repl_num").get<std::string>());
            const auto data_version     = _after.at("data_version").get<std::string>();
            const auto data_type_name   = _after.at("data_type_name").get<std::string>();
            const auto data_size        = std::stoll(_after.at("data_size").get<std::string>());
            //const auto resc_group_name = _after.at("resc_group_name").get<std::string>();
            //const auto resc_name = _after.at("resc_name").get<std::string>();
            const auto data_path        = _after.at("data_path").get<std::string>();
            const auto data_owner_name  = _after.at("data_owner_name").get<std::string>();
            const auto data_owner_zone  = _after.at("data_owner_zone").get<std::string>();
            const auto data_is_dirty    = std::stoi(_after.at("data_is_dirty").get<std::string>());
            const auto data_status      = _after.at("data_status").get<std::string>();
            const auto data_checksum    = _after.at("data_checksum").get<std::string>();
            const auto data_expiry_ts   = _after.at("data_expiry_ts").get<std::string>();
            const auto data_map_id      = std::stoll(_after.at("data_map_id").get<std::string>());
            const auto data_mode        = _after.at("data_mode").get<std::string>();
            const auto r_comment        = _after.at("r_comment").get<std::string>();
            const auto create_ts        = _after.at("create_ts").get<std::string>();
            const auto modify_ts        = _after.at("modify_ts").get<std::string>();
            //const auto resc_hier = _after.at("resc_hier").get<std::string>();
            const auto resc_id          = std::stoll(_after.at("resc_id").get<std::string>());

            // where columns...
            const rodsLong_t b_data_id = std::stoll(_data_id.data());
            const rodsLong_t b_resc_id = std::stoll(_before.at("resc_id").get<std::string>());

            // bind em...
            short index{};
            log::database::trace("data_id:[{}],index:[{}]", data_id, index);
            statement.bind(index++, &data_id);
            log::database::trace("coll_id:[{}],index:[{}]", coll_id, index);
            statement.bind(index++, &coll_id);
            log::database::trace("data_name:[{}],index:[{}]", data_name, index);
            statement.bind(index++, data_name.data());
            log::database::trace("data_repl_num:[{}],index:[{}]", data_repl_num, index);
            statement.bind(index++, &data_repl_num);
            log::database::trace("data_version:[{}],index:[{}]", data_version, index);
            statement.bind(index++, data_version.data());
            log::database::trace("data_type_name:[{}],index:[{}]", data_type_name, index);
            statement.bind(index++, data_type_name.data());
            log::database::trace("data_size:[{}],index:[{}]", data_size, index);
            statement.bind(index++, &data_size);
            //log::database::trace("resc_group_name:[{}],index:[{}]", resc_group_name, index);
            //statement.bind(index++, &resc_group_name);
            //log::database::trace("resc_name:[{}],index:[{}]", resc_name, index);
            //statement.bind(index++, resc_name.data());
            log::database::trace("data_path:[{}],index:[{}]", data_path, index);
            statement.bind(index++, data_path.data());
            log::database::trace("data_owner_name:[{}],index:[{}]", data_owner_name, index);
            statement.bind(index++, data_owner_name.data());
            log::database::trace("data_owner_zone:[{}],index:[{}]", data_owner_zone, index);
            statement.bind(index++, data_owner_zone.data());
            log::database::trace("data_is_dirty:[{}],index:[{}]", data_is_dirty, index);
            statement.bind(index++, &data_is_dirty);
            log::database::trace("data_status:[{}],index:[{}]", data_status, index);
            statement.bind(index++, data_status.data());
            log::database::trace("data_checksum:[{}],index:[{}]", data_checksum, index);
            statement.bind(index++, data_checksum.data());
            log::database::trace("data_expiry_ts:[{}],index:[{}]", data_expiry_ts, index);
            statement.bind(index++, data_expiry_ts.data());
            log::database::trace("data_map_id:[{}],index:[{}]", data_map_id, index);
            statement.bind(index++, &data_map_id);
            log::database::trace("data_mode:[{}],index:[{}]", data_mode, index);
            statement.bind(index++, data_mode.data());
            log::database::trace("r_comment:[{}],index:[{}]", r_comment, index);
            statement.bind(index++, r_comment.data());
            log::database::trace("create_ts:[{}],index:[{}]", create_ts, index);
            statement.bind(index++, create_ts.data());
            log::database::trace("modify_ts:[{}],index:[{}]", modify_ts, index);
            statement.bind(index++, modify_ts.data());
            //log::database::trace("resc_hier:[{}],index:[{}]", resc_hier, index);
            //statement.bind(index++, resc_hier.data());
            log::database::trace("resc_id:[{}],index:[{}]", resc_id, index);
            statement.bind(index++, &resc_id);

            log::database::debug("b_resc_id:[{}],index:[{}]", b_resc_id, index);
            statement.bind(index++, &b_resc_id);
            log::database::debug("b_data_id:[{}],index:[{}]", b_data_id, index);
            statement.bind(index++, &b_data_id);

            log::database::trace("index:[{}]", index);

            execute(statement);
    } // set_replica_state

    auto set_data_object_state(
        nanodbc::connection& _db_conn,
        std::string_view _data_id,
        const json& _before,
        const json& _after) -> void
    {
        try {
            for (auto&& r : replicas) {
                set_replica_state(_db_conn, _data_id, _before, _after);
            }

            log::database::trace("committing transaction");
            _trans.commit();
        }
        catch (const nanodbc::database_error& e) {
            THROW(SYS_LIBRARY_ERROR, e.what());
        }
        catch (const std::exception& e) {
            THROW(SYS_INTERNAL_ERR, e.what());
        }
    } // set_data_object_state

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
        log::database::debug("input:[{}]", input.dump());
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
        log::database::error(e.what());
        return SYS_CONFIG_FILE_ERR;
    }

    return ic::execute_transaction(db_conn, [&](auto& _trans) -> int
    {
        try {
            set_data_object_state(db_conn, data_id, r.at("before"), r.at("after"));
            return 0;
        }
        catch (const irods::exception& e) {
            log::database::error(e.what());
            return e.code();
        }
    });

} // rs_finalize_data_object

