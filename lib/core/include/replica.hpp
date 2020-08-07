#ifndef IRODS_REPLICA_HPP
#define IRODS_REPLICA_HPP

#ifdef RODS_SERVER
    #define IRODS_FILESYSTEM_ENABLE_SERVER_SIDE_API
    #include "rsDataObjChksum.hpp"
#else
    #undef IRODS_FILESYSTEM_ENABLE_SERVER_SIDE_API
    #include "dataObjChksum.h"
#endif

#include "filesystem.hpp"
#include "irods_exception.hpp"
#include "key_value_proxy.hpp"
#include "modDataObjMeta.h"
#include "objInfo.h"
#include "query_builder.hpp"
#include "rcConnect.h"

#include "fmt/format.h"

#include <chrono>
#include <iomanip>
#include <string_view>

namespace irods::experimental::replica {

    enum class verification_calculation
    {
        if_empty,
        always
    };

    namespace {

        namespace fs = irods::experimental::filesystem;

    } // anonymous namespace

    namespace detail {

        inline auto throw_if_path_is_not_a_data_object(rxComm& _comm, const fs::path& _p) -> void
        {
            if (!fs::NAMESPACE_IMPL::is_data_object(_comm, _p)) {
                THROW(SYS_INVALID_INPUT_PARAM, "path does not point to a data object");
            }
        } // throw_if_path_is_not_a_data_object

        inline auto throw_if_replica_number_is_invalid(const int _rn) -> void
        {
            if (_rn < 0) {
                THROW(SYS_INVALID_INPUT_PARAM, "invalid replica number");
            }
        } // throw_if_replica_number_is_invalid

        template<typename rxComm>
        inline auto throw_if_replica_input_is_invalid(
            rxComm& _comm,
            const fs::path& _p,
            const int _replica_number) -> void
        {
            fs::detail::throw_if_path_is_empty(_p);

            fs::detail::throw_if_path_length_exceeds_limit(_p);

            throw_if_path_is_not_a_data_object(_comm, _p);

            throw_if_replica_number_is_invalid(_replica_number);
        } // throw_if_replica_input_is_invalid

    } // namespace detail

    // last write time

    namespace {
        // TODO: make this into an API plugin
        template<typename rxComm>
        auto get_replica_info(
            rxComm& _comm,
            const fs::path& _p,
            //const int _replica_number) -> irods::query<rxComm>::value_type
            const int _replica_number) -> std::vector<std::string>
        {
            query_builder qb;
            qb.zone_hint(*fs::get_zone_name(_p));

            const std::string qstr = fmt::format(
                "SELECT "
                "DATA_ID, "
                "DATA_COLL_ID, "
                "DATA_NAME, "
                "DATA_REPL_NUM, "
                "DATA_VERSION,"
                "DATA_TYPE_NAME, "
                "DATA_SIZE, "
                "DATA_RESC_NAME, "
                "DATA_PATH, "
                "DATA_OWNER_NAME, "
                "DATA_OWNER_ZONE, "
                "DATA_REPL_STATUS, "
                "DATA_STATUS, "
                "DATA_CHECKSUM, "
                "DATA_EXPIRY, "
                "DATA_MAP_ID, "
                "DATA_COMMENTS, "
                "DATA_CREATE_TIME, "
                "DATA_MODIFY_TIME, "
                "DATA_MODE, "
                "DATA_RESC_HIER, "
                "DATA_RESC_ID "
                "WHERE DATA_NAME = '{}' AND COLL_NAME = '{}' AND DATA_REPL_NUM = '{}'",
                _p.object_name().string(), _p.parent_path().string(), _replica_number);

            const auto q = qb.build<rxComm>(_comm, qstr);
            if (q.size() <= 0) {
                THROW(CAT_NO_ROWS_FOUND, "no replica information found");
            }
            return q.front();
        } // get_replica_info

        template<typename rxComm>
        auto replica_mtime(
            rxComm& _comm,
            const fs::path& _p,
            const int _replica_number) -> std::string
        {
            const auto result = get_replica_info(_comm, _p, _replica_number);
            return result[18]; // 18 == index of MODIFY_TIME column from SELECT statement
        } // replica_mtime
    } // anonymous namespace

    template<typename rxComm>
    auto replica_size(rxComm& _comm, const fs::path& _p, const int _replica_number) -> std::uintmax_t
    {
        detail::throw_if_replica_input_is_invalid(_comm, _p, _replica_number);

        const auto result = get_replica_info(_comm, _p, _replica_number);

        return static_cast<std::uintmax_t>(std::stoull(result[6])); // 6 == index of DATA_SIZE column from SELECT statement
    } // replica_size

    template<typename rxComm>
    auto is_replica_empty(rxComm& _comm, const fs::path& _p, const int _replica_number) -> bool
    {
        return replica_size(_comm, _p, _replica_number) == 0;
    } // is_replica_empty

    template<typename rxComm>
    auto replica_checksum(
        rxComm& _comm,
        const fs::path& _p,
        const int _replica_number,
        verification_calculation _calculation = verification_calculation::if_empty) -> std::string
    {
        detail::throw_if_replica_input_is_invalid(_comm, _p, _replica_number);

        dataObjInp_t input{};
        std::string replica_number_string;
        auto cond_input = make_key_value_proxy(input.condInput);

        cond_input[REPL_NUM_KW] = std::to_string(_replica_number);

        std::strncpy(input.objPath, _p.c_str(), std::strlen(_p.c_str()));

        if (verification_calculation::always == _calculation) {
            cond_input[FORCE_CHKSUM_KW] = "";
        }

        char* checksum{};

        if constexpr (std::is_same_v<rxComm, rsComm_t>) {
            if (const auto ec = rsDataObjChksum(&_comm, &input, &checksum); ec < 0) {
                throw fs::filesystem_error{"cannot calculate checksum", _p, fs::detail::make_error_code(ec)};
            }
        }
        else {
            if (const auto ec = rcDataObjChksum(&_comm, &input, &checksum); ec < 0) {
                throw fs::filesystem_error{"cannot calculate checksum", _p, fs::detail::make_error_code(ec)};
            }
        }

        return checksum ? checksum : std::string{};
    } // replica_checksum

    template<typename rxComm>
    auto last_write_time(rxComm& _comm, const fs::path& _p, const int _replica_number) -> fs::object_time_type
    {
        detail::throw_if_replica_input_is_invalid(_comm, _p, _replica_number);

        return fs::object_time_type{std::chrono::seconds{std::stoull(replica_mtime(_comm, _p, _replica_number))}};
    } // last_write_time

    template<typename rxComm>
    auto last_write_time(
        rxComm& _comm,
        const fs::path& _p,
        const int _replica_number,
        const fs::object_time_type _new_time) -> void
    {
        detail::throw_if_replica_input_is_invalid(_comm, _p, _replica_number);

        const auto seconds = _new_time.time_since_epoch();
        std::stringstream new_time;
        new_time << std::setfill('0') << std::setw(11) << std::to_string(seconds.count());

        dataObjInfo_t info{};
        std::strncpy(info.objPath, _p.c_str(), std::strlen(_p.c_str()));

        auto [reg_params, lm] = make_key_value_proxy({{DATA_MODIFY_KW, new_time.str()}});

        modDataObjMeta_t input{};
        input.dataObjInfo = &info;
        input.regParam = reg_params.get();

        const auto mod_obj_info_fcn = [](rxComm& _comm, modDataObjMeta_t& _inp)
        {
            if constexpr (std::is_same_v<rxComm, rsComm_t>) {
                return rsModDataObjMeta(&_comm, &_inp);
            }
            else {
                return rcModDataObjMeta(&_comm, &_inp);
            }
        };

        if (const auto ec = mod_obj_info_fcn(_comm, input); ec != 0) {
            throw fs::filesystem_error{"cannot set mtime", _p, fs::detail::make_error_code(ec)};
        }
    } // last_write_time

} // namespace irods::experimental::replica

#endif // #ifndef IRODS_REPLICA_HPP
