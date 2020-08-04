#ifndef IRODS_CATALOG_UTILITIES_HPP
#define IRODS_CATALOG_UTILITIES_HPP

#include "rcConnect.h"

#include "nanodbc/nanodbc.h"

#include <map>
#include <string>

namespace irods::experimental::catalog {

    namespace r_data_main {
        const std::vector<std::string> columns{
            "data_id",
            "coll_id",
            "data_name",
            "data_repl_num",
            "data_version",
            "data_type_name",
            "data_size",
            //"resc_group_name",
            "resc_name",
            "data_path",
            "data_owner_name",
            "data_owner_zone",
            "data_is_dirty",
            "data_status",
            "data_checksum",
            "data_expiry_ts",
            "data_map_id",
            "data_mode",
            "r_comment",
            "create_ts",
            "modify_ts",
            "resc_hier",
            "resc_id"};
    } // namespace r_data_main

    enum class entity_type {
        data_object,
        collection,
        user,
        resource,
        zone
    };

    const std::map<std::string, entity_type> entity_type_map{
        {"data_object", entity_type::data_object},
        {"collection", entity_type::collection},
        {"user", entity_type::user},
        {"resource", entity_type::resource},
        {"zone", entity_type::zone}
    };

    auto user_has_permission_to_modify_metadata(rsComm_t& _comm,
                                                nanodbc::connection& _db_conn,
                                                int _object_id,
                                                const entity_type _entity) -> bool;
}

#endif // #ifndef IRODS_CATALOG_UTILITIES_HPP
