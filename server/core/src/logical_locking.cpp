#include "objInfo.h"
#include "rcConnect.h"
#include "irods_file_object.hpp"

#include "catalog.hpp"

namespace irods::experimental::replica {

    /// sets specified replica status to intermediate status and write locks sibling replicas
    auto make_intermediate(
        rsComm_t* comm,
        irods::file_object_ptr obj,
        const int repl_num) -> int
    {
        namespace ic = irods::experimental::catalog;

    // get connection to database and make sure it's allowed
        std::string db_instance_name;
        nanodbc::connection db_conn;

        try {
            std::tie(db_instance_name, db_conn) = ic::new_database_connection();
        }
        catch (const std::exception& e) {
            irods::log(LOG_ERROR, e.what());
            return SYS_CONFIG_FILE_ERR;
        }

        return ic::execute_transaction(db_conn, [&](auto& _trans) -> int
        {
            try {
                nanodbc::statement stmt{db_conn};

                nanodbc::prepare(stmt, "update R_DATA_MAIN set data_is_dirty = ? where data_id = ? and data_repl_num = ?");

                stmt.bind(0, std::to_string(INTERMEDIATE_REPLICA).c_str());
                stmt.bind(1, std::to_string(obj->id()).c_str());
                stmt.bind(2, std::to_string(repl_num).c_str());

                // TODO: set all sibling replicas to write lock (except for those in read lock)

                execute(stmt);

                _trans.commit();

                return 0;
            }
            catch (...) {
                return -1;
            }
        });

        return 0;
    } // set_intermediate_replica_status

} // namespace irods::experimental
