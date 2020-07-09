#include "objInfo.h"
#include "rcConnect.h"

namespace {

} // anonymous namespace

namespace irods::experimental::replica {

/// transition current replica status to the appropriate new replica status
auto update_status(
    rsComm_t* comm,
    irods::file_object_ptr obj,
    const int repl_num)
{

    switch (repl_status) {
        case GOOD_REPLICA:
            break;

        case STALE_REPLICA:
            break;

        case INTERMEDIATE_REPLICA:
            break;

        case READ_LOCK_ON_STALE_REPLICA:
            break;

        case READ_LOCK_ON_GOOD_REPLICA:
            break;

        case WRITE_LOCK_ON_REPLICA:
            break;

        default:
            break;
    }

    return 0;
} // update_replica_status

/// sets specified replica status to intermediate status and write locks sibling replicas
auto make_intermediate(
    rsComm_t* comm,
    irods::file_object_ptr obj,
    const int repl_num)
{
// get connection to database and make sure it's allowed

// set replica to intermediate
    nanodbc::statement stmt{db_conn};

    prepare(stmt, "insert into R_OBJT_METAMAP (object_id, meta_id, create_ts, modify_ts) "
                  "values (?, ?, ?, ?)");
    prepare(stmt, "update R_DATA_MAIN set data_is_dirty = ? where data_id = ? and data_repl_num = ?");

    stmt.bind(0, INTERMEDIATE_REPLICA);
    stmt.bind(1, obj->id());
    stmt.bind(2, repl_num);

    execute(stmt);

// set all sibling replicas to write lock (except for those in read lock)

// commit transaction

} // set_intermediate_replica_status

} // namespace irods::experimental
