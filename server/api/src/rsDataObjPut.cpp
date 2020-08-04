#include "dataObjPut.h"
#include "rodsLog.h"
#include "dataPut.h"
#include "filePut.h"
#include "objMetaOpr.hpp"
#include "physPath.hpp"
#include "specColl.hpp"
#include "dataObjOpen.h"
#include "dataObjCreate.h"
#include "dataObjClose.h"
#include "regDataObj.h"
#include "dataObjUnlink.h"
#include "rsGlobalExtern.hpp"
#include "rcGlobalExtern.h"
#include "rsApiHandler.hpp"
#include "subStructFilePut.h"
#include "dataObjRepl.h"
#include "getRemoteZoneResc.h"
#include "icatHighLevelRoutines.hpp"
#include "modDataObjMeta.h"
#include "rsDataObjPut.hpp"
#include "rsDataObjRepl.hpp"
#include "rsDataObjClose.hpp"
#include "rsDataPut.hpp"
#include "rsRegDataObj.hpp"
#include "rsDataObjUnlink.hpp"
#include "rsSubStructFilePut.hpp"
#include "rsFilePut.hpp"
#include "rsUnregDataObj.hpp"
#include "rsDataObjOpen.hpp"
#include "rsDataObjWrite.hpp"
#include "rs_finalize_data_object.hpp"

#include "irods_at_scope_exit.hpp"
#include "irods_exception.hpp"
#include "irods_hierarchy_parser.hpp"
#include "irods_logger.hpp"
#include "irods_resource_backport.hpp"
#include "irods_resource_redirect.hpp"
#include "irods_serialization.hpp"
#include "irods_server_properties.hpp"

#include "fmt/format.h"
#include "json.hpp"

namespace {

using json  = nlohmann::json;
using log   = irods::experimental::log;

auto finalize_data_object(
    rsComm_t& _comm,
    const int _data_id,
    const dataObjInfo_t& _info) -> int
{
    log::server::debug("data_id:[{}],_resc_id:[{}],repl_status:[{}]", _data_id, _resc_id, _repl_status);
    const auto input = json{
        {"data_id", std::to_string(_data_id)},
        {"replicas", json::array({
            {
                {"before", {
                    {"data_id", std::to_string(_info->dataId)},
                    {"coll_id", std::to_string(_info->collId)},
                    {"data_name", std::to_string(fs::path{_info->objPath}.filename())},
                    {"data_repl_num", std::to_string(_info->replNum)},
                    {"data_version", std::to_string(_info->version)},
                    {"data_type_name", std::to_string(_info->dataType)},
                    {"data_size", std::to_string(_info->dataSize)},
                    //{"resc_group_name", std::to_string(_info->rescGroup)},
                    {"resc_name", std::to_string(_info->rescName)},
                    {"data_path", std::to_string(_info->filePath)},
                    {"data_owner_name", std::to_string(_info->dataOwnerName)},
                    {"data_owner_zone", std::to_string(_info->dataOwnerZone)},
                    {"data_is_dirty", std::to_string(_info->replStatus)},
                    {"data_status", std::to_string(_info->statusString)},
                    {"data_checksum", std::to_string(_info->chksum)},
                    {"data_expiry_ts", std::to_string(_info->dataExpiry)},
                    {"data_map_id", std::to_string(_info->dataMapId)},
                    {"data_mode", std::to_string(_info->dataMode)},
                    {"r_comment", std::to_string(_info->dataComments)},
                    {"create_ts", std::to_string(_info->dataCreate)},
                    {"modify_ts", std::to_string(_info->dataModify)},
                    {"resc_hier", std::to_string(_info->rescHier)},
                    {"resc_id", std::to_string(_info->rescId)}
                }},
                {"after", {
                    {"data_id", std::to_string(_info->dataId)},
                    {"coll_id", std::to_string(_info->collId)},
                    {"data_name", std::to_string(fs::path{_info->objPath}.filename())},
                    {"data_repl_num", std::to_string(_info->replNum)},
                    {"data_version", std::to_string(_info->version)},
                    {"data_type_name", std::to_string(_info->dataType)},
                    {"data_size", std::to_string(_info->dataSize)},
                    //{"resc_group_name", std::to_string(_info->rescGroup)},
                    {"resc_name", std::to_string(_info->rescName)},
                    {"data_path", std::to_string(_info->filePath)},
                    {"data_owner_name", std::to_string(_info->dataOwnerName)},
                    {"data_owner_zone", std::to_string(_info->dataOwnerZone)},
                    {"data_is_dirty", std::to_string(_info->replStatus)},
                    {"data_status", std::to_string(_info->statusString)},
                    {"data_checksum", std::to_string(_info->chksum)},
                    {"data_expiry_ts", std::to_string(_info->dataExpiry)},
                    {"data_map_id", std::to_string(_info->dataMapId)},
                    {"data_mode", std::to_string(_info->dataMode)},
                    {"r_comment", std::to_string(_info->dataComments)},
                    {"create_ts", std::to_string(_info->dataCreate)},
                    {"modify_ts", std::to_string(_info->dataModify)},
                    {"resc_hier", std::to_string(_info->rescHier)},
                    {"resc_id", std::to_string(_info->rescId)}
                }}
            }
        })}
    }.dump();

    log::server::debug("json input:[{}]", input);

    bytesBuf_t bb{
        .len = static_cast<int>(input.length()),
        .buf = const_cast<char*>(input.c_str())
    };

    return rs_finalize_data_object(&_comm, &bb, nullptr);
} // finalize_data_object

int parallel_transfer_put(
    rsComm_t *rsComm,
    dataObjInp_t *dataObjInp,
    portalOprOut_t **portalOprOut)
{

    // Parallel transfer
    dataObjInp->openFlags |= O_CREAT | O_RDWR;
    int l1descInx = rsDataObjOpen(rsComm, dataObjInp);
    if ( l1descInx < 0 ) {
        return l1descInx;
    }

    L1desc[l1descInx].oprType = PUT_OPR;
    L1desc[l1descInx].dataSize = dataObjInp->dataSize;

    if ( getStructFileType( L1desc[l1descInx].dataObjInfo->specColl ) >= 0 ) { // JMC - backport 4682
        *portalOprOut = ( portalOprOut_t * ) malloc( sizeof( portalOprOut_t ) );
        bzero( *portalOprOut,  sizeof( portalOprOut_t ) );
        ( *portalOprOut )->l1descInx = l1descInx;
        return l1descInx;
    }


    int status = preProcParaPut( rsComm, l1descInx, portalOprOut );

    if ( status < 0 ) {
        openedDataObjInp_t dataObjCloseInp{};
        dataObjCloseInp.l1descInx = l1descInx;
        L1desc[l1descInx].oprStatus = status;
        rsDataObjClose( rsComm, &dataObjCloseInp );
        return status;
    }

    int allFlag = 0;
    if ( getValByKey( &dataObjInp->condInput, ALL_KW ) != NULL ) {
        allFlag = 1;
    }

    dataObjInp_t replDataObjInp{};
    if ( allFlag == 1 ) {
        /* need to save dataObjInp. get freed in sendAndRecvBranchMsg */
        rstrcpy( replDataObjInp.objPath, dataObjInp->objPath, MAX_NAME_LEN );
        addKeyVal( &replDataObjInp.condInput, UPDATE_REPL_KW, "" );
        addKeyVal( &replDataObjInp.condInput, ALL_KW, "" );
    }
    /* return portalOprOut to the client and wait for the rcOprComplete
     * call. That is when the parallel I/O is done */
    int retval = sendAndRecvBranchMsg( rsComm, rsComm->apiInx, status,
            ( void * ) * portalOprOut, NULL );

    if ( retval < 0 ) {
        openedDataObjInp_t dataObjCloseInp{};
        dataObjCloseInp.l1descInx = l1descInx;
        L1desc[l1descInx].oprStatus = retval;
        rsDataObjClose( rsComm, &dataObjCloseInp );
        if ( allFlag == 1 ) {
            clearKeyVal( &replDataObjInp.condInput );
        }
    }
    else if (1 == allFlag) {
        transferStat_t *transStat = NULL;
        status = rsDataObjRepl(rsComm, &replDataObjInp, &transStat);
        free(transStat);
        clearKeyVal(&replDataObjInp.condInput);
        if (status < 0) {
            const auto err{ERROR(status, "rsDataObjRepl failed")};
            irods::log(err);
            return err.code();
        }
    }

    /* already send the client the status */
    return SYS_NO_HANDLER_REPLY_MSG;
} // parallel_transfer_put

int single_buffer_put(
    rsComm_t* rsComm,
    dataObjInp_t* dataObjInp,
    bytesBuf_t* dataObjInpBBuf)
{

    dataObjInp->openFlags |= O_CREAT | O_RDWR;
    int l1descInx = rsDataObjOpen(rsComm, dataObjInp);
    if (l1descInx <= 2) {
        if ( l1descInx >= 0 ) {
            rodsLog( LOG_ERROR,
                    "%s: rsDataObjOpen of %s error, status = %d",
                    __FUNCTION__,
                    dataObjInp->objPath,
                    l1descInx );
            return SYS_FILE_DESC_OUT_OF_RANGE;
        }
        return l1descInx;
    }

    dataObjInfo_t *myDataObjInfo = L1desc[l1descInx].dataObjInfo;
    openedDataObjInp_t dataObjWriteInp{};
    dataObjWriteInp.len = dataObjInpBBuf->len;
    dataObjWriteInp.l1descInx = l1descInx;

    const auto data_id = myDataObjInfo->dataId;
    const auto repl_num = myDataObjInfo->replNum;
    const auto resc_id = myDataObjInfo->rescId;
    const auto repl_status = myDataObjInfo->replStatus;

    bytesBuf_t dataObjWriteInpBBuf{};
    dataObjWriteInpBBuf.buf = dataObjInpBBuf->buf;
    dataObjWriteInpBBuf.len = dataObjInpBBuf->len;
    int bytesWritten = rsDataObjWrite(rsComm, &dataObjWriteInp, &dataObjWriteInpBBuf);
    if ( bytesWritten < 0 ) {
        rodsLog(LOG_NOTICE,
                "%s: rsDataObjWrite for %s failed with %d",
                __FUNCTION__, L1desc[l1descInx].dataObjInfo->filePath, bytesWritten );
        dataObjInfo_t* data_obj_info = L1desc[l1descInx].dataObjInfo;
        const int unlink_status = dataObjUnlinkS(rsComm, L1desc[l1descInx].dataObjInp, data_obj_info);
        if (unlink_status < 0) {
            irods::log(ERROR(unlink_status,
                (boost::format("dataObjUnlinkS failed for [%s] with [%d]") %
                data_obj_info->filePath % unlink_status).str()));
        }
    }

    if ( bytesWritten == 0 && myDataObjInfo->dataSize > 0 ) {
        /* overwrite with 0 len file */
        L1desc[l1descInx].bytesWritten = 1;
    }
    else {
        L1desc[l1descInx].bytesWritten = bytesWritten;
    }

    L1desc[l1descInx].dataSize = dataObjInp->dataSize;

    openedDataObjInp_t dataObjCloseInp{};
    dataObjCloseInp.l1descInx = l1descInx;
    L1desc[l1descInx].oprStatus = bytesWritten;
    L1desc[l1descInx].oprType = PUT_OPR;
    int status = rsDataObjClose(rsComm, &dataObjCloseInp);
    if ( status < 0 ) {
        rodsLog( LOG_DEBUG,
                "%s: rsDataObjClose of %d error, status = %d",
                __FUNCTION__, l1descInx, status );
    }

    if ( bytesWritten < 0 ) {
        return bytesWritten;
    }

    if (status < 0) {
        return status;
    }

    // TODO: the information from the dataObjInfo is out of date
    //  - It's too late to get the information from the L1desc, it has been freed at this point
    //  1. Go to the catalog to get the latest information
    //  2. Rely on rsDataObjClose or replica_close/finalize to return updated data object information and pass that to data object finalize as "before"
    finalize_data_object(*rsComm, data_id, repl_num, resc_id, repl_status);

    if (getValByKey(&dataObjInp->condInput, ALL_KW)) {
        /* update the rest of copies */
        transferStat_t *transStat{};
        status = rsDataObjRepl( rsComm, dataObjInp, &transStat );
        if (transStat) {
            free(transStat);
        }
    }
    if (status >= 0) {
        status = applyRuleForPostProcForWrite(
                rsComm, dataObjInpBBuf, dataObjInp->objPath);
        if (status >= 0) {
            status = 0;
        }
    }
    return status;
} // single_buffer_put

void throw_if_force_put_to_new_resource(
    rsComm_t* comm,
    dataObjInp_t& data_obj_inp,
    irods::file_object_ptr file_obj)
{
    char* dst_resc_kw   = getValByKey( &data_obj_inp.condInput, DEST_RESC_NAME_KW );
    char* force_flag_kw = getValByKey( &data_obj_inp.condInput, FORCE_FLAG_KW );
    if (file_obj->replicas().empty()  ||
        !dst_resc_kw   ||
        !force_flag_kw ||
        strlen( dst_resc_kw ) == 0) {
        return;
    }

    const auto hier_match{
        [&dst_resc_kw, &replicas = file_obj->replicas()]()
        {
            return std::any_of(replicas.cbegin(), replicas.cend(),
            [&dst_resc_kw](const auto& r) {
                return irods::hierarchy_parser{r.resc_hier()}.first_resc() == dst_resc_kw;
            });
        }()
    };
    if (!hier_match) {
        THROW(HIERARCHY_ERROR,
              (boost::format("cannot force put [%s] to a different resource [%s]") %
               data_obj_inp.objPath % dst_resc_kw).str());
    }
} // throw_if_force_put_to_new_resource

} // anonymous namespace

int rsDataObjPut(
    rsComm_t *rsComm,
    dataObjInp_t *dataObjInp,
    bytesBuf_t *dataObjInpBBuf,
    portalOprOut_t **portalOprOut)
{
    rodsServerHost_t *rodsServerHost{};
    specCollCache_t *specCollCache{};

    resolveLinkedPath( rsComm, dataObjInp->objPath, &specCollCache,
                       &dataObjInp->condInput );
    int remoteFlag = getAndConnRemoteZone( rsComm, dataObjInp, &rodsServerHost,
                                       REMOTE_CREATE );

    if (const char* acl_string = getValByKey( &dataObjInp->condInput, ACL_INCLUDED_KW)) {
        try {
            irods::deserialize_acl(acl_string);
        }
        catch (const irods::exception& e) {
            rodsLog(LOG_ERROR, "%s", e.what());
            return e.code();
        }
    }
    if (const char* metadata_string = getValByKey(&dataObjInp->condInput, METADATA_INCLUDED_KW)) {
        try {
            irods::deserialize_metadata( metadata_string );
        }
        catch (const irods::exception& e) {
            rodsLog(LOG_ERROR, "%s", e.what());
            return e.code();
        }
    }

    if (remoteFlag < 0) {
        return remoteFlag;
    }
    else if (LOCAL_HOST != remoteFlag) {
        int status = _rcDataObjPut( rodsServerHost->conn, dataObjInp, dataObjInpBBuf, portalOprOut );
        if (status < 0 ||
            getValByKey(&dataObjInp->condInput, DATA_INCLUDED_KW)) {
            return status;
        }

        /* have to allocate a local l1descInx to keep track of things
         * since the file is in remote zone. It sets remoteL1descInx,
         * oprType = REMOTE_ZONE_OPR and remoteZoneHost so that
         * rsComplete knows what to do */
        int l1descInx = allocAndSetL1descForZoneOpr(
                        ( *portalOprOut )->l1descInx, dataObjInp, rodsServerHost, NULL );
        if ( l1descInx < 0 ) {
            return l1descInx;
        }
        ( *portalOprOut )->l1descInx = l1descInx;
        return status;
    }

    try {
        dataObjInfo_t* dataObjInfoHead{};
        irods::file_object_ptr file_obj(new irods::file_object());
        file_obj->logical_path(dataObjInp->objPath);
        irods::error fac_err = irods::file_object_factory(rsComm, dataObjInp, file_obj, &dataObjInfoHead);

        throw_if_force_put_to_new_resource(rsComm, *dataObjInp, file_obj);

        std::string hier{};
        const char* h{getValByKey(&dataObjInp->condInput, RESC_HIER_STR_KW)};
        if (!h) {
            auto fobj_tuple = std::make_tuple(file_obj, fac_err);
            std::tie(file_obj, hier) = irods::resolve_resource_hierarchy(
                rsComm,
                irods::CREATE_OPERATION,
                *dataObjInp,
                fobj_tuple);
            addKeyVal(&dataObjInp->condInput, RESC_HIER_STR_KW, hier.c_str());
        }
        else {
            if (!fac_err.ok()) {
                irods::log(fac_err);
            }
            hier = h;
        }

        const auto hier_has_replica{[&hier, &replicas = file_obj->replicas()]() {
            return std::any_of(replicas.begin(), replicas.end(),
                [&](const irods::physical_object& replica) {
                    return replica.resc_hier() == hier;
                });
            }()};

        if (hier_has_replica && !getValByKey(&dataObjInp->condInput, FORCE_FLAG_KW)) {
            return OVERWRITE_WITHOUT_FORCE_FLAG;
        }
    }
    catch (const irods::exception& e) {
        irods::log(LOG_ERROR, e.what());
        return e.code();
    }

    int status2 = applyRuleForPostProcForWrite( rsComm, dataObjInpBBuf, dataObjInp->objPath );
    if ( status2 < 0 ) {
        return ( status2 );
    }

    dataObjInp->openFlags = O_RDWR;

    if (getValByKey(&dataObjInp->condInput, DATA_INCLUDED_KW)) {
        return single_buffer_put(rsComm, dataObjInp, dataObjInpBBuf);
    }

    return parallel_transfer_put( rsComm, dataObjInp, portalOprOut );
} // rsDataObjPut

int preProcParaPut(
    rsComm_t *rsComm,
    int l1descInx,
    portalOprOut_t **portalOprOut)
{
    int status;
    dataOprInp_t dataOprInp;

    initDataOprInp( &dataOprInp, l1descInx, PUT_OPR );
    /* add RESC_HIER_STR_KW for getNumThreads */
    if ( L1desc[l1descInx].dataObjInfo != NULL ) {
        addKeyVal( &dataOprInp.condInput, RESC_HIER_STR_KW,
                   L1desc[l1descInx].dataObjInfo->rescHier );
    }
    if ( L1desc[l1descInx].remoteZoneHost != NULL ) {
        status =  remoteDataPut( rsComm, &dataOprInp, portalOprOut,
                                 L1desc[l1descInx].remoteZoneHost );
    }
    else {
        status =  rsDataPut( rsComm, &dataOprInp, portalOprOut );
    }

    if ( status >= 0 ) {
        ( *portalOprOut )->l1descInx = l1descInx;
        L1desc[l1descInx].bytesWritten = dataOprInp.dataSize;
    }
    clearKeyVal( &dataOprInp.condInput );
    return status;
} // preProcParaPut

