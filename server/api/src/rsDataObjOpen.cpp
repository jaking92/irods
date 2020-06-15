#include "dataObjOpen.h"
#include "dataObjOpenAndStat.h"
#include "rodsErrorTable.h"
#include "rodsLog.h"
#include "objMetaOpr.hpp"
#include "resource.hpp"
#include "dataObjOpr.hpp"
#include "physPath.hpp"
#include "dataObjCreate.h"
#include "dataObjLock.h"
#include "rsDataObjOpen.hpp"
#include "apiNumber.h"
#include "rsDataObjCreate.hpp"
#include "rsModDataObjMeta.hpp"
#include "rsSubStructFileOpen.hpp"
#include "rsFileOpen.hpp"
#include "rsDataObjRepl.hpp"
#include "rsRegReplica.hpp"
#include "rsDataObjClose.hpp"
#include "rsPhyPathReg.hpp"

#include "fileOpen.h"
#include "subStructFileOpen.h"
#include "rsGlobalExtern.hpp"
#include "rcGlobalExtern.h"
#include "getRemoteZoneResc.h"
#include "regReplica.h"
#include "regDataObj.h"
#include "dataObjClose.h"
#include "dataObjRepl.h"
#include "rcMisc.h"

#include "dataObjCreateAndStat.h"
#include "fileCreate.h"
#include "subStructFileCreate.h"
#include "specColl.hpp"
#include "dataObjUnlink.h"
#include "regDataObj.h"
#include "rcGlobalExtern.h"
#include "getRemoteZoneResc.h"
#include "getRescQuota.h"
#include "icatHighLevelRoutines.hpp"
#include "rsObjStat.hpp"
#include "rsRegDataObj.hpp"
#include "rsDataObjUnlink.hpp"
#include "rsSubStructFileCreate.hpp"
#include "rsFileCreate.hpp"
#include "rsGetRescQuota.hpp"
#include "rsUnregDataObj.hpp"
#include "rsModDataObjMeta.hpp"

// =-=-=-=-=-=-=-
#include "irods_resource_backport.hpp"
#include "irods_resource_redirect.hpp"
#include "irods_hierarchy_parser.hpp"
#include "irods_logger.hpp"
#include "irods_stacktrace.hpp"
#include "irods_server_properties.hpp"
#include "irods_server_api_call.hpp"
#include "irods_at_scope_exit.hpp"
#include "key_value_proxy.hpp"

#include "boost/format.hpp"

namespace ix = irods::experimental;
using logger = irods::experimental::log;

namespace {

int register_intermediate_replica(
    rsComm_t* _comm,
    dataObjInp_t* _inp,
    const char* _path) {
    if (!getValByKey(&_inp->condInput, REGISTER_AS_INTERMEDIATE_KW)) {
        addKeyVal(&_inp->condInput, REGISTER_AS_INTERMEDIATE_KW, "");
    }
    addKeyVal(&_inp->condInput, FILE_PATH_KW, _path);
    addKeyVal(&_inp->condInput, DATA_SIZE_KW, "0");
    return rsPhyPathReg(_comm, _inp);
}

int l3CreateByObjInfo(
    rsComm_t* rsComm,
    dataObjInp_t* dataObjInp,
    dataObjInfo_t* dataObjInfo ) {

    int chkType = getchkPathPerm( rsComm, dataObjInp, dataObjInfo );
    if ( chkType == DISALLOW_PATH_REG ) {
        return PATH_REG_NOT_ALLOWED;
    }

    fileCreateInp_t fileCreateInp{};
    rstrcpy(fileCreateInp.resc_name_,    dataObjInfo->rescName, MAX_NAME_LEN);
    rstrcpy(fileCreateInp.resc_hier_,    dataObjInfo->rescHier, MAX_NAME_LEN);
    rstrcpy(fileCreateInp.objPath,       dataObjInfo->objPath,  MAX_NAME_LEN);
    rstrcpy(fileCreateInp.fileName,      dataObjInfo->filePath, MAX_NAME_LEN);
    rstrcpy(fileCreateInp.in_pdmo,       dataObjInfo->in_pdmo,  MAX_NAME_LEN );
    fileCreateInp.mode = getFileMode(dataObjInp);
    copyKeyVal(&dataObjInfo->condInput, &fileCreateInp.condInput);

    if ( chkType == NO_CHK_PATH_PERM ) {
        fileCreateInp.otherFlags |= NO_CHK_PERM_FLAG;
    }

    //loop until we find a valid filename
    int retryCnt = 0;
    int l3descInx;
    do {
        fileCreateOut_t* create_out{};
        l3descInx = rsFileCreate(rsComm, &fileCreateInp, &create_out);

        // update the dataObjInfo with the potential changes made by the resource - hcj
        if (create_out) {
            rstrcpy(dataObjInfo->rescHier, fileCreateInp.resc_hier_, MAX_NAME_LEN);
            rstrcpy(dataObjInfo->filePath, create_out->file_name, MAX_NAME_LEN);
            free(create_out);
        }

        //update the filename in case of a retry
        rstrcpy(fileCreateInp.fileName, dataObjInfo->filePath, MAX_NAME_LEN);
        retryCnt++;
    }
    while ( l3descInx < 0 && getErrno( l3descInx ) == EEXIST &&
            resolveDupFilePath( rsComm, dataObjInfo, dataObjInp ) >= 0 &&
            l3descInx <= 2 && retryCnt < 100 );
    clearKeyVal( &fileCreateInp.condInput );
    return l3descInx;
}

int create_sub_struct_file(
    rsComm_t *rsComm,
    const int l1descInx) {
    dataObjInfo_t *dataObjInfo = L1desc[l1descInx].dataObjInfo;
    std::string location{};
    irods::error ret = irods::get_loc_for_hier_string( dataObjInfo->rescHier, location );
    if (!ret.ok()) {
        irods::log(PASSMSG((boost::format("%s - failed in get_loc_for_hier_string") %
            __FUNCTION__).str(), ret));
        return ret.code();
    }

    subFile_t subFile{};
    rstrcpy( subFile.subFilePath, dataObjInfo->subPath, MAX_NAME_LEN );
    rstrcpy( subFile.addr.hostAddr, location.c_str(), NAME_LEN );

    subFile.specColl = dataObjInfo->specColl;
    subFile.mode = getFileMode( L1desc[l1descInx].dataObjInp );
    return rsSubStructFileCreate( rsComm, &subFile );
}

int l3Create(
    rsComm_t *rsComm,
    const int l1descInx) {
    dataObjInfo_t *dataObjInfo = L1desc[l1descInx].dataObjInfo;
    if (getStructFileType(dataObjInfo->specColl) >= 0) {
        return create_sub_struct_file(rsComm, l1descInx);    
    }
    /* normal or mounted file */
    return l3CreateByObjInfo(rsComm, L1desc[l1descInx].dataObjInp, L1desc[l1descInx].dataObjInfo);
}

int specCollSubCreate(
    rsComm_t* rsComm,
    dataObjInp_t& dataObjInp) {
    dataObjInfo_t* dataObjInfo{};
    int status = resolvePathInSpecColl( rsComm, dataObjInp.objPath,
                                    WRITE_COLL_PERM, 0, &dataObjInfo );
    if (!dataObjInfo) {
        rodsLog(LOG_ERROR,
                "%s :: dataObjInfo is null",
                __FUNCTION__ );
        return status;
    }
    if (status >= 0) {
        rodsLog(LOG_ERROR,
                "%s: phyPath %s already exist",
                __FUNCTION__, dataObjInfo->filePath );
        freeDataObjInfo( dataObjInfo );
        return SYS_COPY_ALREADY_IN_RESC;
    }
    else if (status != SYS_SPEC_COLL_OBJ_NOT_EXIST) {
        freeDataObjInfo( dataObjInfo );
        return status;
    }

    int l1descInx = allocL1desc();
    if (l1descInx < 0) {
        return l1descInx;
    }

    dataObjInfo->replStatus = INTERMEDIATE_REPLICA;
    fillL1desc(l1descInx, &dataObjInp, dataObjInfo, dataObjInfo->replStatus, dataObjInp.dataSize);

    status = l3Create( rsComm, l1descInx );
    if ( status <= 0 ) {
        rodsLog( LOG_NOTICE,
                 "%s: l3Create of %s failed, status = %d",
                 __FUNCTION__, L1desc[l1descInx].dataObjInfo->filePath, status);
        freeL1desc(l1descInx);
        return status;
    }
    L1desc[l1descInx].l3descInx = status;
    return l1descInx;
}

int create_new_replica(
    rsComm_t* rsComm,
    dataObjInp_t& dataObjInp)
{
    rodsObjStat_t* rodsObjStatOut{};
    const irods::at_scope_exit free_obj_stat_out{
        [&rodsObjStatOut]() {
            freeRodsObjStat(rodsObjStatOut);
        }
    };

    addKeyVal(&dataObjInp.condInput, SEL_OBJ_TYPE_KW, "dataObj" );
    int status = rsObjStat( rsComm, &dataObjInp, &rodsObjStatOut );
    if (status < 0) {
        rodsLog(LOG_DEBUG, "[%s:%d] - rsObjStat failed with [%d]", __FUNCTION__, __LINE__, status);
    }

    if (rodsObjStatOut) {
        if (COLL_OBJ_T == rodsObjStatOut->objType) {
            return USER_INPUT_PATH_ERR;
        }

        if (rodsObjStatOut->specColl) {
            // Linked collection should have been translated by this point
            if (LINKED_COLL == rodsObjStatOut->specColl->collClass) {
                return SYS_COLL_LINK_PATH_ERR;
            }

            if (UNKNOWN_OBJ_T == rodsObjStatOut->objType) {
                return specCollSubCreate( rsComm, dataObjInp );
            }
        }
    }

    addKeyVal(&dataObjInp.condInput, OPEN_TYPE_KW, std::to_string(CREATE_TYPE).c_str());
    int l1descInx = allocL1desc();
    if (l1descInx < 0) {
        return l1descInx;
    }

    dataObjInfo_t* dataObjInfo = (dataObjInfo_t*)malloc(sizeof(dataObjInfo_t));
    initDataObjInfoWithInp(dataObjInfo, &dataObjInp);

    const char* resc_hier = getValByKey(&dataObjInp.condInput, RESC_HIER_STR_KW);
    if (resc_hier) {
        // we need to favor the results from the PEP acSetRescSchemeForCreate
        std::string root = irods::hierarchy_parser{resc_hier}.first_resc();
        rstrcpy( dataObjInfo->rescName, root.c_str(), NAME_LEN );
        rstrcpy( dataObjInfo->rescHier, resc_hier, MAX_NAME_LEN );
    }

    irods::error ret = resc_mgr.hier_to_leaf_id(dataObjInfo->rescHier, dataObjInfo->rescId);
    if(!ret.ok()) {
        irods::log(PASS(ret));
        return ret.code();
    }

    dataObjInfo->replStatus = INTERMEDIATE_REPLICA;
    fillL1desc(l1descInx, &dataObjInp, dataObjInfo, dataObjInfo->replStatus, dataObjInp.dataSize);

    status = getFilePathName(rsComm, dataObjInfo, L1desc[l1descInx].dataObjInp);
    if ( status < 0 ) {
        freeL1desc( l1descInx );
        return status;
    }

    status = register_intermediate_replica(rsComm, L1desc[l1descInx].dataObjInp, dataObjInfo->filePath);
    if (status < 0) {
        freeL1desc(l1descInx);
        return status;
    }

    if (getValByKey(&dataObjInp.condInput, NO_OPEN_FLAG_KW)) {
        return l1descInx;
    }

    status = l3Create(rsComm, l1descInx);
    if (status < 0) {
        rodsLog(LOG_NOTICE,
                "%s: l3Create of %s failed, status = %d",
                __FUNCTION__, L1desc[l1descInx].dataObjInfo->filePath, status );
        dataObjInfo_t* data_obj_info = L1desc[l1descInx].dataObjInfo;
        const int unlink_status = dataObjUnlinkS(rsComm, L1desc[l1descInx].dataObjInp, data_obj_info);
        if (unlink_status < 0) {
            irods::log(ERROR(unlink_status,
                (boost::format("dataObjUnlinkS failed for [%s] with [%d]") %
                data_obj_info->filePath % unlink_status).str()));
        }
        freeL1desc(l1descInx);
        return status;
    }
    L1desc[l1descInx].l3descInx = status;
    return l1descInx;
} // create_new_replica

int stageBundledData( rsComm_t * rsComm, dataObjInfo_t **subfileObjInfoHead ) {
    dataObjInfo_t *dataObjInfoHead = *subfileObjInfoHead;
    char* cacheRescName{};
    int status = unbunAndStageBunfileObj(
                    rsComm,
                    dataObjInfoHead->filePath,
                    &cacheRescName);
    if ( status < 0 ) {
        return status;
    }

    /* query the bundle dataObj */
    dataObjInp_t dataObjInp{};
    addKeyVal( &dataObjInp.condInput, RESC_NAME_KW, cacheRescName );
    rstrcpy( dataObjInp.objPath, dataObjInfoHead->objPath, MAX_NAME_LEN );

    dataObjInfo_t* cacheObjInfo{};
    status = getDataObjInfo( rsComm, &dataObjInp, &cacheObjInfo, NULL, 0 );
    clearKeyVal( &dataObjInp.condInput );
    if ( status < 0 ) {
        rodsLog( LOG_ERROR,
                 "%s: getDataObjInfo of subfile %s failed.stat=%d",
                 __FUNCTION__, dataObjInp.objPath, status );
        return status;
    }
    /* que the cache copy at the top */
    queDataObjInfo( subfileObjInfoHead, cacheObjInfo, 0, 1 );
    return status;
}

int l3Open(
    rsComm_t *rsComm,
    int l1descInx)
{
    dataObjInfo_t* dataObjInfo = L1desc[l1descInx].dataObjInfo;
    if (!dataObjInfo) {
        return SYS_INTERNAL_NULL_INPUT_ERR;
    }

    std::string location{};
    irods::error ret = irods::get_loc_for_hier_string( dataObjInfo->rescHier, location );
    if ( !ret.ok() ) {
        irods::log( PASSMSG( "l3Open - failed in get_loc_for_hier_string", ret ) );
        return ret.code();
    }

    if ( getStructFileType( dataObjInfo->specColl ) >= 0 ) {
        subFile_t subFile{};
        rstrcpy( subFile.subFilePath, dataObjInfo->subPath, MAX_NAME_LEN );
        rstrcpy( subFile.addr.hostAddr, location.c_str(), NAME_LEN );
        subFile.specColl = dataObjInfo->specColl;
        subFile.mode = getFileMode( L1desc[l1descInx].dataObjInp );
        subFile.flags = getFileFlags( l1descInx );
        return rsSubStructFileOpen( rsComm, &subFile );
    }

    fileOpenInp_t fileOpenInp{};
    rstrcpy( fileOpenInp.resc_name_, dataObjInfo->rescName, MAX_NAME_LEN );
    rstrcpy( fileOpenInp.resc_hier_, dataObjInfo->rescHier, MAX_NAME_LEN );
    rstrcpy( fileOpenInp.objPath,    dataObjInfo->objPath, MAX_NAME_LEN );
    rstrcpy( fileOpenInp.addr.hostAddr,  location.c_str(), NAME_LEN );
    rstrcpy( fileOpenInp.fileName, dataObjInfo->filePath, MAX_NAME_LEN );
    fileOpenInp.mode = getFileMode(L1desc[l1descInx].dataObjInp);
    fileOpenInp.flags = getFileFlags(l1descInx);
    rstrcpy( fileOpenInp.in_pdmo, dataObjInfo->in_pdmo, MAX_NAME_LEN );

    copyKeyVal(&dataObjInfo->condInput, &fileOpenInp.condInput);

    const int l3descInx = rsFileOpen(rsComm, &fileOpenInp);
    clearKeyVal( &fileOpenInp.condInput );
    return l3descInx;
} // l3Open

int open_with_obj_info(
    rsComm_t* rsComm,
    dataObjInp_t& dataObjInp,
    dataObjInfo_t* dataObjInfo)
{
    int l1descInx = allocL1desc();
    if (l1descInx < 0) {
        return l1descInx;
    }

    copyKeyVal(&dataObjInp.condInput, &dataObjInfo->condInput);

    /* the size was set to -1 because we don't know the target size.
     * For copy and replicate, the calling routine should modify this
     * dataSize */
    fillL1desc(l1descInx, &dataObjInp, dataObjInfo, dataObjInfo->replStatus, -1);

    if (getValByKey(&dataObjInp.condInput, NO_OPEN_FLAG_KW)) {
        /* don't actually physically open the file */
        return l1descInx;
    }

    if (getValByKey(&dataObjInp.condInput, PHYOPEN_BY_SIZE_KW)) {
        int single_buff_sz;
        try {
            single_buff_sz = irods::get_advanced_setting<const int>(irods::CFG_MAX_SIZE_FOR_SINGLE_BUFFER) * 1024 * 1024;
        } catch (const irods::exception& e) {
            irods::log(e);
            return e.code();
        }

        /* open for put or get. May do "dataInclude" */
        if (dataObjInfo->dataSize <= single_buff_sz &&
            (getValByKey(&dataObjInp.condInput, DATA_INCLUDED_KW) ||
             dataObjInfo->dataSize != UNKNOWN_FILE_SZ)) {
            return l1descInx;
        }
    }

    int status = l3Open(rsComm, l1descInx);
    if (status <= 0) {
        rodsLog(LOG_NOTICE, "%s: l3Open of %s failed, status = %d",
                __FUNCTION__, dataObjInfo->filePath, status);
        freeL1desc( l1descInx );
        return status;
    }

    auto& fd = L1desc[l1descInx];
    fd.l3descInx = status;

    // Set the size of the data object to zero in the catalog if the file was truncated.
    // It is important that the catalog reflect truncation immediately because operations
    // following the open may depend on the size of the data object.
    if (fd.dataObjInp->openFlags & O_TRUNC) {
        if (const auto access_mode = (fd.dataObjInp->openFlags & O_ACCMODE);
            access_mode == O_WRONLY || access_mode == O_RDWR)
        {
            dataObjInfo_t info{};
            rstrcpy(info.objPath, fd.dataObjInp->objPath, MAX_NAME_LEN);
            rstrcpy(info.rescHier, fd.dataObjInfo->rescHier, MAX_NAME_LEN);

            keyValPair_t kvp{};
            addKeyVal(&kvp, DATA_SIZE_KW, "0");
            if (getValByKey(&dataObjInp.condInput, ADMIN_KW)) {
                addKeyVal(&kvp, ADMIN_KW, "");
            }

            modDataObjMeta_t input{};
            input.dataObjInfo = &info;
            input.regParam = &kvp;

            if (const auto ec = rsModDataObjMeta(rsComm, &input); ec != 0) {
                logger::api::error("dataOpen: Could not update size of data object [status = {}, path = {}]",
                                   ec, dataObjInp.objPath);
                return ec;
            }

            fd.dataSize = 0;

            if (fd.dataObjInfo) {
                fd.dataObjInfo->dataSize = 0;
            }
        }
    }

    return l1descInx;
} // open_with_obj_info

int applyPreprocRuleForOpen(
    rsComm_t* rsComm,
    dataObjInp_t* dataObjInp,
    dataObjInfo_t** dataObjInfoHead)
{
    ruleExecInfo_t rei;
    initReiWithDataObjInp( &rei, rsComm, dataObjInp );
    rei.doi = *dataObjInfoHead;

    // make resource properties available as rule session variables
    irods::get_resc_properties_as_kvp(rei.doi->rescHier, rei.condInputData);

    int status = applyRule( "acPreprocForDataObjOpen", NULL, &rei, NO_SAVE_REI );
    clearKeyVal(rei.condInputData);
    free(rei.condInputData);

    if ( status < 0 ) {
        if ( rei.status < 0 ) {
            status = rei.status;
        }
        rodsLog( LOG_ERROR,
                 "%s:acPreprocForDataObjOpen error for %s,stat=%d",
                 __FUNCTION__, dataObjInp->objPath, status );
    }
    else {
        *dataObjInfoHead = rei.doi;
    }
    return status;
} // applyPreprocRuleForOpen

int change_replica_status_to_intermediate(
    rsComm_t* rsComm,
    dataObjInp_t& dataObjInp,
    dataObjInfo_t* dataObjInfo)
{
    using namespace irods::experimental;
    keyValPair_t kvp{};
    replKeyVal(&dataObjInp.condInput, &kvp);
    key_value_proxy proxy{kvp};
    proxy[REPL_STATUS_KW] = std::to_string(INTERMEDIATE_REPLICA);
    proxy.erase(ALL_KW);
    proxy.erase(OPEN_TYPE_KW);
    //proxy[IN_PDMO_KW] = dataObjInfo->rescHier;

    modDataObjMeta_t inp{};
    inp.dataObjInfo = dataObjInfo;
    inp.regParam = proxy.get();
    const int status = rsModDataObjMeta(rsComm, &inp);
    if (status < 0) {
        rodsLog(LOG_ERROR,
            "[%s] - rsModDataObjMeta failed with [%d] when modifying [%s] replica [%d]",
            __FUNCTION__, status, dataObjInp.objPath, dataObjInfo->replNum);
    }
    return status;
} // change_replica_status_to_intermediate

} // anonymous namespace

int rsDataObjOpen(
    rsComm_t *rsComm,
    dataObjInp_t *dataObjInp)
{
    if (!dataObjInp) {
        return SYS_INTERNAL_NULL_INPUT_ERR;
    }

    if (has_trailing_path_separator(dataObjInp->objPath)) {
        return USER_INPUT_PATH_ERR;
    }

    if ((dataObjInp->openFlags & O_ACCMODE) == O_RDONLY && (dataObjInp->openFlags & O_TRUNC)) {
        return USER_INCOMPATIBLE_OPEN_FLAGS;
    }

    rodsServerHost_t* rodsServerHost{};
    int remoteFlag = getAndConnRemoteZone(rsComm, dataObjInp, &rodsServerHost, REMOTE_OPEN);
    if (remoteFlag < 0) {
        return remoteFlag;
    }
    else if (REMOTE_HOST == remoteFlag) {
        openStat_t* stat{};
        const int status = rcDataObjOpenAndStat(rodsServerHost->conn, dataObjInp, &stat);
        if (status < 0) {
            return status;
        }
        const int l1descInx = allocAndSetL1descForZoneOpr(status, dataObjInp, rodsServerHost, stat);
        if (stat) {
            free(stat);
        }
        return l1descInx;
    }

    auto cond_input = ix::make_key_value_proxy(dataObjInp->condInput);

    try {
        // Get replica information for data object, resolving hierarchy if necessary
        dataObjInfo_t* dataObjInfoHead{};
        irods::file_object_ptr file_obj(new irods::file_object());
        if (!cond_input.contains(RESC_HIER_STR_KW)) {
            std::string hier{};
            const auto operation = (dataObjInp->openFlags & O_CREAT) ?
                irods::CREATE_OPERATION : irods::OPEN_OPERATION;
            std::tie(file_obj, hier) = irods::resolve_resource_hierarchy(operation, rsComm, *dataObjInp, &dataObjInfoHead);
            cond_input[RESC_HIER_STR_KW] = hier;
        }
        else {
            irods::file_object_ptr obj(new irods::file_object());
            irods::error fac_err = irods::file_object_factory(rsComm, dataObjInp, obj, &dataObjInfoHead);
            if (!fac_err.ok()) {
                irods::log(fac_err);
            }
            file_obj.swap(obj);
        }

        int lockFd = -1;
        if (cond_input.contains(LOCK_TYPE_KW) && cond_input.at(LOCK_TYPE_KW).value().data()) {
            rodsLog(LOG_NOTICE, "[%s:%d] - locking file with type [%s]",
                __FUNCTION__, __LINE__, getValByKey(&dataObjInp->condInput, LOCK_TYPE_KW));
            lockFd = irods::server_api_call(
                         DATA_OBJ_LOCK_AN,
                         rsComm, dataObjInp,
                         NULL, (void**)NULL, NULL);

            if (lockFd <= 0) {
                rodsLog(LOG_ERROR, "%s: lock error for %s. lockType = %s, lockFd: %d",
                        __FUNCTION__, dataObjInp->objPath, cond_input.at(LOCK_TYPE_KW).value().data(), lockFd );
                return lockFd;
            }

            /* rm it so it won't be done again causing deadlock */
            cond_input.erase(LOCK_TYPE_KW);
        }

        const auto unlock_data_obj{[&]() {
            char fd_string[NAME_LEN]{};
            snprintf( fd_string, sizeof( fd_string ), "%-d", lockFd );
            cond_input[LOCK_FD_KW] = fd_string;
            irods::server_api_call(
                DATA_OBJ_UNLOCK_AN,
                rsComm,
                dataObjInp,
                NULL,
                ( void** ) NULL,
                NULL );
        }};

        // Determine if this is a replica creation and do so
        const int writeFlag = getWriteFlag(dataObjInp->openFlags);
        if (dataObjInp->openFlags & O_CREAT && writeFlag > 0) {
            const auto hier_has_replica{
                [&cond_input, &replicas = file_obj->replicas()]()
                {
                    return std::any_of(replicas.begin(), replicas.end(),
                        [&](const irods::physical_object& replica) {
                            return replica.resc_hier() == cond_input.at(RESC_HIER_STR_KW);
                        });
                }()};

            if (!hier_has_replica) {
                const int l1descInx = create_new_replica(rsComm, *dataObjInp);
                if ( lockFd >= 0 ) {
                    if ( l1descInx > 0 ) {
                        L1desc[l1descInx].lockFd = lockFd;
                    }
                    else {
                        unlock_data_obj();
                    }
                }
                return l1descInx;
            }

            // This is an overwrite - swizzle some flags
            dataObjInp->openFlags |= O_TRUNC | O_RDWR;
            const auto hier = cond_input.at(RESC_HIER_STR_KW).value();
            cond_input[DEST_RESC_NAME_KW] = irods::hierarchy_parser{hier.data()}.first_resc();
            cond_input[OPEN_TYPE_KW] = std::to_string(OPEN_FOR_WRITE_TYPE);
        }

        // sort replica list based on some set of criteria
        int status = sortObjInfoForOpen(&dataObjInfoHead, cond_input.get(), writeFlag);
        if (status < 0) {
            std::stringstream msg;
            msg << __FUNCTION__;
            msg << " - Unable to select a data obj info matching the resource hierarchy from the keywords.";
            irods::log(ERROR(status, msg.str()));
            if (lockFd > 0) {
                unlock_data_obj();
            }
            return status;
        }

        // acPreProcForOpen
        status = applyPreprocRuleForOpen( rsComm, dataObjInp, &dataObjInfoHead );
        if (status < 0) {
            if (lockFd > 0) {
                unlock_data_obj();
            }
            return status;
        }

        // reshuffling dataObjInfo based on provided resource keywords (shouldn't this be done in voting?)
        if ( getStructFileType( dataObjInfoHead->specColl ) < 0 && writeFlag > 0 ) {
            const std::vector<const char*> kw{DEST_RESC_NAME_KW, BACKUP_RESC_NAME_KW, DEF_RESC_NAME_KW};

            const auto resc_name = std::find_first_of(
                std::begin(cond_input), std::end(cond_input),
                std::begin(kw),         std::end(kw));

            if (std::end(cond_input) != resc_name) {
                status = requeDataObjInfoByResc(&dataObjInfoHead, (*resc_name).value().data(), writeFlag, 1);
            }

            if ( status < 0 ) {
                if (lockFd > 0) {
                    unlock_data_obj();
                }
                freeAllDataObjInfo( dataObjInfoHead );
                return status;
            }
        }

        // Stage bundled data to cache directory, if necessary
        std::string resc_class{};
        irods::error prop_err = irods::get_resource_property<std::string>(
                                    dataObjInfoHead->rescId, irods::RESOURCE_CLASS, resc_class);
        if (prop_err.ok() && resc_class == irods::RESOURCE_CLASS_BUNDLE) {
            status = stageBundledData(rsComm, &dataObjInfoHead);
            if ( status < 0 ) {
                rodsLog( LOG_ERROR,
                         "%s: stageBundledData of %s failed stat=%d",
                         __FUNCTION__, dataObjInfoHead->objPath, status );
                if (lockFd > 0) {
                    unlock_data_obj();
                }
                freeAllDataObjInfo( dataObjInfoHead );
                return status;
            }
        }

        // open replica
        dataObjInfo_t* tmpDataObjInfo = dataObjInfoHead;
        tmpDataObjInfo->next = NULL;
        int l1descInx = open_with_obj_info(rsComm, *dataObjInp, tmpDataObjInfo);
        if (l1descInx < 0) {
            if (lockFd > 0) {
                unlock_data_obj();
            }
            return l1descInx;
        }

        // lock data object
        //irods::experimental::data_object::lock(rsComm, dataObjInp, l1descInx);
        if (writeFlag > 0) {
            status = change_replica_status_to_intermediate(rsComm, *dataObjInp, tmpDataObjInfo);
            if (status < 0) {
                if (lockFd > 0) {
                    unlock_data_obj();
                }
                THROW(status, "failed to change replica status");
            }
        }
        L1desc[l1descInx].openType = writeFlag ? OPEN_FOR_WRITE_TYPE : OPEN_FOR_READ_TYPE;
        return l1descInx;
    }
    catch (const irods::exception& e) {
        rodsLog(LOG_ERROR, "[%s] - resolve_resource_hierarchy failed with [%d] when opening [%s]",
                __FUNCTION__, e.code(), dataObjInp->objPath);
        return e.code();
    }
} // rsDataObjOpen

