#ifndef IRODS_DATA_OBJECT_FINALIZE_HPP
#define IRODS_DATA_OBJECT_FINALIZE_HPP

/// \file

struct RcComm;

#ifdef __cplusplus
extern "C" {
#endif

// TODO: doxygen
int rc_data_object_finalize(RcComm* _comm, const char* _json_input, char** _json_output);

#ifdef __cplusplus
} // extern "C"
#endif

#endif // IRODS_DATA_OBJECT_FINALIZE_HPP

