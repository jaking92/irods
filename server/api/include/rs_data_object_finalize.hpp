#ifndef RS_DATA_OBJECT_FINALIZE_HPP
#define RS_DATA_OBJECT_FINALIZE_HPP

/// \file

#include "rcConnect.h"
#include "rodsDef.h"

#ifdef __cplusplus
extern "C" {
#endif

// TODO: doxygen
auto rs_data_object_finalize(rsComm_t* _comm, const char* _json_input, char** _json_output) -> int;

#ifdef __cplusplus
} // extern "C"
#endif

#endif // #ifndef RS_DATA_OBJECT_FINALIZE_HPP

