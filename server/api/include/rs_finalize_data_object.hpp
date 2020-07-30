#ifndef RS_FINALIZE_DATA_OBJECT_HPP
#define RS_FINALIZE_DATA_OBJECT_HPP

#include "rcConnect.h"
#include "rodsDef.h"

auto rs_finalize_data_object(
    rsComm_t* _comm,
    bytesBuf_t* _input,
    bytesBuf_t** _output) -> int;

#endif // #ifndef RS_FINALIZE_DATA_OBJECT_HPP
