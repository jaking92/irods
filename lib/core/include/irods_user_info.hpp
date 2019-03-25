#ifndef IRODS_USER_INFO_HPP
#define IRODS_USER_INFO_HPP

#include "rcConnect.h"
#include <string>

namespace irods {
    std::string get_printable_user_info_string(rcComm_t* _comm, const std::string& _user_name_string);
    std::string get_printable_user_list_string(rcComm_t* _comm);
    std::string get_printable_group_info_string(rcComm_t* _comm, const std::string& _group_name_string);
    std::string get_printable_group_member_list_string(rcComm_t* _comm, const std::string& _user_name_string);
    std::string get_printable_group_list_string(rcComm_t* _comm);
}

#endif // IRODS_USER_INFO_HPP
