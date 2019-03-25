#include "getRodsEnv.h"
#include "irods_error.hpp"
#include "irods_query.hpp"
#include "irods_user_info.hpp"
#include "rodsUser.h"
#include <boost/format.hpp>

namespace {
    std::string construct_userinfo_query_string(
        const userInfo_t& _info,
        const std::string& _select_string) {
        return std::string{(boost::format("SELECT %s WHERE USER_NAME = '%s' AND USER_ZONE = '%s' AND USER_TYPE %s 'rodsgroup'") %
                            _select_string % _info.userName % _info.rodsZone %
                            (0 == std::string{_info.userType}.compare("rodsgroup") ? "=" : "!=")).str()};
    }

    std::string get_user_general_info(
        rcComm_t* _comm,
        const userInfo_t& _info) {
        const std::string select_string{
            "USER_NAME, USER_ID, USER_TYPE, USER_ZONE, USER_INFO, USER_COMMENT, USER_CREATE_TIME, USER_MODIFY_TIME"};
        const std::string qstr{construct_userinfo_query_string(_info, select_string)};
        irods::query<rcComm_t> qobj{_comm, qstr};
        if (qobj.begin() == qobj.end()) {
            THROW(CAT_NO_ROWS_FOUND, (boost::format("[%s#%s] does not exist.") % _info.userName % _info.rodsZone).str());
        }

        int i{};
        std::string out{};
        const std::vector<std::string> general_info_labels{
            "name", "id", "type", "zone", "info", "comment", "create time", "modify time"};
        for (const auto& selections: *qobj.begin()) {
            if (std::string::npos != (general_info_labels[i].find("time"))) {
                char local_time[TIME_LEN]{};
                getLocalTimeFromRodsTime(selections.c_str(), local_time);
                out += std::string{(boost::format("%s: %s: %s\n") %
                                    general_info_labels[i] % selections % local_time).str()};
            }
            else {
                out += std::string{(boost::format("%s: %s\n") %
                                    general_info_labels[i] % selections).str()};
            }
            i++;
        }
        return out;
    }

    std::string get_user_auth_info(
        rcComm_t* _comm,
        const userInfo_t& _info) {
        irods::query<rcComm_t> qobj{_comm, construct_userinfo_query_string(_info, "USER_DN")};
        std::string out{};
        for (const auto& result: qobj) {
            out += (boost::format("GSI DN or Kerberos Principal Name: %s\n") % result[0]).str();
        }
        return out;
    }

    std::string get_user_group_info(
        rcComm_t* _comm,
        const userInfo_t& _info) {
        irods::query<rcComm_t> qobj{_comm, construct_userinfo_query_string(_info, "USER_GROUP_NAME")};
        if (qobj.begin() == qobj.end()) {
            return std::string{"Not a member of any group\n"};
        }
        std::string out{};
        for (const auto& result: qobj) {
            out += (boost::format("member of group: %s\n") % result[0]).str();
        }
        return out;
    }

    userInfo_t get_user_info_from_string(const std::string& _user_name_string) {
        userInfo_t info{};
        int status = parseUserName(_user_name_string.c_str(), info.userName, info.rodsZone);
        if (status < 0) {
            THROW(status, (boost::format("Failed parsing input:[%s]") % _user_name_string).str());
        }
        if (std::string(info.rodsZone).empty()) {
            rodsEnv env;
            status = getRodsEnv(&env);
            if (status < 0) {
                THROW(status, "Failed retrieving iRODS environment");
            }
            snprintf(info.rodsZone, sizeof(info.rodsZone), "%s", env.rodsZone);
        }
        return info;
    }

    std::string generate_user_list_string(
        rcComm_t* _comm,
        const std::string& _qstr) {
        irods::query<rcComm_t> qobj{_comm, _qstr};
        std::string out{};
        for(const auto& result: qobj) {
            out += (boost::format("%s#%s\n") % result[0] % result[1]).str();
        }
        return out;
    }

}

namespace irods {
    std::string get_printable_user_info_string(
        rcComm_t* _comm,
        const std::string& _user_name_string) {
        userInfo_t info{get_user_info_from_string(_user_name_string)};

        std::string out{get_user_general_info(_comm, info)};
        out += get_user_auth_info(_comm, info);
        out += get_user_group_info(_comm, info);
        return out;
    }

    std::string get_printable_group_info_string(
        rcComm_t* _comm,
        const std::string& _group_name_string) {
        userInfo_t info{get_user_info_from_string(_group_name_string)};
        rstrcpy(info.userType, "rodsgroup", sizeof(info.userType));
        return get_user_general_info(_comm, info);
    }

    std::string get_printable_user_list_string(rcComm_t* _comm) {
        return generate_user_list_string(_comm,
            "SELECT USER_NAME, USER_ZONE WHERE USER_TYPE != 'rodsgroup'");
    }

    std::string get_printable_group_list_string(rcComm_t* _comm) {
        return generate_user_list_string(_comm,
            "SELECT USER_NAME, USER_ZONE WHERE USER_TYPE = 'rodsgroup'");
    }

    std::string get_printable_group_member_list_string(
        rcComm_t* _comm,
        const std::string& _user_name_string) {
        userInfo_t info{get_user_info_from_string(_user_name_string)};
        std::string out{generate_user_list_string(_comm, (boost::format(
            "SELECT USER_NAME, USER_ZONE WHERE USER_TYPE != 'rodsgroup' AND USER_GROUP_NAME = '%s'") % info.userName).str())};
        if(out.empty()) {
            THROW(CAT_NO_ROWS_FOUND, (boost::format("No members in group %s#%s.") % info.userName % info.rodsZone).str());
        }
        return (boost::format("Members of group %s#%s:\n") % info.userName % info.rodsZone).str() + out;
    }
}
