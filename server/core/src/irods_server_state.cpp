

#include "rodsErrorTable.h"
#include "irods_server_state.hpp"

#include <algorithm>
#include <vector>

namespace {
    const std::vector<std::string> server_states{
        irods::server_state::RUNNING,
        irods::server_state::PAUSED,
        irods::server_state::STOPPED,
        irods::server_state::EXITED};
}

namespace irods {

    const std::string server_state::RUNNING( "server_state_running" );
    const std::string server_state::PAUSED( "server_state_paused" );
    const std::string server_state::STOPPED( "server_state_stopped" );
    const std::string server_state::EXITED( "server_state_exited" );

    server_state::server_state()
        : state_{RUNNING}
    {
    }

    server_state& server_state::instance() {
        static server_state instance_;
        return instance_;
    }

    error server_state::operator()(const std::string& s) {
        std::unique_lock<std::mutex> l{mutex_};
        if (std::none_of(
                server_states.cbegin(),
                server_states.cend(),
                [&s](const std::string& state) { return s == state; })) {
            std::string msg( "invalid state [" );
            msg += s;
            msg += "]";
            return ERROR(SYS_INVALID_INPUT_PARAM, msg);
        }
        state_ = s;
        return SUCCESS();
    }

    std::string server_state::operator()() {
        std::unique_lock<std::mutex> l{mutex_};
        return state_;
    }


}; // namespace irods




