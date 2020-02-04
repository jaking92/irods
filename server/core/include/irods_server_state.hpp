#ifndef IRODS_SERVER_STATE_HPP
#define IRODS_SERVER_STATE_HPP

#include "irods_error.hpp"
#include <map>
#include <mutex>

namespace irods {
    enum class server_process_t {
        irods_server,
        agent_spawner,
        xmsg_server,
        re_server
    };

    enum class server_state_t {
        RUNNING,
        PAUSED,
        STOPPED,
        EXITED
    };

    class server_state_mgr {
        public:
            server_state_mgr(server_state_mgr&) = delete;
            server_state_mgr(const server_state_mgr&) = delete;

            static server_state_mgr& instance();
            void server_state(
                const server_state_t s,
                const server_process_t p = server_process_t::irods_server);
            server_state_t server_state(
                const server_process_t p = server_process_t::irods_server);

        private:
            server_state_mgr();

            std::mutex mutex_;
            std::map<server_process_t, server_state_t> state_;

    }; // class server_state_mgr

    auto get_server_state(const server_process_t p = server_process_t::irods_server) -> server_state_t;

}; // namespace irods

#endif // IRODS_SERVER_STATE_HPP
