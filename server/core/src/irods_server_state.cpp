#include "rodsLog.h"
#include "irods_server_state.hpp"

namespace irods {
    server_state_mgr::server_state_mgr()
    {
        using p = server_process_t;
        using s = server_state_t;
        std::unique_lock<std::mutex> l{mutex_};
        state_[p::irods_server] = s::RUNNING;
        state_[p::agent_spawner] = s::RUNNING;
        state_[p::xmsg_server] = s::RUNNING;
        state_[p::re_server] = s::RUNNING;
    }

    server_state_mgr& server_state_mgr::instance() {
        static server_state_mgr instance_;
        return instance_;
    }

    void server_state_mgr::server_state(
        const server_state_t s,
        const server_process_t p)
    {
        std::unique_lock<std::mutex> l{mutex_};
        rodsLog(LOG_NOTICE, "[%s:%d] - setting server state for [%d]:[%d]",
            __FUNCTION__,
            __LINE__,
            p,
            s);
        state_[p] = s;
    }

    server_state_t server_state_mgr::server_state(
        const server_process_t p)
    {
        std::unique_lock<std::mutex> l{mutex_};
        rodsLog(LOG_NOTICE, "[%s:%d] - getting server state for [%d]:[%d]",
            __FUNCTION__,
            __LINE__,
            p,
            state_[p]);
        return state_[p];
    }

    server_state_t get_server_state(
        const server_process_t p)
    {
        return server_state_mgr::instance().server_state(p);
    }

    void stop_server(
        const server_process_t t)
    {
        server_state_mgr::instance().server_state(
            server_state_t::STOPPED, t);
    }

    void pause_server(
        const server_process_t t)
    {
        server_state_mgr::instance().server_state(
            server_state_t::PAUSED, t);
    }

    void resume_server(
        const server_process_t t)
    {
        server_state_mgr::instance().server_state(
            server_state_t::RUNNING, t);
    }
}; // namespace irods
