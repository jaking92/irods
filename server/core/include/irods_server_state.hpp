#ifndef IRODS_SERVER_STATE_HPP
#define IRODS_SERVER_STATE_HPP

#include "irods_error.hpp"
#include <map>
#include <mutex>

namespace irods {
    /// \class server_process_t
    ///
    /// \brief Enumeration epresenting different iRODS server processes
    ///
    /// NOTE: irods_server is the original server process from which come the other server processes.
    /// iRODS agent processes are not represented as a member of this enumeration.
    ///
    /// \since 4.2.8
    enum class server_process_t {
        irods_server,
        agent_spawner,
        xmsg_server,
        re_server
    };

    /// \class server_state_t
    ///
    /// \brief Enumeration representing different states for an iRODS server process
    ///
    /// \since 4.2.8
    enum class server_state_t {
        RUNNING,
        PAUSED,
        STOPPED,
        EXITED
    };

    /// \class server_state_mgr
    ///
    /// \brief Manager of the state of various iRODS server processes.
    ///
    /// This Singleton maintains a map of iRODS server processes and their states.
    /// The server_state_mgr should only be used by iRODS server processes and the server control plane.
    ///
    /// \since 4.2.8
    class server_state_mgr {
        public:
            // Disables copy
            server_state_mgr(server_state_mgr&) = delete;
            server_state_mgr(const server_state_mgr&) = delete;

            /// \fn static server_state_mgr& instance()
            ///
            /// \brief Gets/Creates the static global instance of the Singleton
            ///
            /// \since 4.2.8
            static server_state_mgr& instance();

            /// \fn void server_state(const server_state_t, const server_process_t p)
            ///
            /// \brief Sets the state of the server process p to state s
            ///
            /// \param[in] s - Server state to which the server process will be set.
            /// \param[in] p - Server process being targeted. Defaults to irods_server.
            ///
            /// \since 4.2.8
            void server_state(
                const server_state_t s,
                const server_process_t p = server_process_t::irods_server);

            /// \fn void server_state(const server_process_t p)
            ///
            /// \brief Returns the state of the server process p
            ///
            /// \param[in] p - Server process being targeted. Defaults to irods_server.
            ///
            /// \returns server_state_t
            /// \retval enumerated state of the server process
            ///
            /// \since 4.2.8
            server_state_t server_state(
                const server_process_t p = server_process_t::irods_server);

        private:
            /// \fn server_state_mgr()
            ///
            /// \brief Default constructor for server_state_mgr
            ///
            /// Initiates all server process states to RUNNING.
            ///
            /// \since 4.2.8
            server_state_mgr();

            /// \var mutex_
            ///
            /// \brief Mutex for locking the server process state map
            ///
            /// \since 4.2.8
            std::mutex mutex_;

            /// \var state_
            ///
            /// \brief Map of iRODS server process and their states
            ///
            /// \since 4.2.8
            std::map<server_process_t, server_state_t> state_;

    }; // class server_state_mgr

    /// \fn server_state_t get_server_state(const server_process_t p = server_process_t::irods_server)
    ///
    /// \brief Convenience function for getting the state of the indicated server process.
    ///
    /// \param[in] p - Server process being targeted. Defaults to irods_server.
    ///
    /// \returns server_state_t
    /// \retval enumerated state of the server process
    ///
    /// \since 4.2.8
    auto get_server_state(const server_process_t p = server_process_t::irods_server) -> server_state_t;

    /// \fn void pause_server(const server_process_t p = server_process_t::irods_server)
    ///
    /// \brief Convenience function for setting the state of the indicated server process to PAUSED.
    ///
    /// \param[in] p - Server process being targeted. Defaults to irods_server.
    ///
    /// \since 4.2.8
    auto pause_server(const server_process_t p = server_process_t::irods_server) -> void;

    /// \fn void resume_server(const server_process_t p = server_process_t::irods_server)
    ///
    /// \brief Convenience function for setting the state of the indicated server process to RUNNING.
    ///
    /// \param[in] p - Server process being targeted. Defaults to irods_server.
    ///
    /// \since 4.2.8
    auto resume_server(const server_process_t p = server_process_t::irods_server) -> void;

    /// \fn void stop_server(const server_process_t p = server_process_t::irods_server)
    ///
    /// \brief Convenience function for setting the state of the indicated server process to STOPPED.
    ///
    /// \param[in] p - Server process being targeted. Defaults to irods_server.
    ///
    /// \since 4.2.8
    auto stop_server(const server_process_t p = server_process_t::irods_server) -> void;


}; // namespace irods

#endif // IRODS_SERVER_STATE_HPP
