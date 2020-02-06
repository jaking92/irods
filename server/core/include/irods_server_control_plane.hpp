#ifndef IRODS_SERVER_CONTROL_PLANE_HPP
#define IRODS_SERVER_CONTROL_PLANE_HPP

#include "irods_lookup_table.hpp"
#include "server_control_plane_command.hpp"
#include "zmq.hpp"

#include <functional>
#include <thread>
#include <unordered_map>

#include "boost/atomic.hpp"

namespace irods {
    // Commands
    const std::string SERVER_CONTROL_SHUTDOWN( "server_control_shutdown" );
    const std::string SERVER_CONTROL_PAUSE( "server_control_pause" );
    const std::string SERVER_CONTROL_RESUME( "server_control_resume" );
    const std::string SERVER_CONTROL_STATUS( "server_control_status" );
    const std::string SERVER_CONTROL_PING( "server_control_ping" );

    // Options
    const std::string SERVER_CONTROL_OPTION_KW( "server_control_option" );
    const std::string SERVER_CONTROL_HOST_KW( "server_control_host" );

    // Timeout options
    const std::string SERVER_CONTROL_FORCE_AFTER_KW( "server_control_force_after" );
    const std::string SERVER_CONTROL_WAIT_FOREVER_KW( "server_control_wait_forever" );

    // Host options
    const std::string SERVER_CONTROL_ALL_OPT( "all" );
    const std::string SERVER_CONTROL_HOSTS_OPT( "hosts" );

    // Status messages
    const std::string SERVER_CONTROL_SUCCESS( "server_control_success" );
    const std::string SERVER_PAUSED_ERROR( "The server is Paused, resume before issuing any other commands" );


    // this is a hand-chosen polling time for the control plane
    static const size_t SERVER_CONTROL_POLLING_TIME_MILLI_SEC = 500;

    // derived from above - used to wait for the server to shut down or resume
    static const size_t SERVER_CONTROL_FWD_SLEEP_TIME_MILLI_SEC = SERVER_CONTROL_POLLING_TIME_MILLI_SEC / 4.0;

    /// \class server_control_executor
    ///
    /// \brief Functor which controls execution of iRODS grid commands.
    ///
    /// \since 4.1.0
    class server_control_executor {
        public:
            /// \brief Function signature for a control plane operation
            typedef std::function<error(const std::string&, const size_t, std::string&)> ctrl_func_t;

            // Disables copy
            server_control_executor(const server_control_executor&) = delete;
            server_control_executor& operator=(const server_control_executor&) = delete;

            /// \brief Constructor
            ///
            /// \param[in] prop - Control plane port property from server config
            /// \param[in] op_map - Map of iRODS grid commands to functions
            server_control_executor(
                const std::string& prop,
                const std::unordered_map<std::string, ctrl_func_t>& op_map);

            /// \brief operator operator for use in the control thread
            void operator()();

        private:
            /// \brief Vector containing a list of hostnames
            typedef std::vector<std::string> host_list_t;

            /// \brief Holds parameters and name of an iRODS grid command
            struct grid_command {
                std::string cmd_name;
                std::string cmd_option;
                std::string wait_option;
                size_t wait_seconds;
            };

            // members
            /// \brief Process received ZMQ message as a control plane operation
            ///
            /// \param[in] msg - ZeroMQ message representing a grid command
            /// \param[out] output - ZeroMQ response message from grid command processing
            ///
            /// \returns irods::error
            error process_operation(
                const zmq::message_t& msg,
                std::string&          output);
            // TODO: throws and returns output
            //std::string process_operation(
                //const zmq::message_t& msg);

            /// \brief Extract various pieces of the iRODS grid command into out variables.
            ///
            /// \param[in] cmd - Avro-generated grid command from which paramaters are extracted
            /// \param[out] name - Grid command name
            /// \param[out] option - Grid command options
            /// \param[out] wait_option - "Wait for" option
            /// \param[out] wait_seconds - Number of seconds to wait
            /// \param[out] hosts - Host list option
            ///
            /// \returns irods::error
            error extract_command_parameters(
                const control_plane_command& cmd,
                std::string&                 name,
                std::string&                 option,
                std::string&                 wait_option,
                size_t&                      wait_seconds,
                host_list_t&                 hosts);
            // TODO: throws and returns a tuple
            //typedef std::tuple<std::string,
                               //std::string,
                               //std::string,
                               //size_t,
                               //host_list_t> command_parameters_t;
            //command_parameters_t server_control_executor::extract_command_parameters(
                //const control_plane_command& cmd);

            /// \brief Forward command to be processed by each host in the list
            ///
            /// \param[in] name - Grid command name
            /// \param[in] wait_option - "Wait for" option
            /// \param[in] wait_seconds - Number of seconds to wait
            /// \param[in] hosts - List of hosts to which command is sent
            /// \param[out] output - ZeroMQ response message from grid command processing
            ///
            /// \returns irods::error
            error process_host_list(
                const std::string& cmd_name,
                const std::string& wait_option,
                const size_t&      wait_seconds,
                const host_list_t& hosts,
                std::string&       output);
            //std::string server_control_executor::process_host_list(
                //const command_parameters_t& command_params);

            /// \brief Validate list of hosts
            ///
            /// \param[in] irods_hosts - List of known iRODS hosts
            /// \param[in] cmd_hosts - List of hosts passed as an option with a grid command
            /// \param[out] valid_hosts - List of target hosts which are valid iRODS hosts
            ///
            /// \returns irods::error
            error validate_host_list(
                const host_list_t& irods_hosts,
                const host_list_t& cmd_hosts,
                host_list_t&       valid_hosts);

            // TODO: throw and return list of valid hosts
            //host_list_t validate_host_list(
                //const host_list_t& irods_hosts,
                //const host_list_t& cmd_hosts)

            /// \brief Gets the full list of catalog consumer hostnames
            ///
            /// \param[out] host_names - List of catalog consumer hosts
            ///
            /// \returns irods::error
            error get_resource_host_names(
                host_list_t& host_names);

            // TODO: throw and return list
            //host_list_t get_resource_host_names();

            /// \brief Grid command pre-operation
            ///
            /// Sends grid command to certain iRODS servers in the grid in a certain
            /// order before the actual grid command is run on the target hosts.
            ///
            /// \param[in] name - Grid command name
            /// \param[in] option - Grid command options
            /// \param[in] wait_option - "Wait for" option
            /// \param[in] wait_seconds - Number of seconds to wait
            /// \param[in] hosts - List of hosts to which command is sent
            /// \param[out] output - ZeroMQ response message from grid command processing
            ///
            /// \returns irods::error
            error notify_icat_and_local_servers_preop(
                const std::string& cmd_name,
                const std::string& cmd_option,
                const std::string& wait_option,
                const size_t&      wait_seconds,
                const host_list_t& cmd_hosts,
                std::string&       output);

            // TODO: throw and return output
            //std::string notify_icat_and_local_servers_preop(
                //const command_parameters_t& command_params);

            /// \brief Grid command post-operation
            ///
            /// Sends grid command to certain iRODS servers in the grid in a certain
            /// order after the actual grid command is run on the target hosts.
            ///
            /// \param[in] name - Grid command name
            /// \param[in] option - Grid command options
            /// \param[in] wait_option - "Wait for" option
            /// \param[in] wait_seconds - Number of seconds to wait
            /// \param[in] hosts - List of hosts to which command is sent
            /// \param[out] output - ZeroMQ response message from grid command processing
            ///
            /// \returns irods::error
            error notify_icat_and_local_servers_postop(
                const std::string& cmd_name,
                const std::string& cmd_option,
                const std::string& wait_option,
                const size_t&      wait_seconds,
                const host_list_t& cmd_hosts,
                std::string&       output);

            // TODO: throw and return output
            //std::string notify_icat_and_local_servers_postop(
                //const command_parameters_t& command_params);

            /// \brief Grid command post-operation
            ///
            /// Sends grid command to certain iRODS servers in the grid in a certain
            /// order after the actual grid command is run on the target hosts.
            ///
            /// \param[in] name - Grid command name
            /// \param[in] host - Host to which command is forwarded
            /// \param[in] port_keyword - Server config keyword from which control plane port will come
            /// \param[in] wait_option - "Wait for" option
            /// \param[in] wait_seconds - Number of seconds to wait
            /// \param[out] output - ZeroMQ response message from grid command processing
            ///
            /// \returns irods::error
            error forward_command(
                const std::string& name,
                const std::string& host,
                const std::string& port_keyword,
                const std::string& wait_option,
                const size_t&      wait_seconds,
                std::string&       output);

            // TODO: throw and return output
            //std::string forward_command(
                //const std::string& host,
                //const std::string& port_keyword,
                //const command_parameters_t& command_params);

            /// \brief Checks if specified hostname is in the list of hostnames
            ///
            /// \param[in] hn - Hostname to search for
            /// \param[in] hosts - List of hosts to search
            ///
            /// \returns bool
            /// \retval true if hn is found in hosts; otherwise, false.
            bool is_host_in_list(
                const std::string& hn,
                const host_list_t& hosts);

            // attributes
            /// \brief Server configuration keyword from which server control plane port is derived
            const std::string port_prop_;
            /// \brief Maps control plane commands to functions
            std::unordered_map<std::string, ctrl_func_t> op_map_;
            /// \brief Hostname of the local iRODS server
            std::string my_host_name_;
            /// \brief Hostnme of local zone catalog provider
            std::string icat_host_name_;

    }; // class server_control_executor

    /// \class server_control_plane
    ///
    /// \brief Services irods-grid commands through an owned server_control_executor.
    ///
    /// See: https://docs.irods.org/master/system_overview/control_plane
    ///
    /// \since 4.1.0
    class server_control_plane {
        public:
            // Disables copy
            server_control_plane(const server_control_plane&) = delete;
            server_control_plane& operator=(const server_control_plane&) = delete;

            /// \brief Constructor
            ///
            /// \param[in] prop - Control plane port property from server config
            /// \param[in] op_map - Map of iRODS grid commands to functions
            server_control_plane(
                const std::string& prop,
                const std::unordered_map<std::string, server_control_executor::ctrl_func_t>& op_map);

            /// \brief Destructor
            ///
            /// Joins the server control plane thread.
            ~server_control_plane();

        private:
            /// \brief Functor which manages the control
            server_control_executor control_executor_;

            /// \brief Thread which manages the control loop
            std::thread control_thread_;

    }; // server_control_plane

}; // namespace irods

#endif // IRODS_SERVER_CONTROL_PLANE_HPP
