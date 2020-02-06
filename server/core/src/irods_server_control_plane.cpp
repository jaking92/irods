#include "avro/Encoder.hh"
#include "avro/Decoder.hh"
#include "avro/Specific.hh"

#include "stdio.h"

#include "rcMisc.h"
#include "sockComm.h"
#include "miscServerFunct.hpp"
#include "rodsServer.hpp"

#include "irods_log.hpp"
#include "irods_server_control_plane.hpp"
#include "irods_server_properties.hpp"
#include "irods_buffer_encryption.hpp"
#include "irods_resource_manager.hpp"
#include "irods_server_state.hpp"
#include "irods_exception.hpp"
#include "irods_stacktrace.hpp"

#include "connection_pool.hpp"
#include "query_builder.hpp"

#include "boost/lexical_cast.hpp"

#include "json.hpp"

#include <algorithm>
#include <ctime>
#include <functional>
#include <unistd.h>

namespace {
    irods::error forward_server_control_command(
        const std::string& _name,
        const std::string& _host,
        const std::string& _port_keyword,
        std::string&       _output )
    {
        if (irods::EMPTY_RESC_HOST == _host) {
            return SUCCESS();
        }

        int time_out, port, num_hash_rounds;
        boost::optional<const std::string&> encryption_algorithm;
        irods::buffer_crypt::array_t shared_secret;
        try {
            time_out = irods::get_server_property<const int>(irods::CFG_SERVER_CONTROL_PLANE_TIMEOUT);
            port = irods::get_server_property<const int>(_port_keyword);
            num_hash_rounds = irods::get_server_property<const int>(irods::CFG_SERVER_CONTROL_PLANE_ENCRYPTION_NUM_HASH_ROUNDS_KW);
            encryption_algorithm.reset(irods::get_server_property<const std::string>(irods::CFG_SERVER_CONTROL_PLANE_ENCRYPTION_ALGORITHM_KW));
            const auto& key = irods::get_server_property<const std::string>(irods::CFG_SERVER_CONTROL_PLANE_KEY);
            shared_secret.assign(key.begin(), key.end());
        } catch ( const irods::exception& e ) {
            return irods::error(e);
        }

        // stringify the port
        std::stringstream port_sstr;
        port_sstr << port;
        // standard zmq rep-req communication pattern
        zmq::context_t zmq_ctx( 1 );
        zmq::socket_t  zmq_skt( zmq_ctx, ZMQ_REQ );
        zmq_skt.setsockopt( ZMQ_RCVTIMEO, &time_out, sizeof( time_out ) );
        zmq_skt.setsockopt( ZMQ_SNDTIMEO, &time_out, sizeof( time_out ) );
        zmq_skt.setsockopt( ZMQ_LINGER, 0 );

        // this is the client so we connect rather than bind
        std::string conn_str( "tcp://" );
        conn_str += _host;
        conn_str += ":";
        conn_str += port_sstr.str();

        try {
            zmq_skt.connect( conn_str.c_str() );
        }
        catch ( zmq::error_t& e_ ) {
            _output += "{\n    \"failed_to_connect\" : \"" + conn_str + "\"\n},\n";
            return ERROR(
                       SYS_INVALID_INPUT_PARAM,
                       _output );
        }

        // build the command to forward
        irods::control_plane_command cmd;
        cmd.command = _name;
        cmd.options[ irods::SERVER_CONTROL_OPTION_KW ] = irods::SERVER_CONTROL_HOSTS_OPT;
        cmd.options[ irods::SERVER_CONTROL_HOST_KW ]   = _host;

        // serialize using the generated avro class
        auto out = avro::memoryOutputStream();
        avro::EncoderPtr e = avro::binaryEncoder();
        e->init( *out );
        avro::encode( *e, cmd );
        std::shared_ptr<std::vector<uint8_t>> data = avro::snapshot(*out);

        irods::buffer_crypt crypt(
            shared_secret.size(),  // key size
            0,                     // salt size ( we dont send a salt )
            num_hash_rounds,       // num hash rounds
            encryption_algorithm->c_str() );

        irods::buffer_crypt::array_t iv;
        irods::buffer_crypt::array_t data_to_send;
        irods::buffer_crypt::array_t data_to_encrypt(
            data->data(),
            data->data() + data->size() );
        irods::error ret = crypt.encrypt(
                  shared_secret,
                  iv,
                  data_to_encrypt,
                  data_to_send );
        if ( !ret.ok() ) {
            return PASS( ret );

        }

        // copy binary encoding into a zmq message for transport
        zmq::message_t rep( data_to_send.size() );
        memcpy(
            rep.data(),
            data_to_send.data(),
            data_to_send.size() );
        zmq_skt.send( rep );

        // wait for the server response
        zmq::message_t req;
        zmq_skt.recv( &req );

        if ( 0 == req.size() ) {
            _output += "{\n    \"response_message_is_empty_from\" : \"" + conn_str + "\"\n},\n";
            return ERROR(
                       SYS_INVALID_INPUT_PARAM,
                       "empty response string" );

        }

        // decrypt the message before passing to avro
        irods::buffer_crypt::array_t data_to_process;
        const uint8_t* data_ptr = static_cast< const uint8_t* >( req.data() );
        irods::buffer_crypt::array_t data_to_decrypt(
            data_ptr,
            data_ptr + req.size() );
        ret = crypt.decrypt(
                  shared_secret,
                  iv,
                  data_to_decrypt,
                  data_to_process );
        if ( !ret.ok() ) {
            irods::log( PASS( ret ) );
            _output += "{\n    \"failed_to_decrpyt_message_from\" : \"" + conn_str + "\"\n},\n";
            rodsLog( LOG_ERROR, "Failed to decrpyt [%s]", req.data() );
            return PASS( ret );

        }

        std::string rep_str(
            reinterpret_cast< char* >( data_to_process.data() ),
            data_to_process.size() );
        if (irods::SERVER_CONTROL_SUCCESS != rep_str) {
            // check if the result is really an error or a status
            if ( std::string::npos == rep_str.find( "[-]" ) ) {
                _output += rep_str;
            }
            else {
                _output += "{\n    \"invalid_message_format_from\" : \"" + conn_str + "\"\n},\n";
                return ERROR(
                           CONTROL_PLANE_MESSAGE_ERROR,
                           rep_str );
            }
        }

        return SUCCESS();

    } // forward_server_control_command

    bool compare_host_names(
        const std::string& _hn1,
        const std::string& _hn2)
    {
        const bool we_are_the_host{_hn1 == _hn2};
        if (!we_are_the_host) {
            const bool host_has_dots{std::string::npos != _hn1.find(".")};
            const bool my_host_has_dots{std::string::npos != _hn2.find(".")};
            if (host_has_dots && !my_host_has_dots) {
                return std::string::npos != _hn1.find(_hn2);
            }
            else if (!host_has_dots && my_host_has_dots) {
                return std::string::npos != _hn2.find(_hn1);
            }
        }
        return we_are_the_host;
    } // compare_host_names

}

namespace irods {

    bool server_control_executor::is_host_in_list(
        const std::string& hn,
        const host_list_t& hosts)
    {
        return hosts.cend() != std::find(hosts.cbegin(), hosts.cend(), hn);
    } // is_host_in_list

    server_control_plane::server_control_plane(
        const std::string& prop,
        const std::unordered_map<std::string, server_control_executor::ctrl_func_t>& op_map)
        : control_executor_{prop, op_map}
        , control_thread_{std::ref(control_executor_)}
    {
    } // ctor

    server_control_plane::~server_control_plane()
    {
        try {
            control_thread_.join();
        }
        catch (const std::system_error& e) {
            irods::log(LOG_ERROR, e.what());
        }
    } // dtor

    server_control_executor::server_control_executor(
        const std::string& prop,
        const std::unordered_map<std::string, server_control_executor::ctrl_func_t>& op_map)
        : port_prop_{prop}
        , op_map_{std::move(op_map)}
     {
        if (port_prop_.empty()) {
            THROW(
                SYS_INVALID_INPUT_PARAM,
                "control_plane_port key is empty");
        }

        // get our hostname for ordering
        rodsEnv my_env;
        _reloadRodsEnv( my_env );
        my_host_name_ = my_env.rodsHost;

        // get the IES host for ordereing
        icat_host_name_ = boost::any_cast<const std::string&>(get_server_property<const std::vector<boost::any>>(CFG_CATALOG_PROVIDER_HOSTS_KW)[0]);

        // repave icat_host_name_ as we do not want to process 'localhost'
        if ( "localhost" == icat_host_name_ ) {
            icat_host_name_ = my_host_name_;
            rodsLog(
                LOG_ERROR,
                "server_control_executor - icat_host_name is localhost, not a fqdn" );
            // TODO :: throw fancy exception here when we disallow localhost
        }

    } // ctor

    error server_control_executor::forward_command(
        const std::string& _name,
        const std::string& _host,
        const std::string& _port_keyword,
        const std::string& _wait_option,
        const size_t&      _wait_seconds,
        std::string&       _output)
    {
        const bool we_are_the_host = compare_host_names(_host, my_host_name_);

        if (we_are_the_host) {
            host_list_t hosts;
            hosts.push_back(_host);
            return process_host_list(
                _name,
                _wait_option,
                _wait_seconds,
                hosts,
                _output );
        }
        return forward_server_control_command(
            _name,
            _host,
            _port_keyword,
            _output);
    } // forward_command

    error server_control_executor::get_resource_host_names(
        host_list_t& _host_names) {
        try {
            auto p = irods::make_connection_pool();
            auto comm = p->get_connection();
            irods::experimental::query_builder qb;
            qb.zone_hint(static_cast<rcComm_t&>(comm).clientUser.rodsZone);
            for (const auto& result : qb.build<rcComm_t>(comm, "select RESC_LOC")) {
                if (0 != result[0].compare("localhost")) {
                    _host_names.push_back(result[0]);
                }
            }
            return SUCCESS();
        }
        catch (const irods::exception& e) {
            return irods::error(e);
        }
    } // get_resource_host_names

    void server_control_executor::operator()() {

        int port, num_hash_rounds;
        boost::optional<const std::string&> encryption_algorithm;
        buffer_crypt::array_t shared_secret;
        try {
            port = get_server_property<const int>(port_prop_);
            num_hash_rounds = get_server_property<const int>(CFG_SERVER_CONTROL_PLANE_ENCRYPTION_NUM_HASH_ROUNDS_KW);
            encryption_algorithm.reset(get_server_property<const std::string>(CFG_SERVER_CONTROL_PLANE_ENCRYPTION_ALGORITHM_KW));
            const auto& key = get_server_property<const std::string>(CFG_SERVER_CONTROL_PLANE_KEY);
            shared_secret.assign(key.begin(), key.end());
        } catch ( const irods::exception& e ) {
            irods::log(e);
            return;
        }

        if ( shared_secret.empty() ||
                encryption_algorithm->empty() ||
                0 == port ||
                0 == num_hash_rounds ) {
            rodsLog(
                LOG_NOTICE,
                "control plane is not configured properly" );
            return;
        }

        while ( true ) {
            try {
                zmq::context_t zmq_ctx( 1 );
                zmq::socket_t  zmq_skt( zmq_ctx, ZMQ_REP );

                int time_out = SERVER_CONTROL_POLLING_TIME_MILLI_SEC;
                zmq_skt.setsockopt( ZMQ_RCVTIMEO, &time_out, sizeof( time_out ) );
                zmq_skt.setsockopt( ZMQ_SNDTIMEO, &time_out, sizeof( time_out ) );
                zmq_skt.setsockopt( ZMQ_LINGER, 0 );

                std::stringstream port_sstr;
                port_sstr << port;
                std::string conn_str( "tcp://*:" );
                conn_str += port_sstr.str();
                zmq_skt.bind( conn_str.c_str() );

                rodsLog(
                        LOG_NOTICE,
                        ">>> control plane :: listening on port %d\n",
                        port );

                auto& s = irods::server_state_mgr::instance();
                while (server_state_t::STOPPED != s.server_state() &&
                       server_state_t::EXITED != s.server_state()) {

                    zmq::message_t req;
                    zmq_skt.recv( &req );
                    if ( 0 == req.size() ) {
                        continue;

                    }

                    // process the message
                    std::string output;
                    std::string rep_msg( SERVER_CONTROL_SUCCESS );
                    error ret = process_operation( req, output );

                    rep_msg = output;
                    if ( !ret.ok() ) {
                        log( PASS( ret ) );
                    }

                    if ( !output.empty() ) {
                        rep_msg = output;

                    }

                    buffer_crypt crypt(
                            shared_secret.size(), // key size
                            0,                    // salt size ( we dont send a salt )
                            num_hash_rounds,      // num hash rounds
                            encryption_algorithm->c_str() );

                    buffer_crypt::array_t iv;
                    buffer_crypt::array_t data_to_send;
                    buffer_crypt::array_t data_to_encrypt(
                            rep_msg.begin(),
                            rep_msg.end() );
                    ret = crypt.encrypt(
                            shared_secret,
                            iv,
                            data_to_encrypt,
                            data_to_send );
                    if ( !ret.ok() ) {
                        irods::log( PASS( ret ) );

                    }

                    zmq::message_t rep( data_to_send.size() );
                    memcpy(
                            rep.data(),
                            data_to_send.data(),
                            data_to_send.size() );

                    zmq_skt.send( rep );

                } // while
                // exited control loop normally, we're done
                break;
            } catch ( const zmq::error_t& _e ) {
                rodsLog(LOG_ERROR, "ZMQ encountered an error in the control plane loop: [%s] Restarting control thread...", _e.what());
                continue;
            }
        }

    } // control operation

    error server_control_executor::notify_icat_and_local_servers_preop(
        const std::string& _cmd_name,
        const std::string& _cmd_option,
        const std::string& _wait_option,
        const size_t&      _wait_seconds,
        const host_list_t& _cmd_hosts,
        std::string&       _output ) {

        if ( SERVER_CONTROL_RESUME != _cmd_name ) {
            return SUCCESS();
        }

        error ret = SUCCESS();
        const bool is_all_opt  = ( SERVER_CONTROL_ALL_OPT == _cmd_option );
        const bool found_my_host = is_host_in_list(
                                       my_host_name_,
                                       _cmd_hosts );
        const bool found_icat_host = is_host_in_list(
                                         icat_host_name_,
                                         _cmd_hosts );
        const bool is_icat_host = compare_host_names( my_host_name_, icat_host_name_ );
        // for pause or shutdown: pre-op forwards to the ies first,
        // then to myself, then others
        // for resume: we skip doing work here (we'll go last in post-op)
        if ( found_icat_host || is_all_opt ) {
            ret = forward_command(
                      _cmd_name,
                      icat_host_name_,
                      CFG_SERVER_CONTROL_PLANE_PORT,
                      _wait_option,
                      _wait_seconds,
                      _output );
            // takes sec, microsec
            rodsSleep(
                0, SERVER_CONTROL_FWD_SLEEP_TIME_MILLI_SEC );
        }

        // pre-op forwards to the local server second
        // such as for resume
        if ( !is_icat_host && ( found_my_host || is_all_opt ) ) {
            ret = forward_command(
                      _cmd_name,
                      my_host_name_,
                      CFG_SERVER_CONTROL_PLANE_PORT,
                      _wait_option,
                      _wait_seconds,
                      _output );
        }

        return ret;

    } // notify_icat_and_local_servers_preop

    error server_control_executor::notify_icat_and_local_servers_postop(
        const std::string& _cmd_name,
        const std::string& _cmd_option,
        const std::string& _wait_option,
        const size_t&      _wait_seconds,
        const host_list_t& _cmd_hosts,
        std::string&       _output ) {
        error ret = SUCCESS();
        if ( SERVER_CONTROL_RESUME == _cmd_name ) {
            return SUCCESS();

        }

        bool is_all_opt  = ( SERVER_CONTROL_ALL_OPT == _cmd_option );
        const bool found_my_host = is_host_in_list(
                                       my_host_name_,
                                       _cmd_hosts );
        const bool found_icat_host = is_host_in_list(
                                         icat_host_name_,
                                         _cmd_hosts );
        const bool is_icat_host = compare_host_names( my_host_name_, icat_host_name_ );

        // post-op forwards to the local server first
        // then the icat such as for shutdown
        if ( !is_icat_host && ( found_my_host || is_all_opt ) ) {
            ret = forward_command(
                      _cmd_name,
                      my_host_name_,
                      CFG_SERVER_CONTROL_PLANE_PORT,
                      _wait_option,
                      _wait_seconds,
                      _output );
        }

        // post-op forwards to the ies last
        if ( found_icat_host || is_all_opt ) {
            ret = forward_command(
                      _cmd_name,
                      icat_host_name_,
                      CFG_SERVER_CONTROL_PLANE_PORT,
                      _wait_option,
                      _wait_seconds,
                      _output );
        }

        return ret;

    } // notify_icat_and_local_servers_postop

    error server_control_executor::validate_host_list(
        const host_list_t&  _irods_hosts,
        const host_list_t&  _cmd_hosts,
        host_list_t&        _valid_hosts ) {

        for (auto&& host : _cmd_hosts) {
            // check host value against list from the icat
            if (!is_host_in_list(host, _irods_hosts) &&
                host != icat_host_name_) {
                std::string msg( "invalid server hostname [" );
                msg += host;
                msg += "]";
                return ERROR(
                           SYS_INVALID_INPUT_PARAM,
                           msg );
            }

            // skip the provider since it is a special case
            // and handled elsewhere
            if (compare_host_names(icat_host_name_, host)) {
                continue;
            }

            // skip the local server since it is also a
            // special case and handled elsewhere
            if (compare_host_names(my_host_name_, host)) {
                continue;
            }

            // add the host to our newly ordered list
            _valid_hosts.push_back(host);
        } // for itr

        return SUCCESS();

    } // validate_host_list

    error server_control_executor::extract_command_parameters(
        const control_plane_command& _cmd,
        std::string&                 _name,
        std::string&                 _option,
        std::string&                 _wait_option,
        size_t&                      _wait_seconds,
        host_list_t&                 _hosts ) {
        // capture and validate the command parameter
        _name = _cmd.command;
        if (0 == op_map_.count(_name)) {
            std::string msg("invalid command [");
            msg += _name;
            msg += "]";
            return ERROR(
                       SYS_INVALID_INPUT_PARAM,
                       msg);
        }

        // capture and validate the option parameter
        auto itr = _cmd.options.find(irods::SERVER_CONTROL_OPTION_KW);
        if (_cmd.options.end() == itr) {
            return ERROR(
                       SYS_INVALID_INPUT_PARAM,
                       "option parameter is empty");
        }

        _option = itr->second;
        if (SERVER_CONTROL_ALL_OPT   != _option &&
            SERVER_CONTROL_HOSTS_OPT != _option ) {
            std::string msg("invalid command option [");
            msg += _option;
            msg += "]";
            return ERROR(
                       SYS_INVALID_INPUT_PARAM,
                       msg );
        }

        // capture and validate the server hosts, skip the option key
        for (auto&& opt : _cmd.options) {
            if (irods::SERVER_CONTROL_OPTION_KW == opt.first) {
                continue;
            }
            else if (SERVER_CONTROL_FORCE_AFTER_KW == opt.first) {
                _wait_option = SERVER_CONTROL_FORCE_AFTER_KW;
                _wait_seconds = boost::lexical_cast<size_t>(opt.second);
            }
            else if (SERVER_CONTROL_WAIT_FOREVER_KW == opt.first) {
                _wait_option = SERVER_CONTROL_WAIT_FOREVER_KW;
                _wait_seconds = 0;
            }
            else if (opt.first.find(SERVER_CONTROL_HOST_KW) != std::string::npos) {
                _hosts.push_back(opt.second);
            }
            else {
                std::string msg( "invalid option key [" );
                msg += opt.first;
                msg += "]";
                return ERROR(
                           SYS_INVALID_INPUT_PARAM,
                           msg);
            }
        } // for itr

        return SUCCESS();

    } // extract_command_parameters

    error server_control_executor::process_host_list(
        const std::string& _cmd_name,
        const std::string& _wait_option,
        const size_t&      _wait_seconds,
        const host_list_t& _hosts,
        std::string&       _output ) {
        if ( _hosts.empty() ) {
            return SUCCESS();
        }

        error fwd_err = SUCCESS();
        for (auto&& host : _hosts) {
            if ("localhost" == host) {
                continue;
            }

            std::string output{};
            if (compare_host_names(host, my_host_name_)) {
                error ret = op_map_[_cmd_name](
                                _wait_option,
                                _wait_seconds,
                                output);
                if ( !ret.ok() ) {
                    fwd_err = PASS( ret );
                }
                _output += output;
            }
            else {
                error ret = forward_command(
                                _cmd_name,
                                host,
                                port_prop_,
                                _wait_option,
                                _wait_seconds,
                                output );
                if ( !ret.ok() ) {
                    _output += output;
                    log( PASS( ret ) );
                    fwd_err = PASS( ret );
                }
                else {
                    _output += output;
                }
            }
        } // for itr
        return fwd_err;
    } // process_host_list

    

    error server_control_executor::process_operation(
        const zmq::message_t& _msg,
        std::string&          _output)
    {
        if (_msg.size() <= 0) {
            return SUCCESS();
        }

        error final_ret = SUCCESS();

        int num_hash_rounds;
        boost::optional<const std::string&> encryption_algorithm;
        buffer_crypt::array_t shared_secret;
        try {
            num_hash_rounds = get_server_property<const int>(CFG_SERVER_CONTROL_PLANE_ENCRYPTION_NUM_HASH_ROUNDS_KW);
            encryption_algorithm.reset(get_server_property<const std::string>(CFG_SERVER_CONTROL_PLANE_ENCRYPTION_ALGORITHM_KW));
            const auto& key = get_server_property<const std::string>(CFG_SERVER_CONTROL_PLANE_KEY);
            shared_secret.assign(key.begin(), key.end());
        } catch ( const irods::exception& e ) {
            return irods::error(e);
        }

        // decrypt the message before passing to avro
        buffer_crypt crypt(
            shared_secret.size(), // key size
            0,                    // salt size ( we dont send a salt )
            num_hash_rounds,      // num hash rounds
            encryption_algorithm->c_str() );

        buffer_crypt::array_t iv;
        buffer_crypt::array_t data_to_process;

        const uint8_t* data_ptr = static_cast< const uint8_t* >( _msg.data() );
        buffer_crypt::array_t data_to_decrypt(
            data_ptr,
            data_ptr + _msg.size() );
        irods::error ret = crypt.decrypt(
                  shared_secret,
                  iv,
                  data_to_decrypt,
                  data_to_process );
        if ( !ret.ok() ) {
            irods::log( PASS( ret ) );
            return PASS( ret );
        }

        auto in = avro::memoryInputStream(
                static_cast<const uint8_t*>(
                    data_to_process.data() ),
                data_to_process.size() );
        avro::DecoderPtr dec = avro::binaryDecoder();
        dec->init( *in );

        control_plane_command cmd;
        avro::decode( *dec, cmd );

        std::string cmd_name, cmd_option, wait_option;
        host_list_t cmd_hosts;
        size_t wait_seconds = 0;
        ret = extract_command_parameters(
                  cmd,
                  cmd_name,
                  cmd_option,
                  wait_option,
                  wait_seconds,
                  cmd_hosts );
        if ( !ret.ok() ) {
            irods::log( PASS( ret ) );
            return PASS( ret );
        }

        // add safeguards - if server is paused only allow a resume call
        if (server_state_t::PAUSED == irods::get_server_state() &&
            SERVER_CONTROL_RESUME != cmd_name) {
            _output = SERVER_PAUSED_ERROR;
            return SUCCESS();
        }

        // the icat needs to be notified first in certain
        // cases such as RESUME where it is needed to capture
        // the host list for validation, etc
        ret = notify_icat_and_local_servers_preop(
                  cmd_name,
                  cmd_option,
                  wait_option,
                  wait_seconds,
                  cmd_hosts,
                  _output );
        if ( !ret.ok() ) {
            final_ret = PASS( ret );
            irods::log( final_ret );
        }

        host_list_t irods_hosts;
        ret = get_resource_host_names(
                  irods_hosts );
        if ( !ret.ok() ) {
            final_ret = PASS( ret );
            irods::log( final_ret );
        }

        if ( SERVER_CONTROL_ALL_OPT == cmd_option ) {
            cmd_hosts = irods_hosts;
        }

        host_list_t valid_hosts;
        ret = validate_host_list(
                  irods_hosts,
                  cmd_hosts,
                  valid_hosts );
        if ( !ret.ok() ) {
            final_ret = PASS( ret );
            irods::log( final_ret );
        }

        ret = process_host_list(
                  cmd_name,
                  wait_option,
                  wait_seconds,
                  valid_hosts,
                  _output );
        if ( !ret.ok() ) {
            final_ret = PASS( ret );
            irods::log( final_ret );
        }

        // the icat needs to be notified last in certain
        // cases such as SHUTDOWN or PAUSE  where it is
        // needed to capture the host list for validation
        ret = notify_icat_and_local_servers_postop(
                  cmd_name,
                  cmd_option,
                  wait_option,
                  wait_seconds,
                  cmd_hosts, // dont want sanitized
                  _output );
        if ( !ret.ok() ) {
            final_ret = PASS( ret );
            irods::log( final_ret );
        }

        return final_ret;

    } // process_operation

} // namespace irods
