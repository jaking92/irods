#include "connection_pool.hpp"
#include "initServer.hpp"
#include "irodsReServer.hpp"
#include "irods_at_scope_exit.hpp"
#include "irods_server_control_plane.hpp"
#include "irods_delay_queue.hpp"
#include "irods_log.hpp"
#include "irods_query.hpp"
#include "irods_re_structs.hpp"
#include "irods_server_properties.hpp"
#include "irods_server_state.hpp"
#include "miscServerFunct.hpp"
#include "query_processor.hpp"
#include "rodsClient.h"
#include "rodsPackTable.h"
#include "rsGlobalExtern.hpp"
#include "rsLog.hpp"
#include "ruleExecDel.h"
#include "ruleExecSubmit.h"
#include "sockComm.h"
#include "thread_pool.hpp"

#include "json.hpp"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <thread>

#include <boost/filesystem/convenience.hpp>
#include <boost/filesystem/operations.hpp>
#include <boost/format.hpp>

namespace {
    static std::condition_variable term_cv;
    static std::atomic_bool re_server_terminated{};

    irods::server_state_t re_server_state()
    {
        return irods::get_server_state(
            irods::server_process_t::re_server);
    }

    int init_log() {
        /* Handle option to log sql commands */
        auto sql_log_level = getenv(SP_LOG_SQL);
        if(sql_log_level) {
            int j{1};
            #ifdef SYSLOG
                j = atoi(sql_log_level);
            #endif
            rodsLogSqlReq(j);
        }

        /* Set the logging level */
        rodsLogLevel(LOG_NOTICE); /* default */
        auto log_level = getenv(SP_LOG_LEVEL);
        if (log_level) {
            rodsLogLevel(std::atoi(log_level));
        }

        #ifdef SYSLOG
            /* Open a connection to syslog */
            openlog("irodsDelayServer", LOG_ODELAY | LOG_PID, LOG_DAEMON);
        #endif
        int log_fd = logFileOpen(SERVER, nullptr, RULE_EXEC_LOGFILE);
        if (log_fd >= 0) {
            daemonize(SERVER, log_fd);
        }
        return log_fd;
    }

    ruleExecSubmitInp_t fill_rule_exec_submit_inp(
        const std::vector<std::string>& exec_info) {
        namespace bfs = boost::filesystem;

        ruleExecSubmitInp_t rule_exec_submit_inp{};
        rule_exec_submit_inp.packedReiAndArgBBuf = (bytesBuf_t*)malloc(sizeof(bytesBuf_t));

        const auto& rule_exec_id = exec_info[0].c_str();
        const auto& rei_file_path = exec_info[2].c_str();
        bfs::path p{rei_file_path};
        if (!bfs::exists(p)) {
            const int status{UNIX_FILE_STAT_ERR - errno};
            THROW(status, (boost::format("stat error for rei file [%s], id [%s]") %
                  rei_file_path % rule_exec_id).str());
        }

        rule_exec_submit_inp.packedReiAndArgBBuf->len = static_cast<int>(bfs::file_size(p));
        rule_exec_submit_inp.packedReiAndArgBBuf->buf = malloc(rule_exec_submit_inp.packedReiAndArgBBuf->len + 1);

        int fd = open(rei_file_path, O_RDONLY, 0);
        if (fd < 0) {
            const int status{UNIX_FILE_STAT_ERR - errno};
            THROW(status, (boost::format("open error for rei file [%s]") %
                  rei_file_path).str());
        }

        memset(rule_exec_submit_inp.packedReiAndArgBBuf->buf, 0,
               rule_exec_submit_inp.packedReiAndArgBBuf->len + 1);
        ssize_t status{read(fd, rule_exec_submit_inp.packedReiAndArgBBuf->buf,
                        rule_exec_submit_inp.packedReiAndArgBBuf->len)};
        close(fd);
        if (rule_exec_submit_inp.packedReiAndArgBBuf->len != static_cast<int>(status)) {
            if (status >= 0) {
                THROW(SYS_COPY_LEN_ERR, (boost::format("read error for [%s],toRead [%d], read [%d]") %
                      rei_file_path % rule_exec_submit_inp.packedReiAndArgBBuf->len % status).str());
            }
            status = UNIX_FILE_READ_ERR - errno;
            irods::log(LOG_ERROR, (boost::format("read error for file [%s], status = [%d]") %
                       rei_file_path % status).str());
        }

        rstrcpy(rule_exec_submit_inp.ruleExecId, rule_exec_id, NAME_LEN);
        rstrcpy(rule_exec_submit_inp.ruleName, exec_info[1].c_str(), META_STR_LEN);
        rstrcpy(rule_exec_submit_inp.reiFilePath, rei_file_path, MAX_NAME_LEN);
        rstrcpy(rule_exec_submit_inp.userName, exec_info[3].c_str(), NAME_LEN);
        rstrcpy(rule_exec_submit_inp.exeAddress, exec_info[4].c_str(), NAME_LEN);
        rstrcpy(rule_exec_submit_inp.exeTime, exec_info[5].c_str(), TIME_LEN);
        rstrcpy(rule_exec_submit_inp.exeFrequency, exec_info[6].c_str(), NAME_LEN);
        rstrcpy(rule_exec_submit_inp.priority, exec_info[7].c_str(), NAME_LEN);
        rstrcpy(rule_exec_submit_inp.lastExecTime, exec_info[8].c_str(), NAME_LEN);
        rstrcpy(rule_exec_submit_inp.exeStatus, exec_info[9].c_str(), NAME_LEN);
        rstrcpy(rule_exec_submit_inp.estimateExeTime, exec_info[10].c_str(), NAME_LEN);
        rstrcpy(rule_exec_submit_inp.notificationAddr, exec_info[11].c_str(), NAME_LEN);

        return rule_exec_submit_inp;
    }

    int update_entry_for_repeat(
        rcComm_t& _comm,
        ruleExecSubmitInp_t& _inp,
        int _exec_status) {
        // Prepare input for rule exec mod
        _exec_status = _exec_status > 0 ? 0 : _exec_status;

        // Prepare input for getting next repeat time
        char current_time[NAME_LEN]{};
        char* ef_string = _inp.exeFrequency;
        char next_time[NAME_LEN]{};
        snprintf(current_time, NAME_LEN, "%ld", std::time(nullptr));

        const auto delete_rule_exec_info{[&_comm, &_inp]() -> int {
            ruleExecDelInp_t rule_exec_del_inp{};
            rstrcpy(rule_exec_del_inp.ruleExecId, _inp.ruleExecId, NAME_LEN);
            const int status = rcRuleExecDel(&_comm, &rule_exec_del_inp);
            if (status < 0) {
                irods::log(LOG_ERROR, (boost::format(
                           "%s:%d - rcRuleExecDel failed %d for id %s") %
                           __FUNCTION__ % __LINE__ % status % rule_exec_del_inp.ruleExecId).str());
            }
            return status;
        }};

        const auto update_rule_exec_info = [&](const bool repeat_rule) -> int {
            ruleExecModInp_t rule_exec_mod_inp{};
            rstrcpy(rule_exec_mod_inp.ruleId, _inp.ruleExecId, NAME_LEN);

            addKeyVal(&rule_exec_mod_inp.condInput, RULE_LAST_EXE_TIME_KW, current_time);
            addKeyVal(&rule_exec_mod_inp.condInput, RULE_EXE_TIME_KW, next_time);
            if(repeat_rule) {
                addKeyVal(&rule_exec_mod_inp.condInput, RULE_EXE_FREQUENCY_KW, ef_string);
            }
            const int status = rcRuleExecMod(&_comm, &rule_exec_mod_inp);
            if (status < 0) {
                irods::log(LOG_ERROR, (boost::format(
                           "%s:%d - rcRuleExecMod failed %d for id %s") %
                           __FUNCTION__ % __LINE__ % status % rule_exec_mod_inp.ruleId).str());
            }
            if (rule_exec_mod_inp.condInput.len > 0) {
                clearKeyVal(&rule_exec_mod_inp.condInput);
            }
            return status;
        };

        irods::log(LOG_DEBUG,
            (boost::format("[%s:%d] - time:[%s],ef:[%s],next:[%s]") %
            __FUNCTION__ % __LINE__ % current_time % ef_string % next_time).str());
        const int repeat_status = getNextRepeatTime(current_time, ef_string, next_time);
        switch(repeat_status) {
            case 0:
                // Continue with given delay regardless of success
                return update_rule_exec_info(false);
            case 1:
                // Remove if successful, otherwise update next exec time
                return !_exec_status ? delete_rule_exec_info() : update_rule_exec_info(false);
            case 2:
                // Remove regardless of success
                return delete_rule_exec_info();
            case 3:
                // Update with new exec time and frequency regardless of success
                return update_rule_exec_info(true);
            case 4:
                // Delete if successful, otherwise update with new exec time and frequency
                return !_exec_status ? delete_rule_exec_info() : update_rule_exec_info(true);
            default:
                irods::log(LOG_ERROR, (boost::format(
                           "%s:%d - getNextRepeatTime returned unknown value %d for id %s") %
                           __FUNCTION__ % __LINE__ % repeat_status % _inp.ruleExecId).str());
                return repeat_status; 
        }
    }

    exec_rule_expression_t pack_exec_rule_expression(
        ruleExecSubmitInp_t& _inp) {
        exec_rule_expression_t exec_rule{};

        int packed_rei_len = _inp.packedReiAndArgBBuf->len;
        exec_rule.packed_rei_.len = packed_rei_len;
        exec_rule.packed_rei_.buf = _inp.packedReiAndArgBBuf->buf;

        size_t rule_len = strlen(_inp.ruleName);
        exec_rule.rule_text_.buf = (char*)malloc(rule_len+1);
        exec_rule.rule_text_.len = rule_len+1;
        rstrcpy( (char*)exec_rule.rule_text_.buf, _inp.ruleName, rule_len+1);
        return exec_rule;
    }

    int run_rule_exec(
        rcComm_t& _comm,
        ruleExecSubmitInp_t& _inp) {
        // unpack the rei to get the user information
        ruleExecInfoAndArg_t* rei_and_arg{};
        int status = unpackStruct(
                         _inp.packedReiAndArgBBuf->buf,
                         (void**)&rei_and_arg,
                         "ReiAndArg_PI",
                         RodsPackTable,
                         NATIVE_PROT);
        if (status < 0) {
            irods::log(LOG_ERROR,
                (boost::format("[%s] - unpackStruct error. status [%d]") %
                __FUNCTION__ % status).str());
            return status;
        }

        // set the proxy user from the rei before delegating to the agent
        // following behavior from touchupPackedRei
        _comm.proxyUser = *rei_and_arg->rei->uoic;

        exec_rule_expression_t exec_rule = pack_exec_rule_expression(_inp);
        exec_rule.params_ = rei_and_arg->rei->msParamArray;
        irods::at_scope_exit<std::function<void()>> at_scope_exit{[&exec_rule, &rei_and_arg] {
            clearBBuf(&exec_rule.rule_text_);
            if(rei_and_arg->rei) {
                if(rei_and_arg->rei->rsComm) {
                    free(rei_and_arg->rei->rsComm);
                }
                freeRuleExecInfoStruct(rei_and_arg->rei, (FREE_MS_PARAM | FREE_DOINP));
            }
            free(rei_and_arg);
        }};

        status = rcExecRuleExpression(&_comm, &exec_rule);
        if (re_server_terminated) {
            irods::log(LOG_NOTICE,
                (boost::format("Rule [%s] completed with status [%d] but RE server was terminated.") %
                _inp.ruleExecId % status).str());
        }

        if (strlen(_inp.exeFrequency) > 0) {
            return update_entry_for_repeat(_comm, _inp, status);
        }
        else if(status < 0) {
            irods::log(LOG_ERROR,
                (boost::format("ruleExec of %s: %s failed.") %
                _inp.ruleExecId % _inp.ruleName).str());
            ruleExecDelInp_t rule_exec_del_inp{};
            rstrcpy(rule_exec_del_inp.ruleExecId, _inp.ruleExecId, NAME_LEN);
            status = rcRuleExecDel(&_comm, &rule_exec_del_inp);
            if (status < 0) {
                irods::log(LOG_ERROR,
                    (boost::format("rcRuleExecDel failed for %s, stat=%d") %
                    _inp.ruleExecId % status).str());
                // Establish a new connection as the original may be invalid
                rodsEnv env{};
                _getRodsEnv(env);
                auto tmp_pool = std::make_shared<irods::connection_pool>(
                    1,
                    env.rodsHost,
                    env.rodsPort,
                    env.rodsUserName,
                    env.rodsZone,
                    env.irodsConnectionPoolRefreshTime);
                status = rcRuleExecDel(&static_cast<rcComm_t&>(tmp_pool->get_connection()), &rule_exec_del_inp);
                if (status < 0) {
                    irods::log(LOG_ERROR,
                            (boost::format("rcRuleExecDel failed again for %s, stat=%d - exiting") %
                            _inp.ruleExecId % status).str());
                }
            }
            return status;
        }
        else {
            // Success - remove rule from catalog
            ruleExecDelInp_t rule_exec_del_inp{};
            rstrcpy(rule_exec_del_inp.ruleExecId, _inp.ruleExecId, NAME_LEN);
            status = rcRuleExecDel(&_comm, &rule_exec_del_inp);
            if(status < 0) {
                irods::log(LOG_ERROR,
                    (boost::format("Failed deleting rule exec %s from catalog") %
                    rule_exec_del_inp.ruleExecId).str());
            }
            return status;
        }
    }

    void execute_rule(
        std::shared_ptr<irods::connection_pool> conn_pool,
        irods::delay_queue& queue,
        const std::vector<std::string>& rule_info)
        //const std::vector<std::string>& rule_info,
        //const std::atomic_bool& re_server_terminated)
    {
        if (re_server_terminated) {
            return;
        }
        ruleExecSubmitInp_t rule_exec_submit_inp{};

        irods::at_scope_exit<std::function<void()>> at_scope_exit{[&rule_exec_submit_inp] {
            freeBBuf(rule_exec_submit_inp.packedReiAndArgBBuf);
        }};

        try{
            rule_exec_submit_inp = fill_rule_exec_submit_inp(rule_info);
        } catch(const irods::exception& e) {
            irods::log(e);
            return;
        }

        try {
            irods::log(LOG_DEBUG,
                (boost::format("Executing rule [%s]") %
                rule_exec_submit_inp.ruleExecId).str());
            int status = run_rule_exec(conn_pool->get_connection(), rule_exec_submit_inp);
            if(status < 0) {
                irods::log(LOG_ERROR,
                    (boost::format("Rule exec for [%s] failed. status = [%d]") %
                    rule_exec_submit_inp.ruleExecId % status).str());
            }
        } catch(const std::exception& e) {
            irods::log(LOG_ERROR,
                (boost::format("Exception caught during execution of rule [%s]: [%s]") %
                rule_exec_submit_inp.ruleExecId % e.what()).str());
        }

        if (!re_server_terminated) {
            queue.dequeue_rule(std::string(rule_exec_submit_inp.ruleExecId));
        }
    }

    auto make_delay_queue_query_processor(
        std::shared_ptr<irods::connection_pool> conn_pool,
        irods::thread_pool& thread_pool,
        irods::delay_queue& queue) -> irods::query_processor<rcComm_t>
        //irods::delay_queue& queue,
        //const std::atomic_bool& re_server_terminated) -> irods::query_processor<rcComm_t>
    {
        using result_row = irods::query_processor<rsComm_t>::result_row;
        const auto now = std::to_string(std::time(nullptr));
        const auto qstr = (boost::format(
            "SELECT RULE_EXEC_ID, \
                    RULE_EXEC_NAME, \
                    RULE_EXEC_REI_FILE_PATH, \
                    RULE_EXEC_USER_NAME, \
                    RULE_EXEC_ADDRESS, \
                    RULE_EXEC_TIME, \
                    RULE_EXEC_FREQUENCY, \
                    RULE_EXEC_PRIORITY, \
                    RULE_EXEC_LAST_EXE_TIME, \
                    RULE_EXEC_STATUS, \
                    RULE_EXEC_ESTIMATED_EXE_TIME, \
                    RULE_EXEC_NOTIFICATION_ADDR \
            WHERE RULE_EXEC_TIME <= '%s'") % now).str();
        const auto job = [&](const result_row& result) -> void
        {
            const auto& rule_id = result[0];
            if(queue.contains_rule_id(rule_id)) {
                return;
            }
            irods::log(LOG_DEBUG,
                (boost::format("Enqueueing rule [%s]")
                % rule_id).str());
            queue.enqueue_rule(rule_id);
            irods::thread_pool::post(thread_pool, [conn_pool, &queue, result] {
            //irods::thread_pool::post(thread_pool, [conn_pool, &queue, result, &re_server_terminated] {
                execute_rule(conn_pool, queue, result);
                //execute_rule(conn_pool, queue, result, re_server_terminated);
            });
        };
        return {qstr, job};
    }

    irods::error operation_status(
        const std::string&,
        const size_t,
        std::string& _output)
    {
        using json = nlohmann::json;

        rodsEnv my_env;
        _reloadRodsEnv(my_env);

        json obj{
            {"hostname", my_env.rodsHost},
            {"re_server_pid", getpid()},
            {"status", irods::get_server_state()}
        };

        _output += obj.dump(4);
        _output += ",";

        return SUCCESS();
    } // operation_status

    // TODO: Identify server process
    irods::error operation_ping(
        const std::string&,
        const size_t,
        std::string& _output)
    {
        _output += "{\n    \"status\": \"alive\"\n},\n";
        return SUCCESS();
    }

    // TODO: Identify server process
    irods::error operation_pause(
        const std::string&,
        const size_t,
        std::string& _output)
    {
        rodsEnv my_env;
        _reloadRodsEnv(my_env);

        _output += "{\n    \"pausing\": \"";
        _output += my_env.rodsHost;
        _output += "\"\n},\n";

        irods::pause_server();
        return SUCCESS();
    } // operation_pause

    // TODO: Identify server process
    irods::error operation_resume(
        const std::string&,
        const size_t,
        std::string& _output)
    {
        rodsEnv my_env;
        _reloadRodsEnv(my_env);

        _output += "{\n    \"resuming\": \"";
        _output += my_env.rodsHost;
        _output += "\"\n},\n";

        irods::resume_server();
        return SUCCESS();
    } // operation_resume

    irods::error operation_shutdown(
        const std::string& _wait_option,
        const size_t       _wait_seconds,
        std::string&       _output)
    {
        rodsEnv my_env;
        _reloadRodsEnv( my_env );
        _output += "{\n    \"shutting down\": \"";
        _output += my_env.rodsHost;
        //_output += ",\n    \"server_process\": \"";
        //_output += "re_server";
        _output += "\"\n},\n";

        int sleep_time_out_milli_sec = 0;
        try {
            sleep_time_out_milli_sec = irods::get_server_property<const int>(irods::CFG_SERVER_CONTROL_PLANE_TIMEOUT);
        } catch (const irods::exception& e) {
            return irods::error(e);
        }

        if ( irods::SERVER_CONTROL_FORCE_AFTER_KW == _wait_option ) {
            // convert sec to millisec for comparison
            sleep_time_out_milli_sec = _wait_seconds * 1000;
        }

        //irods::pause_server();
        //std::raise(SIGINT);
        //irods::stop_server();

        irods::stop_server();
        std::raise(SIGINT);

        // block until server exits to return
        const int wait_milliseconds = irods::SERVER_CONTROL_POLLING_TIME_MILLI_SEC;
        int sleep_time{};
        bool timeout_flg{};
        while(!timeout_flg) {
            if (irods::server_state_t::EXITED == irods::get_server_state()) {
                irods::log(LOG_NOTICE,
                    "server state exited, we out");
                break;
            }
            irods::log(LOG_NOTICE,
                "not exited yet -- sleeping...");
            rodsSleep(0, wait_milliseconds * 1000);
            sleep_time += wait_milliseconds;
            if (sleep_time > sleep_time_out_milli_sec) {
                irods::log(LOG_NOTICE,
                    "timed out!!");
                timeout_flg = true;
            }
        } // while
        return SUCCESS();
    } // server_operation_shutdown

    using operation_map_t = std::unordered_map<std::string, irods::server_control_executor::ctrl_func_t>;

    operation_map_t build_control_plan_op_map()
    {
        operation_map_t op_map;
        op_map[irods::SERVER_CONTROL_PAUSE] = operation_pause;
        op_map[irods::SERVER_CONTROL_RESUME] = operation_resume;
        op_map[irods::SERVER_CONTROL_STATUS] = operation_status;
        op_map[irods::SERVER_CONTROL_PING] = operation_ping;
        op_map[irods::SERVER_CONTROL_SHUTDOWN] = operation_shutdown;
        return op_map;
    }
}

int main() {
    static std::mutex term_m;
    const auto signal_exit_handler = [](int signal) {
        irods::log(LOG_NOTICE,
            (boost::format("RE server received signal [%d]")
            % signal).str());
        re_server_terminated = true;
        term_cv.notify_all();
    };
    signal(SIGINT, signal_exit_handler);
    signal(SIGHUP, signal_exit_handler);
    signal(SIGTERM, signal_exit_handler);
    signal(SIGUSR1, signal_exit_handler);

    auto log_fd = init_log();
    if(log_fd < 0) {
        exit(log_fd);
    }

    const auto sleep_time = [] {
        int sleep_time = irods::default_re_server_sleep_time;
        try {
            sleep_time = irods::get_advanced_setting<const int>(irods::CFG_RE_SERVER_SLEEP_TIME);
        } catch (const irods::exception& e) {
            irods::log(e);
        }
        return sleep_time;
    }();

    const auto go_to_sleep = [&sleep_time]() {
        std::unique_lock<std::mutex> sleep_lock{term_m};
        const auto until = std::chrono::system_clock::now() + std::chrono::seconds(sleep_time);
        if (std::cv_status::no_timeout == term_cv.wait_until(sleep_lock, until)) {
            irods::log(LOG_NOTICE, "I have been awoken by a notify!");
        }
    };

    const auto thread_count = [] {
        int thread_count = irods::default_max_number_of_concurrent_re_threads;
        try {
            thread_count = irods::get_advanced_setting<const int>(irods::CFG_MAX_NUMBER_OF_CONCURRENT_RE_PROCS);
        } catch (const irods::exception& e) {
            irods::log(e);
        }
        return thread_count;
    }();
    irods::thread_pool thread_pool{thread_count};
    irods::delay_queue queue;

    rodsEnv env{};
    _getRodsEnv(env);
    try {
        irods::server_control_plane ctrl_plane(
            irods::CFG_RULE_ENGINE_CONTROL_PLANE_PORT,
            build_control_plan_op_map());

        irods::at_scope_exit<std::function<void()>> dp{[]()
        {
            rodsLog(LOG_NOTICE, "disconnected pool");
        }};

        auto worker_conn_pool = std::make_shared<irods::connection_pool>(
            thread_count,
            env.rodsHost,
            env.rodsPort,
            env.rodsUserName,
            env.rodsZone,
            env.irodsConnectionPoolRefreshTime);

        irods::at_scope_exit<std::function<void()>> tb{[]()
        {
            rodsLog(LOG_NOTICE, "leaving main try block");
        }};

        while(!re_server_terminated) {
            using state = irods::server_state_t;
            //const auto s = re_server_state();
            irods::log(LOG_NOTICE,
                "getting server state...");
            const auto s = irods::get_server_state();
            irods::log(LOG_NOTICE,
                "server state acquired");
            //irods::log(LOG_NOTICE,
                //(boost::format("re server state is [%d]") % s).str());
            if (state::STOPPED == s) {
                irods::log(LOG_NOTICE,
                    "delay server has been stopped.");
                    //(boost::format("delay server is exiting with state [%s]")
                    //% the_server_state.c_str()).str());
                re_server_terminated = true;
                break;
            }
            else if (state::PAUSED == s) {
                // TODO: This is from main server
                irods::log(LOG_NOTICE,
                    "re server is paused...");
                std::this_thread::sleep_for(std::chrono::seconds(1));
                continue;
            }
            else {
                if (state::RUNNING != s) {
                    irods::log(LOG_NOTICE,
                        "invalid delay server state.");
                        //(boost::format("invalid delay server state [%s]")
                        //% the_server_state.c_str()).str());
                }
                irods::log(LOG_NOTICE,
                    "delay server is running");
            }

            try {
                auto delay_queue_processor = make_delay_queue_query_processor(worker_conn_pool, thread_pool, queue);
                //auto delay_queue_processor = make_delay_queue_query_processor(worker_conn_pool, thread_pool, queue, re_server_terminated);
                auto query_conn_pool = irods::make_connection_pool();
                auto query_conn = query_conn_pool->get_connection();
                auto future = delay_queue_processor.execute(thread_pool, static_cast<rcComm_t&>(query_conn));
                auto errors = future.get();
                if(errors.size() > 0) {
                    for(const auto& [code, msg] : errors) {
                        irods::log(LOG_ERROR,
                            (boost::format("executing delayed rule failed - [%d]::[%s]")
                            % code
                            % msg).str());
                    }
                }
            } catch(const std::exception& e) {
                irods::log(LOG_ERROR, e.what());
            } catch(const irods::exception& e) {
                irods::log(e);
            }
            irods::log(LOG_NOTICE,
                "going to sleep...");
            go_to_sleep();
            irods::log(LOG_NOTICE,
                "I'm awake! bottom of the loop");
        }
        irods::server_state_mgr::instance().server_state(
            irods::server_state_t::EXITED);
        irods::log(LOG_NOTICE, "out of the loop");
    }
    catch (const irods::exception& e) {
        irods::log(LOG_ERROR,
            (boost::format("Exception caught in delay server loop\n%s")
            % e.what()).str());
        return e.code();
    }
    irods::log(LOG_NOTICE, "RE server exiting...");
}
