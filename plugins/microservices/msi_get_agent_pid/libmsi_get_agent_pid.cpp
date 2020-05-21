#include "irods_ms_plugin.hpp"
#include "irods_re_structs.hpp"
#include "msParam.h"
#include "rodsErrorTable.h"
#include "irods_error.hpp"
#include "irods_logger.hpp"

#include <exception>
#include <functional>
#include <string>

namespace
{
    using log = irods::experimental::log;

    auto msi_impl(msParam_t* out_pid, ruleExecInfo_t* rei) -> int
    {
        try {
            const char* pid = std::to_string(getpid()).c_str();
            fillStrInMsParam(out_pid, pid);
            return 0;
        }
        catch (const std::exception& e) {
            log::microservice::error(e.what());
            return SYS_INTERNAL_ERR;
        }
        catch (...) {
            log::microservice::error("An unknown error occurred while processing the request.");
            return SYS_UNKNOWN_ERROR;
        }
    } // msi_impl

    template <typename... Args, typename Function>
    auto make_msi(const std::string& name, Function func) -> irods::ms_table_entry*
    {
        auto* msi = new irods::ms_table_entry{sizeof...(Args)};
        msi->add_operation<Args..., ruleExecInfo_t*>(name, std::function<int(Args..., ruleExecInfo_t*)>(func));
        return msi;
    } // make_msi
} // anonymous namespace

extern "C"
auto plugin_factory() -> irods::ms_table_entry*
{
    return make_msi<msParam_t*>("msi_get_agent_pid", msi_impl);
}

