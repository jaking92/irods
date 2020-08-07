#ifndef IRODS_DATA_OBJECT_PROXY_HPP
#define IRODS_DATA_OBJECT_PROXY_HPP

#include "dataObjInpOut.h"
#include "rcConnect.h"
#include "replica_proxy.hpp"

#include <algorithm>
#include <string_view>
#include <vector>

namespace irods::experimental::data_object
{
    /// \brief Presents a data object-level interface to a dataObjInfo_t legacy iRODS struct.
    ///
    /// Holds a pointer to a dataObjInfo_t whose lifetime is managed outside of the proxy object.
    ///
    /// The data_object_proxy is essentially a wrapper around the linked list of a dataObjInfo_t struct.
    /// This is meant to be used as an interface to the logical representation of data, which has physical
    /// representations in the form of replicas. The data_object_proxy has a vector of replica_proxy
    /// objects to interact with individual physical representations of data.
    ///
    /// \since 4.2.9
    template<
        typename I,
        typename = std::enable_if_t<
            std::is_same_v<dataObjInfo_t, typename std::remove_const_t<I>>
        >
    >
    class data_object_proxy {
    public:
        // Aliases for various types used in data_object_proxy
        using doi_type = I;
        using doi_pointer_type = doi_type*;
        using replica_list = std::vector<replica::replica_proxy<doi_type>>;
        using size_type = int;

        /// \brief Constructs proxy using an existing doi_type
        /// \since 4.2.9
        explicit data_object_proxy(doi_type& _doi)
            : doi_{&_doi}
            , r_{fill_replica_list(_doi)}
        {
        }

        // accessors

        /// \returns rodsLong_t
        /// \retval id of the data object from the catalog
        /// \since 4.2.9
        auto data_id() const -> rodsLong_t { return doi_->dataId; }

        /// \returns rodsLong_t
        /// \retval id of the collection containing this data object
        /// \since 4.2.9
        auto collection_id() const -> rodsLong_t { return doi_->collId; }

        /// \returns std::string_view
        /// \retval Logical path of the data object
        /// \since 4.2.9
        auto logical_path() const -> std::string_view { return doi_->objPath; }

        /// \returns std::string_view
        /// \retval Owner user name of the data object
        /// \since 4.2.9
        auto owner_user_name() const -> std::string_view { return doi_->dataOwnerName; }

        /// \returns std::string_view
        /// \retval Owner zone name of the data object
        /// \since 4.2.9
        auto owner_user_zone() const -> std::string_view { return doi_->dataOwnerZone; }

        /// \returns size_type
        /// \retval Number of replicas for this data object
        /// \since 4.2.9
        auto replica_count() const -> size_type { return r_.size(); }

        /// \returns const replica_list&
        /// \retval Reference to the list of replica_proxy objects
        /// \since 4.2.9
        auto replicas() const -> const replica_list& { return r_; }

        /// \returns const doi_pointer_type
        /// \retval Pointer to the underlying struct
        /// \since 4.2.9
        auto get() const -> const doi_pointer_type { return doi_; }

        // mutators

        /// \brief Set the data id for the data object
        /// \param[in] _did - New value for data id
        /// \returns void
        /// \since 4.2.9
        template<
            typename P = doi_type,
            typename = std::enable_if_t<!std::is_const_v<P>>>
        auto data_id(const rodsLong_t _did) -> void
        {
            for (auto& r : r_) {
                r.data_id(_did);
            }
        }

        /// \brief Set the collection id for the data object (i.e. all replicas)
        /// \param[in] _cid - New value for collection id
        /// \returns void
        /// \since 4.2.9
        template<
            typename P = doi_type,
            typename = std::enable_if_t<!std::is_const_v<P>>>
        auto collection_id(const rodsLong_t _cid) -> void 
        {
            // TODO: should this set the logical path too?
            for (auto& r : r_) {
                r.collection_id(_cid);
            }
        }

        /// \brief Set the logical path for the data object (i.e. all replicas)
        /// \param[in] _lp - New value for logical path
        /// \returns void
        /// \since 4.2.9
        template<
            typename P = doi_type,
            typename = std::enable_if_t<!std::is_const_v<P>>>
        auto logical_path(std::string_view _lp) -> void
        {
            // TODO: should this set the collId too?
            for (auto& r : r_) {
                r.logical_path(_lp);
            }
        }

        /// \brief Set the owner user name for the data object (i.e. all replicas)
        /// \param[in] _un - New value for owner user name
        /// \returns void
        /// \since 4.2.9
        template<
            typename P = doi_type,
            typename = std::enable_if_t<!std::is_const_v<P>>>
        auto owner_user_name(std::string_view _un) -> void
        {
            for (auto& r : r_) {
                r.owner_user_name(_un);
            }
        }

        /// \brief Set the owner zone name for the data object (i.e. all replicas)
        /// \param[in] _zn - New value for owner zone name
        /// \returns void
        /// \since 4.2.9
        template<
            typename P = doi_type,
            typename = std::enable_if_t<!std::is_const_v<P>>>
        auto owner_zone_name(std::string_view _zn) -> void
        {
            for (auto& r : r_) {
                r.owner_zone_name(_zn);
            }
        }

        /// \brief Get reference to replica list
        /// \returns replica_list&
        /// \since 4.2.9
        template<
            typename P = doi_type,
            typename = std::enable_if_t<!std::is_const_v<P>>>
        auto replicas() -> replica_list& { return r_; }

        /// \brief Get pointer to the underlying dataObjInfo_t struct
        /// \returns doi_pointer_type
        /// \since 4.2.9
        template<
            typename P = doi_type,
            typename = std::enable_if_t<!std::is_const_v<P>>>
        auto get() -> doi_pointer_type { return doi_; }

    private:
        /// \brief Pointer to underlying doi_type
        /// \since 4.2.9
        doi_pointer_type doi_;

        /// \brief List of objects representing physical replicas
        /// \since 4.2.9
        replica_list r_;

        /// \brief Generates replica proxy objects from doi_type's linked list and stores them in a list
        /// \since 4.2.9
        static auto fill_replica_list(doi_type& _doi) -> replica_list
        {
            replica_list r;
            for (doi_type* d = &_doi; d; d = d->next) {
                r.push_back(replica::replica_proxy{*d});
            }
            // TODO: do we allow empty replica list?
            return r;
        } // fill_replica_list
    }; // data_object_proxy

    /// \brief Wraps an existing doi_type with a proxy object.
    /// \param[in] _doi - Pre-existing doi_type which will be wrapped by the returned proxy.
    /// \return data_object_proxy
    /// \since 4.2.9
    template<typename doi_type>
    static auto make_data_object_proxy(doi_type& _doi) -> data_object_proxy<doi_type>
    {
        return data_object_proxy{_doi};
    } // make_data_object_proxy

} // namespace irods::experimental::data_object

#endif // #ifndef IRODS_DATA_OBJECT_PROXY_HPP
