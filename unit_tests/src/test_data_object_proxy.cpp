#include "catch.hpp"

#include "data_object_proxy.hpp"
#include "lifetime_manager.hpp"
#include "replica_proxy.hpp"

#include <string_view>

namespace
{
    // clang-format off
    std::string_view LOGICAL_PATH_1 = "/tempZone/home/rods/foo";
    std::string_view LOGICAL_PATH_2 = "/tempZone/home/rods/goo";
    std::string_view RESC_1         = "demoResc";
    std::string_view RESC_2         = "otherResc";
    constexpr rodsLong_t DATA_ID_1  = 10101; 
    constexpr rodsLong_t DATA_ID_2  = 10102;
    constexpr rodsLong_t COLL_ID_1  = 20202; 
    constexpr rodsLong_t COLL_ID_2  = 20203; 

    namespace data_object = irods::experimental::data_object;
    namespace replica     = irods::experimental::replica;
    // clang-format on
} // anonymous namespace

TEST_CASE("test_lib", "[lib]")
{
    dataObjInfo_t r0{};
    r0.dataId = DATA_ID_1;
    r0.collId = COLL_ID_1;
    std::strncpy(r0.objPath,  LOGICAL_PATH_1.data(), sizeof(r0.objPath));
    std::strncpy(r0.rescName, RESC_1.data(),         sizeof(r0.rescName));

    SECTION("replica_proxy")
    {
        std::string_view KEY = "KEY";
        std::string_view VAL = "VAL";

        auto rp = replica::make_replica_proxy(r0);

        // access
        REQUIRE(&r0            == rp.get());
        REQUIRE(DATA_ID_1      == rp.data_id());
        REQUIRE(COLL_ID_1      == rp.collection_id());
        REQUIRE(LOGICAL_PATH_1 == rp.logical_path());
        REQUIRE(RESC_1         == rp.resource());

        // modification
        rp.data_id(DATA_ID_2);
        REQUIRE(DATA_ID_2 == rp.data_id());
        REQUIRE(DATA_ID_2 == r0.dataId);

        rp.collection_id(COLL_ID_2);
        REQUIRE(COLL_ID_2 == rp.collection_id());
        REQUIRE(COLL_ID_2 == r0.collId);

        rp.logical_path(LOGICAL_PATH_2);
        REQUIRE(LOGICAL_PATH_2 == rp.logical_path());
        REQUIRE(LOGICAL_PATH_2 == r0.objPath);

        rp.resource(RESC_2);
        REQUIRE(RESC_2 == rp.resource());
        REQUIRE(RESC_2 == r0.rescName);

        rp.cond_input()[KEY] = VAL;
        REQUIRE(rp.cond_input().contains(KEY));
        REQUIRE(VAL == rp.cond_input().at(KEY));
    }

    SECTION("data_object_proxy")
    {
        dataObjInfo_t r1{};
        r1.dataId = DATA_ID_1;
        r1.collId = COLL_ID_1;
        std::strncpy(r1.objPath,  LOGICAL_PATH_1.data(), sizeof(r1.objPath));
        std::strncpy(r1.rescName, RESC_2.data(),         sizeof(r1.rescName));

        r0.next = &r1;

        data_object::data_object_proxy o{r0};

        constexpr int REPLICA_COUNT = 2;

        // object-level access
        REQUIRE(REPLICA_COUNT   == o.replica_count());
        REQUIRE(DATA_ID_1       == o.data_id());
        REQUIRE(COLL_ID_1       == o.collection_id());
        REQUIRE(LOGICAL_PATH_1  == o.logical_path());

        // replica-level access
        const auto& o_r0 = o.replicas()[0];
        REQUIRE(&r0             == o_r0.get());
        REQUIRE(DATA_ID_1       == o_r0.data_id());
        REQUIRE(COLL_ID_1       == o_r0.collection_id());
        REQUIRE(LOGICAL_PATH_1  == o_r0.logical_path());
        REQUIRE(RESC_1          == o_r0.resource());

        const auto& o_r1 = o.replicas()[1];
        REQUIRE(&r1             == o_r1.get());
        REQUIRE(DATA_ID_1       == o_r1.data_id());
        REQUIRE(COLL_ID_1       == o_r1.collection_id());
        REQUIRE(LOGICAL_PATH_1  == o_r1.logical_path());
        REQUIRE(RESC_2          == o_r1.resource());

        // modification
        o.data_id(DATA_ID_2);
        REQUIRE(DATA_ID_2 == o.data_id());
        REQUIRE(DATA_ID_2 == o_r0.data_id());
        REQUIRE(DATA_ID_2 == o_r1.data_id());
        REQUIRE(DATA_ID_2 == r0.dataId);
        REQUIRE(DATA_ID_2 == r1.dataId);

        o.collection_id(COLL_ID_2);
        REQUIRE(COLL_ID_2 == o.collection_id());
        REQUIRE(COLL_ID_2 == o_r0.collection_id());
        REQUIRE(COLL_ID_2 == o_r1.collection_id());
        REQUIRE(COLL_ID_2 == r0.collId);
        REQUIRE(COLL_ID_2 == r1.collId);

        o.logical_path(LOGICAL_PATH_2);
        REQUIRE(LOGICAL_PATH_2 == o.logical_path());
        REQUIRE(LOGICAL_PATH_2 == o_r0.logical_path());
        REQUIRE(LOGICAL_PATH_2 == o_r1.logical_path());
        REQUIRE(LOGICAL_PATH_2 == r0.objPath);
        REQUIRE(LOGICAL_PATH_2 == r1.objPath);
    }
}

TEST_CASE("test_factory_and_lifetime_manager", "[lib][lifetime_manager]")
{
    auto [rp, lm] = replica::make_replica_proxy();

    rp.data_id(DATA_ID_1);
    rp.collection_id(COLL_ID_1);
    rp.logical_path(LOGICAL_PATH_1);
    rp.resource(RESC_1);

    SECTION("class")
    {
        REQUIRE(DATA_ID_1      == rp.data_id());
        REQUIRE(COLL_ID_1      == rp.collection_id());
        REQUIRE(LOGICAL_PATH_1 == rp.logical_path());
        REQUIRE(RESC_1         == rp.resource());
    }

    SECTION("struct")
    {
        dataObjInfo_t* doi = rp.get();

        REQUIRE(nullptr != doi);
        REQUIRE(DATA_ID_1      == doi->dataId);
        REQUIRE(COLL_ID_1      == doi->collId);
        REQUIRE(LOGICAL_PATH_1 == doi->objPath);
        REQUIRE(RESC_1         == doi->rescName);

        // modify struct and see changes in class
        doi->dataId = DATA_ID_2;
        REQUIRE(DATA_ID_2 == rp.data_id());
        REQUIRE(DATA_ID_2 == doi->dataId);
    }
}

TEST_CASE("const_tests", "[replica][const][struct]")
{
    dataObjInfo_t doi{};
    doi.dataId = DATA_ID_1;

    const dataObjInfo_t* doip{&doi};

    SECTION("get data_object_proxy")
    {
        data_object::data_object_proxy p{*doip}; 

        using struct_type = decltype(p)::doi_type;
        using pointer_type = decltype(p)::doi_pointer_type;
        using const_struct_type = std::remove_reference_t<decltype(*doip)>;

        static_assert(std::is_const_v<const_struct_type>);
        static_assert(std::is_same_v<struct_type, const_struct_type>);
        static_assert(std::is_same_v<pointer_type, const struct_type*>);

        // auto -> const dataObjInfo_t*
        {
        auto d = p.get();
        static_assert(std::is_const_v<std::remove_reference_t<decltype(*d)>>);
        static_assert(std::is_same_v<decltype(d), pointer_type>);
        }

        // auto -> const dataObjInfo_t* const
        {
        const auto d = p.get();
        static_assert(std::is_const_v<decltype(d)>);
        static_assert(std::is_const_v<std::remove_reference_t<decltype(*d)>>);
        static_assert(std::is_same_v<decltype(d), const pointer_type>);
        }

        // auto -> const dataObjInfo_t* const
        {
        const auto& d = p.get();
        static_assert(std::is_const_v<std::remove_reference_t<decltype(d)>>);
        static_assert(std::is_const_v<std::remove_reference_t<decltype(*d)>>);
        static_assert(std::is_same_v<decltype(d), const pointer_type&>);
        }
    }

    SECTION("get replica_proxy")
    {
        replica::replica_proxy r{*doip};

        using struct_type = decltype(r)::doi_type;
        using pointer_type = decltype(r)::doi_pointer_type;
        using const_struct_type = std::remove_reference_t<decltype(*doip)>;

        static_assert(std::is_const_v<const_struct_type>);
        static_assert(std::is_same_v<struct_type, const_struct_type>);
        static_assert(std::is_same_v<pointer_type, const struct_type*>);

        // auto -> const dataObjInfo_t*
        {
        auto d = r.get();
        static_assert(std::is_const_v<std::remove_reference_t<decltype(*d)>>);
        static_assert(std::is_same_v<decltype(d), pointer_type>);
        }

        // auto -> const dataObjInfo_t* const
        {
        const auto d = r.get();
        static_assert(std::is_const_v<decltype(d)>);
        static_assert(std::is_const_v<std::remove_reference_t<decltype(*d)>>);
        static_assert(std::is_same_v<decltype(d), const pointer_type>);
        }

        // auto -> const dataObjInfo_t* const
        {
        const auto& d = r.get();
        static_assert(std::is_const_v<std::remove_reference_t<decltype(d)>>);
        static_assert(std::is_const_v<std::remove_reference_t<decltype(*d)>>);
        static_assert(std::is_same_v<decltype(d), const pointer_type&>);
        }
    }
}

