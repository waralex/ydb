#include <library/cpp/testing/unittest/registar.h>
#include <ydb/library/actors/protos/unittests.pb.h>

#include <array>
#include <optional>
#include <ranges>

#include "event_bs.h"

using namespace NActorsBinarySerialization;
using namespace NActors;

struct TTestPlain {
    uint64_t Field1 = 0;
    double Field2 = 0;
    int64_t Field3 = 0;
    bool operator==(const TTestPlain&) const = default;
};

template <>
struct TBinarySerializer<TTestPlain>
    : public TStructSerializer<TFieldSerializer<&TTestPlain::Field1>,
                               TFieldSerializer<&TTestPlain::Field2>,
                               TFieldSerializer<&TTestPlain::Field3> > {};

struct TTestNested {
    uint64_t a = 0;
    TTestPlain nested_a;
    int64_t b = 0;
    TTestPlain nested_b;
};

template <>
struct TBinarySerializer<TTestNested>
    : public TStructSerializer<TFieldSerializer<&TTestNested::a>,
                               TFieldSerializer<&TTestNested::nested_a>,
                               TFieldSerializer<&TTestNested::b>,
                               TFieldSerializer<&TTestNested::nested_b> > {};

struct TBoolEnum {
    enum TEnum {
        TE1,
        TE2,
        TE3,

    };
    TEnum a;
    bool b;
    bool c;
    TEnum d;
};

template <>
struct TBinarySerializer<TBoolEnum>
    : public TStructSerializer<
          TFieldSerializer<&TBoolEnum::a>, TFieldSerializer<&TBoolEnum::b>,
          TFieldSerializer<&TBoolEnum::c>, TFieldSerializer<&TBoolEnum::d> > {};

struct TVectors {
    std::vector<int> a;
    double b;
    std::vector<TTestPlain> c;
    TTestPlain d;
    int e[4];
    std::array<double, 2> f;
};

/* template <>
struct TBinarySerializer<TVectors>
    : public TStructSerializer<
          TFieldSerializer<&TVectors::a>, TFieldSerializer<&TVectors::b>,
          TFieldSerializer<&TVectors::c>, TFieldSerializer<&TVectors::d>,
          TFieldSerializer<&TVectors::e>, TFieldSerializer<&TVectors::f> > {}; */


struct TTestOptional {
    uint64_t a = 0;
    std::optional<double> b = 0;
    std::optional<int64_t> c = 0;
    bool operator==(const TTestOptional&) const = default;
};

template <>
struct TBinarySerializer<TTestOptional>
    : public TStructSerializer<TFieldSerializer<&TTestOptional::a>,
                               TFieldSerializer<&TTestOptional::b>,
                               TFieldSerializer<&TTestOptional::c> > {};

struct TTestOptionalBig {
    uint64_t a1 = 0;
    std::optional<int> a2 = 0;
    std::optional<int> a3 = 0;
    std::optional<int> a4 = 0;
    std::optional<int> a5 = 0;
    std::optional<int> a6 = 0;
    std::optional<int> a7 = 0;
    std::optional<int> a8 = 0;
    std::optional<int> a9 = 0;
    std::optional<int> a10 = 0;
    bool operator==(const TTestOptionalBig&) const = default;
};

template <>
struct TBinarySerializer<TTestOptionalBig>
    : public TStructSerializer<TFieldSerializer<&TTestOptionalBig::a1>,
                               TFieldSerializer<&TTestOptionalBig::a2>,
                               TFieldSerializer<&TTestOptionalBig::a3>,
                               TFieldSerializer<&TTestOptionalBig::a4>,
                               TFieldSerializer<&TTestOptionalBig::a5>,
                               TFieldSerializer<&TTestOptionalBig::a6>,
                               TFieldSerializer<&TTestOptionalBig::a7>,
                               TFieldSerializer<&TTestOptionalBig::a8>,
                               TFieldSerializer<&TTestOptionalBig::a9>,
                               TFieldSerializer<&TTestOptionalBig::a10> > {};

struct TTestOptionalNested {
    uint64_t a = 0;
    std::optional<double> b;
    std::optional<TTestOptional> c;
    std::optional<TTestOptional> d;
    bool operator==(const TTestOptionalNested&) const = default;
};

template <>
struct TBinarySerializer<TTestOptionalNested>
    : public TStructSerializer<TFieldSerializer<&TTestOptionalNested::a>,
                               TFieldSerializer<&TTestOptionalNested::b>,
                               TFieldSerializer<&TTestOptionalNested::c>,
                               TFieldSerializer<&TTestOptionalNested::d>> {};

class TTestEvent : public TEventBS<TTestEvent, TTestPlain, 1> {
public:
    TTestEvent() = default;
    TTestEvent(TTestPlain test) : TEventBS(test) {
    }
};

template <typename T>
TString TestSerialize(const T& record) {
    TBinaryOutBuffer buffer(SerializedSize(record));
    Serialize(record, buffer);
    return buffer.GetBuffer();
}

template <typename T>
void TestDeserialize(T& record, const TString& s) {
    TBinaryInBuffer buffer(s.data());
    Deserialize(record, buffer);
}

Y_UNIT_TEST_SUITE(TEventByteSerialization) {
    Y_UNIT_TEST(SimpleStruct) {
        TTestPlain test{100, 123.33, -222};


        auto s = TestSerialize(test);


        TTestPlain res;
        TestDeserialize(res, s);

        UNIT_ASSERT_VALUES_EQUAL(res.Field1, 100);
        UNIT_ASSERT_VALUES_EQUAL(res.Field2, 123.33);
        UNIT_ASSERT_VALUES_EQUAL(res.Field3, -222);
    }
    Y_UNIT_TEST(NestedStruct) {
        TTestNested test;
        test.a = 100;
        test.b = -50;
        test.nested_a.Field1 = 1;
        test.nested_a.Field2 = 1.1;
        test.nested_a.Field3 = -1;

        test.nested_b.Field1 = 2;
        test.nested_b.Field2 = 2.2;
        test.nested_b.Field3 = -2;


        auto s = TestSerialize(test);
        
        TTestNested res;
        TestDeserialize(res, s);

        UNIT_ASSERT_VALUES_EQUAL(res.a, 100);
        UNIT_ASSERT_VALUES_EQUAL(res.b, -50);
        UNIT_ASSERT_VALUES_EQUAL(res.nested_a.Field1, 1);
        UNIT_ASSERT_VALUES_EQUAL(res.nested_a.Field2, 1.1);
        UNIT_ASSERT_VALUES_EQUAL(res.nested_a.Field3, -1);
        UNIT_ASSERT_VALUES_EQUAL(res.nested_b.Field1, 2);
        UNIT_ASSERT_VALUES_EQUAL(res.nested_b.Field2, 2.2);
        UNIT_ASSERT_VALUES_EQUAL(res.nested_b.Field3, -2);
    }
    Y_UNIT_TEST(ZeroBuffer) {
        TTestNested test;
        test.b = -50;
        test.nested_a.Field1 = 1;
        test.nested_a.Field3 = -1;

        test.nested_b.Field2 = 2.2;


        auto s = TestSerialize(test);
        
        TTestNested res;
        TestDeserialize(res, s);

        UNIT_ASSERT_VALUES_EQUAL(res.a, 0);
        UNIT_ASSERT_VALUES_EQUAL(res.b, -50);
        UNIT_ASSERT_VALUES_EQUAL(res.nested_a.Field1, 1);
        UNIT_ASSERT_VALUES_EQUAL(res.nested_a.Field2, 0.);
        UNIT_ASSERT_VALUES_EQUAL(res.nested_a.Field3, -1);
        UNIT_ASSERT_VALUES_EQUAL(res.nested_b.Field1, 0);
        UNIT_ASSERT_VALUES_EQUAL(res.nested_b.Field2, 2.2);
        UNIT_ASSERT_VALUES_EQUAL(res.nested_b.Field3, 0);
    }

    Y_UNIT_TEST(BoolAndEnum) {
        TBoolEnum test;
        test.a = TBoolEnum::TE3;
        test.b = false;
        test.c = true;
        test.d = TBoolEnum::TE1;


        auto s = TestSerialize(test);
        TBoolEnum res;
        TestDeserialize(res, s);

        UNIT_ASSERT(res.a == TBoolEnum::TE3);
        UNIT_ASSERT_VALUES_EQUAL(res.b, false);
        UNIT_ASSERT_VALUES_EQUAL(res.c, true);
        UNIT_ASSERT(res.d == TBoolEnum::TE1);
    }

    Y_UNIT_TEST(SimpleOptional) {
        TTestOptional test;
        test.a = 10;
        test.b = std::nullopt;
        test.c = 1;

        auto s = TestSerialize(test);
        TTestOptional res;
        TestDeserialize(res, s);

        UNIT_ASSERT(res.a == 10);
        UNIT_ASSERT(res.b ==std::nullopt);
        UNIT_ASSERT(res.c == 1);
    }
    Y_UNIT_TEST(BigOptional) {
        TTestOptionalBig test;
        test.a1 = 10;
        test.a2 = std::nullopt;
        test.a3 = 101;
        test.a4 = std::nullopt;
        test.a5 = 102;
        test.a6 = 102;
        test.a7 = 102;
        test.a8 = std::nullopt;
        test.a9 = 102;
        test.a10 = 102;

        auto s = TestSerialize(test);
        TTestOptionalBig res;
        TestDeserialize(res, s);

        UNIT_ASSERT(res == test);
    }
    Y_UNIT_TEST(NestedOptional) {
        TTestOptionalNested test;
        test.a = 10;
        test.b = 10.4;
        test.c = std::nullopt;
        test.d = TTestOptional{1, std::nullopt, 3};

        auto s = TestSerialize(test);
        TTestOptionalNested res;
        TestDeserialize(res, s);

        UNIT_ASSERT(res == test);
    }
    /* Y_UNIT_TEST(Vectors) {
        TVectors test;
        test.a = {1, 2, 3, 4};
        test.b = 333.3;
        test.c = {TTestPlain{1, 2.5, -1}, TTestPlain{3, 4.5, 5}};
        test.d = TTestPlain{1, 2.5, -1};
        test.e[0] = 4;
        test.e[1] = 3;
        test.e[2] = 2;
        test.e[3] = 1;
        test.f = {0.42, -0.53};

        auto baseSerializer = MakeHolder<TAllocChunkSerializer>();
        TBinaryChunkSerializer serializer(baseSerializer.Get());

        Serialize(test, serializer);
        serializer.Finish();
        auto buffers = baseSerializer->Release(TEventSerializationInfo());
        UNIT_ASSERT_EQUAL(buffers->GetSize(), SerializedSize(test));
        auto iter = buffers->GetBeginIter();
        auto stream = TRopeStream(iter, buffers->GetSize());
        TVectors res;
        TBinaryChunkDeserializer deserializer(buffers->GetBeginIter(),
                                              buffers->GetEndIter());
        Deserialize(res, deserializer);

        UNIT_ASSERT(res.a == test.a);
        UNIT_ASSERT(res.b == test.b);
        UNIT_ASSERT(res.c == test.c);
        UNIT_ASSERT(res.d == test.d);
    } */

    Y_UNIT_TEST(Event) {
        TTestPlain test{10, 0.43, -100};
        auto ev = MakeHolder<TTestEvent>(test);
        auto serializer = MakeHolder<TAllocChunkSerializer>();
        ev->SerializeToArcadiaStream(serializer.Get());
        auto buffers = serializer->Release(ev->CreateSerializationInfo());

        auto res =
            THolder(static_cast<TTestEvent*>(TTestEvent::Load(buffers.Get())));
        UNIT_ASSERT(res->Record == test);
        UNIT_ASSERT_VALUES_EQUAL(res->GetPayloadCount(), 0);
    }
    Y_UNIT_TEST(EventWithPayload) {
        TTestPlain test{10, 0.43, -100};
        auto ev = MakeHolder<TTestEvent>(test);
        ev->AddPayload(TRope("first payload"));
        ev->AddPayload(TRope("second payload"));
        ev->AddPayload(TRope("third payload"));
        auto serializer = MakeHolder<TAllocChunkSerializer>();
        ev->SerializeToArcadiaStream(serializer.Get());
        auto buffers = serializer->Release(ev->CreateSerializationInfo());

        auto res =
            THolder(static_cast<TTestEvent*>(TTestEvent::Load(buffers.Get())));
        UNIT_ASSERT(res->Record == test);
        UNIT_ASSERT_VALUES_EQUAL(res->GetPayloadCount(), ev->GetPayloadCount());
        for (auto id : std::views::iota((ui32)0, ev->GetPayloadCount())) {
            UNIT_ASSERT(res->GetPayload(id) == ev->GetPayload(id));
        }
    }
}
