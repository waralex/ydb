#include <library/cpp/testing/unittest/registar.h>
#include <ydb/library/actors/protos/unittests.pb.h>

#include <array>
#include <ranges>

#include "event_bs.h"

using namespace NActorsBinarySerialization;
using namespace NActors;

struct TTestPlain {
    uint64_t Field1;
    double Field2;
    int64_t Field3;
    bool operator==(const TTestPlain&) const = default;
};

template <>
struct TBinarySerializer<TTestPlain>
    : public TStructSerializer<TFieldSerializer<&TTestPlain::Field1>,
                               TFieldSerializer<&TTestPlain::Field2>,
                               TFieldSerializer<&TTestPlain::Field3> > {};

struct TTestNested {
    uint64_t a;
    TTestPlain nested_a;
    int64_t b;
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

template <>
struct TBinarySerializer<TVectors>
    : public TStructSerializer<
          TFieldSerializer<&TVectors::a>, TFieldSerializer<&TVectors::b>,
          TFieldSerializer<&TVectors::c>, TFieldSerializer<&TVectors::d>,
          TFieldSerializer<&TVectors::e>, TFieldSerializer<&TVectors::f> > {};

class TTestEvent : public TEventBS<TTestEvent, TTestPlain, 1> {
public:
    TTestEvent() = default;
    TTestEvent(TTestPlain test) : TEventBS(test) {
    }
};

Y_UNIT_TEST_SUITE(TEventByteSerialization) {
    Y_UNIT_TEST(SimpleStruct) {
        TTestPlain test{100, 123.33, -222};

        auto baseSerializer = MakeHolder<TAllocChunkSerializer>();
        TBinaryChunkSerializer serializer(baseSerializer.Get());

        Serialize(test, serializer);
        serializer.Finish();
        auto buffers = baseSerializer->Release(TEventSerializationInfo());

        UNIT_ASSERT_EQUAL(buffers->GetSize(), SerializedSize(test));

        TTestPlain res;
        TBinaryChunkDeserializer deserializer(buffers->GetBeginIter(),
                                              buffers->GetEndIter());
        Deserialize(res, deserializer);

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

        auto baseSerializer = MakeHolder<TAllocChunkSerializer>();
        TBinaryChunkSerializer serializer(baseSerializer.Get());

        Serialize(test, serializer);
        serializer.Finish();
        auto buffers = baseSerializer->Release(TEventSerializationInfo());
        UNIT_ASSERT_EQUAL(buffers->GetSize(), SerializedSize(test));
        auto iter = buffers->GetBeginIter();
        auto stream = TRopeStream(iter, buffers->GetSize());
        TTestNested res;
        TBinaryChunkDeserializer deserializer(buffers->GetBeginIter(),
                                              buffers->GetEndIter());
        Deserialize(res, deserializer);

        UNIT_ASSERT_VALUES_EQUAL(res.a, 100);
        UNIT_ASSERT_VALUES_EQUAL(res.b, -50);
        UNIT_ASSERT_VALUES_EQUAL(res.nested_a.Field1, 1);
        UNIT_ASSERT_VALUES_EQUAL(res.nested_a.Field2, 1.1);
        UNIT_ASSERT_VALUES_EQUAL(res.nested_a.Field3, -1);
        UNIT_ASSERT_VALUES_EQUAL(res.nested_b.Field1, 2);
        UNIT_ASSERT_VALUES_EQUAL(res.nested_b.Field2, 2.2);
        UNIT_ASSERT_VALUES_EQUAL(res.nested_b.Field3, -2);
    }

    Y_UNIT_TEST(BoolAndEnum) {
        TBoolEnum test;
        test.a = TBoolEnum::TE3;
        test.b = false;
        test.c = true;
        test.d = TBoolEnum::TE1;

        auto baseSerializer = MakeHolder<TAllocChunkSerializer>();
        TBinaryChunkSerializer serializer(baseSerializer.Get());

        Serialize(test, serializer);
        serializer.Finish();
        auto buffers = baseSerializer->Release(TEventSerializationInfo());
        UNIT_ASSERT_EQUAL(buffers->GetSize(), SerializedSize(test));
        auto iter = buffers->GetBeginIter();
        auto stream = TRopeStream(iter, buffers->GetSize());
        TBoolEnum res;
        TBinaryChunkDeserializer deserializer(buffers->GetBeginIter(),
                                              buffers->GetEndIter());
        Deserialize(res, deserializer);

        UNIT_ASSERT(res.a == TBoolEnum::TE3);
        UNIT_ASSERT_VALUES_EQUAL(res.b, false);
        UNIT_ASSERT_VALUES_EQUAL(res.c, true);
        UNIT_ASSERT(res.d == TBoolEnum::TE1);
    }

    Y_UNIT_TEST(Vectors) {
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
    }

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
