#include <library/cpp/testing/unittest/registar.h>
#include <ydb/library/actors/protos/unittests.pb.h>

#include "event_bs.h"

using namespace NActorsBinarySerialization;

struct Field1;
struct Field2;
struct Field3;
struct FieldTmp;
class TTestRecord
    : public TRecord<TField<Field1, uint32_t>, TField<Field2, double>,
                     TField<Field3, int64_t> > {};

Y_UNIT_TEST_SUITE(TEventByteSerialization) {
    Y_UNIT_TEST(Develop) {
        TTestRecord test;
        test.SetValue<Field1>(10);
        test.SetValue<Field2>(874.710);
        test.SetValue<Field3>(-100);
        UNIT_ASSERT_VALUES_EQUAL(test.GetValue<Field1>(), 10);
        UNIT_ASSERT_VALUES_EQUAL(test.GetValue<Field2>(), 874.710);
        UNIT_ASSERT_VALUES_EQUAL(test.GetValue<Field3>(), -100);
    }
}
