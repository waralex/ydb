
#include <algorithm>
#include <array>
#include <numeric>
#include <ranges>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/executor_pool_basic.h>
#include <ydb/library/actors/core/scheduler_basic.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/util/should_continue.h>
#include <util/system/sigset.h>
#include <util/generic/xrange.h>

#include "../vdisk_events.h"
#include "util/datetime/base.h"
#include "util/generic/algorithm.h"
#include "util/generic/reserve.h"
#include "util/generic/vector.h"
#include "util/stream/format.h"
#include "ydb/core/base/blobstorage.h"

using namespace NKikimr;
TRope MakeStringRope(const TString& message) {
    return message ? TRope(message) : TRope();
}

TString MakeString(size_t len) {
    TString res;
    for (size_t i = 0; i < len; ++i) {
        res += RandomNumber<char>();
    }
    return res;
}
namespace {
    TString GenerateRandomString(ui32 len) {
        TString res = TString::Uninitialized(len);
        char* p = res.Detach();
        char* end = p + len;
        TReallyFastRng32 rng(RandomNumber<ui64>());
        for (; p + sizeof(ui32) < end; p += sizeof(ui32)) {
            *reinterpret_cast<ui32*>(p) = rng();
        }
        for (; p < end; ++p) {
            *p = rng();
        }
        return res;
    }

    struct TBenchmarkStats {
        ESizeFormat Format = SF_QUANTITY;
        double Avg;
        double P50;
        double P75;
        double P95;

        TBenchmarkStats() = default;
        TBenchmarkStats(ESizeFormat fmt) : Format(fmt) {
        }

        void Out(IOutputStream& os) const {
            os << "avg: " << HumanReadableSize(Avg, Format)
               << ", [p50; p75; p95]: [" << HumanReadableSize(P50, Format)
               << ", " << HumanReadableSize(P75, Format) << ", "
               << HumanReadableSize(P95, Format) << "]";
        }
    };

    class BenchmarkResult {
    public:
        struct UnitResult {
            uint64_t Microseconds;
            size_t SerializedBytes;
            size_t PayloadBytes;
            size_t Requests;
            UnitResult() = default;
            UnitResult(uint64_t microseconds, size_t serializedBytes,
                       size_t payloadBytes, size_t requests)
                : Microseconds(microseconds),
                  SerializedBytes(serializedBytes),
                  PayloadBytes(payloadBytes),
                  Requests(requests) {
            }
            UnitResult(TDuration duration, size_t serializedBytes,
                       size_t payloadBytes, size_t requests)
                : Microseconds(duration.MicroSeconds()),
                  SerializedBytes(serializedBytes),
                  PayloadBytes(payloadBytes),
                  Requests(requests) {
            }
        };

    private:
        TVector<UnitResult> Results;

    public:
        BenchmarkResult() = default;
        BenchmarkResult(size_t size_hint) : Results(Reserve(size_hint)) {
        }

        void pushBack(UnitResult& res) {
            Results.push_back(res);
        }

        void pushBack(UnitResult&& res) {
            Results.push_back(std::move(res));
        }

        TBenchmarkStats GetRpsStats() const {
            return GetStats([](const UnitResult& res) {
                return static_cast<double>(res.Requests) * 1000000. /
                       res.Microseconds;
            });
        }
        TBenchmarkStats GetSerializeThroughputStats() const {
            return GetStats(
                [](const UnitResult& res) {
                    return static_cast<double>(res.SerializedBytes) * 1000000. /
                           res.Microseconds;
                },
                SF_BYTES);
        }
        TBenchmarkStats GetPayloadThroughputStats() const {
            return GetStats(
                [&](const UnitResult& res) {
                    return static_cast<double>(res.PayloadBytes) * 1000000. /
                           res.Microseconds;
                },
                SF_BYTES);
        }

        void OutStats(IOutputStream& os, size_t offset = 4) {
            auto padding = std::string(offset, ' ');

            os << padding << "RPS:" << Endl;
            os << padding << "    " << GetRpsStats() << Endl;
            os << padding << "Serialized data throughput:" << Endl;
            os << padding << "    " << GetSerializeThroughputStats() << Endl;
            os << padding << "Payload data throughput:" << Endl;
            os << padding << "    " << GetPayloadThroughputStats() << Endl;
        }

    private:
        template <class Fn>
        TBenchmarkStats GetStats(Fn statsFn,
                                 ESizeFormat fmt = SF_QUANTITY) const {
            TBenchmarkStats res(fmt);
            auto view = std::views::transform(Results, statsFn);
            res.Avg =
                std::accumulate(view.begin(), view.end(), 0.0) / Results.size();

            TVector<double> sorted(Results.size());
            std::ranges::partial_sort_copy(view, sorted);

            res.P50 = sorted[sorted.size() / 2];
            res.P75 = sorted[std::ceil(sorted.size() * 0.75) - 1];
            res.P95 = sorted[std::ceil(sorted.size() * 0.95) - 1];

            return res;
        }
    };

    template <typename TEv>
    class TSerializationBenchmarkUnit {
        size_t Count;
        size_t PayloadSize;
        TVector<THolder<TEv>> IncomingEvents;
        TVector<TIntrusivePtr<TEventSerializedData>> Buffers;
        TVector<THolder<TEv>> OutcomingEvents;

    public:
        TSerializationBenchmarkUnit(size_t count, size_t payloadSize)
            : Count(count),
              PayloadSize(payloadSize),
              IncomingEvents(Reserve(count)),
              Buffers(Reserve(count)),
              OutcomingEvents(Reserve(count)) {
        }

        void Prepare() {
            IncomingEvents.clear();
            OutcomingEvents.clear();
            Buffers.clear();
            for (size_t i = 0; i < Count; i++) {
                AddEvent(PayloadSize);
            }
        }

        BenchmarkResult::UnitResult RunSerialization() {
            size_t totalSize = 0;
            auto start = TInstant::Now();
            for (auto& ev : IncomingEvents) {
                auto serializer = MakeHolder<TAllocChunkSerializer>();
                ev->SerializeToArcadiaStream(serializer.Get());
                auto buffers =
                    serializer->Release(ev->CreateSerializationInfo());
                totalSize += buffers->GetSize();
                Buffers.push_back(std::move(buffers));
            }
            auto duration = TInstant::Now() - start;
            return BenchmarkResult::UnitResult(duration, totalSize,
                                               Count * PayloadSize, Count);
        }

        BenchmarkResult::UnitResult RunDeserialization() {
            size_t totalSize = 0;
            auto start = TInstant::Now();
            for (auto& buff : Buffers) {
                totalSize += buff->GetSize();
                OutcomingEvents.emplace_back(
                    static_cast<TEv*>(TEv::Load(buff.Get())));
            }
            auto duration = TInstant::Now() - start;
            return BenchmarkResult::UnitResult(duration, totalSize,
                                               Count * PayloadSize, Count);
        }

    private:
        void AddEvent(size_t payloadSize) {
            TBlobStorageGroupType::EErasureSpecies erasureSpecies =
                TBlobStorageGroupType::Erasure3Plus1Block;
            ui64 vDiskIdx = 1;
            TRope testData(GenerateRandomString(payloadSize));
            TBlobStorageGroupType type(erasureSpecies);
            TLogoBlobID logoblobid(1, 1, 1, 0, 100, 3, 1);

            TVDiskID vDiskId(0, 1, 0, vDiskIdx, 0);
            IncomingEvents.emplace_back(new TEv(logoblobid, testData, vDiskId,
                                                false, nullptr, TInstant::Max(),
                                                NKikimrBlobStorage::AsyncBlob));
        }
    };

    template <typename TEv>
    class TSerializationBenchmark {
        size_t CountInUnit;
        size_t UnitsCount;
        size_t PayloadSize;

    public:
        TSerializationBenchmark(size_t countInUnit, size_t unitsCount,
                                size_t payloadSize)
            : CountInUnit(countInUnit),
              UnitsCount(unitsCount),
              PayloadSize(payloadSize) {
        }

        auto Run() {
            BenchmarkResult serRes(UnitsCount);
            BenchmarkResult deserRes(UnitsCount);
            auto totalDuration = TDuration();
            for (size_t i = 0; i < UnitsCount; i++) {
                auto unit =
                    TSerializationBenchmarkUnit<TEv>(CountInUnit, PayloadSize);
                unit.Prepare();
                auto start = TInstant::Now();
                serRes.pushBack(unit.RunSerialization());
                deserRes.pushBack(unit.RunDeserialization());
                auto duration = TInstant::Now() - start;
                totalDuration += duration;
            }
            return std::make_tuple(serRes, deserRes);
        }
    };

}  // namespace

template<>
[[maybe_unused]] inline void Out<TBenchmarkStats>(IOutputStream& o, const TBenchmarkStats &stats) {
    stats.Out(o);
}
int main(int argc, char** argv) {
    Y_UNUSED(argc);
    Y_UNUSED(argv);
    {
        Cerr << "32 bytes of payload" << Endl;
        TSerializationBenchmark<TEvBlobStorage::TEvVPut> bench(10000, 100, 32);
        auto [serRes, deserRes] = bench.Run();
        Cerr << "    [========== Serialization Benchmark ===========]" << Endl;
        serRes.OutStats(Cerr, 8);
        Cerr << "    [========== Deserialization Benchmark ===========]"
             << Endl;
        deserRes.OutStats(Cerr, 8);
    }
    {
        Cerr << "32 bytes of payload bs" << Endl;
        TSerializationBenchmark<TEvBlobStorage::TEvVPutBS> bench(10000, 100,
                                                                 32);
        auto [serRes, deserRes] = bench.Run();
        Cerr << "    [========== Serialization Benchmark ===========]" << Endl;
        serRes.OutStats(Cerr, 8);
        Cerr << "    [========== Deserialization Benchmark ===========]"
             << Endl;
        deserRes.OutStats(Cerr, 8);
    }
}
