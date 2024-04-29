
#include <util/generic/xrange.h>
#include <util/system/sigset.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/executor_pool_basic.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/core/scheduler_basic.h>
#include <ydb/library/actors/util/should_continue.h>

#include <algorithm>
#include <array>
#include <numeric>
#include <ranges>

#include "../vdisk_events.h"
#include "util/datetime/base.h"
#include "util/generic/algorithm.h"
#include "util/generic/ptr.h"
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
    struct TFill {
        void operator()(TEv* ev) {
            Y_UNUSED(ev);
        }
    };

    template <>
    struct TFill<TEvBlobStorage::TEvVPutBS> {
        void operator()(TEvBlobStorage::TEvVPutBS* ev) {
            ev->Record.Timestamps.SentByDSProxyUs = 12210000;
            ev->Record.Timestamps.SentByVDiskUs = 1221;
            ev->Record.Timestamps.ReceivedByVDiskUs = 12200001;
            ev->Record.Timestamps.ReceivedByDSProxyUs = 122000010000;

            ev->Record.MsgQoS.MsgId.MsgId = 10088;
            ev->Record.MsgQoS.MsgId.SequenceId = 10088333;
            auto& costSettings = ev->Record.MsgQoS.CostSettings;
            costSettings.SeekTimeUs = 400000;
            costSettings.ReadSpeedBps = 401000;
            costSettings.WriteSpeedBps = 401000;
            costSettings.ReadBlockSize = 401600;
            costSettings.WriteBlockSize = 411600;
            costSettings.MinREALHugeBlobInBytes = 41;

            auto& windowFeedback = ev->Record.MsgQoS.Window;
            windowFeedback.ActualWindowSize = 1000;
            windowFeedback.MaxWindowSize = 1500;
            windowFeedback.FailedMsgId.MsgId = 100;
            windowFeedback.FailedMsgId.SequenceId = 300;

            windowFeedback.ExpectedMsgId.MsgId = 100;
            windowFeedback.ExpectedMsgId.SequenceId = 300;

            auto& execTimeStats = ev->Record.MsgQoS.ExecTimeStats;
            execTimeStats.SubmitTimestamp = 4000000;
            execTimeStats.InSenderQueue = 4000001;
            execTimeStats.Total = 400220001;
            execTimeStats.InQueue = 4002204001;
            execTimeStats.Execution = 4002204001;
            execTimeStats.HugeWriteTime = 4002204001;
        }
    };

    template <>
    struct TFill<TEvBlobStorage::TEvVPut> {
        void operator()(TEvBlobStorage::TEvVPut* ev) {
            auto timestamps = ev->Record.MutableTimestamps();
            timestamps->SetSentByDSProxyUs(12210000);
            timestamps->SetSentByVDiskUs(1221);
            timestamps->SetReceivedByVDiskUs(12200001);
            timestamps->SetReceivedByDSProxyUs(122000010000);

            auto msgQoS = ev->Record.MutableMsgQoS();
            msgQoS->MutableMsgId()->SetMsgId(10088);
            msgQoS->MutableMsgId()->SetSequenceId(10088333);
            auto costSettings = msgQoS->MutableCostSettings();
            costSettings->SetSeekTimeUs(400000);
            costSettings->SetReadSpeedBps(401000);
            costSettings->SetWriteSpeedBps(401000);
            costSettings->SetReadBlockSize(401600);
            costSettings->SetWriteBlockSize(411600);
            costSettings->SetMinREALHugeBlobInBytes(41);

            auto windowFeedback = msgQoS->MutableWindow();
            windowFeedback->SetActualWindowSize(1000);
            windowFeedback->SetMaxWindowSize(1500);
            windowFeedback->MutableFailedMsgId()->SetMsgId(100);
            windowFeedback->MutableFailedMsgId()->SetSequenceId(300);

            windowFeedback->MutableExpectedMsgId()->SetMsgId(100);
            windowFeedback->MutableExpectedMsgId()->SetSequenceId(300);

            auto execTimeStats = msgQoS->MutableExecTimeStats();
            execTimeStats->SetSubmitTimestamp(4000000);
            execTimeStats->SetInSenderQueue(4000001);
            execTimeStats->SetTotal(400220001);
            execTimeStats->SetInQueue(4002204001);
            execTimeStats->SetExecution(4002204001);
            execTimeStats->SetHugeWriteTime(4002204001);
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

        void Compare() {
            for (size_t i = 0; i < IncomingEvents.size(); i++) {
                auto& inEvent = IncomingEvents[i];
                auto& OutEvent = OutcomingEvents[i];
                if (!inEvent->GetPayloadCount()) {
                    Cout << "!!! Payload count is zero" << Endl;
                }
                if (inEvent->GetPayloadCount() != OutEvent->GetPayloadCount()) {
                    Cout << "!!! Payload counts not equal" << Endl;
                }
                size_t totalSize = 0;
                for (size_t pi = 0; pi < inEvent->GetPayloadCount(); pi++) {
                    totalSize += inEvent->GetPayload(pi).GetSize();
                    if (inEvent->GetPayload(pi) != OutEvent->GetPayload(pi)) {
                        Cout << "!!! Payloads not equal" << Endl;
                    }
                }
                if (totalSize != PayloadSize) {
                    Cout << "!!! Total size not equal" << Endl;
                }
            }
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
            auto ev =
                MakeHolder<TEv>(logoblobid, testData, vDiskId, false, nullptr,
                                TInstant::Max(), NKikimrBlobStorage::AsyncBlob);
            TFill<TEv>()(ev.Get());

            IncomingEvents.push_back(std::move(ev));
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
                unit.Compare();
                auto duration = TInstant::Now() - start;
                totalDuration += duration;
            }
            return std::make_tuple(serRes, deserRes);
        }
    };

}  // namespace

template <>
[[maybe_unused]] inline void Out<TBenchmarkStats>(
    IOutputStream& o, const TBenchmarkStats& stats) {
    stats.Out(o);
}
int main(int argc, char** argv) {
    Y_UNUSED(argc);
    Y_UNUSED(argv);
    std::array payloadSizes{32, 128, 4096};
    for (auto pSize : payloadSizes) {
        Cout << "========== " << pSize
             << " bytes of payload =============" << Endl;
        {
            Cout << "    ProtoBuf" << Endl;
            TSerializationBenchmark<TEvBlobStorage::TEvVPut> bench(100000, 20,
                                                                   pSize);
            auto [serRes, deserRes] = bench.Run();
            Cout << "        [========== Serialization Benchmark ===========]"
                 << Endl;
            serRes.OutStats(Cout, 12);
            Cout << "        [========== Deserialization Benchmark ===========]"
                 << Endl;
            deserRes.OutStats(Cout, 12);
        }
        {
            Cout << "    BinarySerialization" << Endl;
            TSerializationBenchmark<TEvBlobStorage::TEvVPutBS> bench(100000, 20,
                                                                     pSize);
            auto [serRes, deserRes] = bench.Run();
            Cout << "        [========== Serialization Benchmark ===========]"
                 << Endl;
            serRes.OutStats(Cout, 12);
            Cout << "        [========== Deserialization Benchmark ===========]"
                 << Endl;
            deserRes.OutStats(Cout, 12);
        }
    }
}
