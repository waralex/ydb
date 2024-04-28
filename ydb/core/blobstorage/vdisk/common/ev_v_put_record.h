#pragma once

#include <ydb/library/actors/core/event_bs.h>

namespace BSRecords {

    struct TLogoBlobIDBS {
        ui64 RawX1;
        ui64 RawX2;
        ui64 RawX3;
    };

    struct TVDiskIDBS {
        ui32 GroupID;
        ui32 GroupGeneration;
        ui32 Ring;
        ui32 Domain;
        ui32 VDisk;
    };

    enum class EPutHandleClassBS : ui8 {
        TabletLog = 1,  // usually small size, requires low latency, rarely
                        // read sequentially (RT/TMP)
        AsyncBlob = 2,  // async blobs, usually sstables and their parts
                        // (NoneRT/NoneTMP)
        UserData =
            3  // user data that we write as separate blobs (RT/NoneTMP)
        // NOTE: currently NoneRT/TMP blobs must be treated as AsyncBlob
    };

    struct TMessageIdBS {
        // (SequenceId, MsgId) pair identifies message order, used to
        // implement flow control (i.e. windows)
        ui64 SequenceId;
        ui64 MsgId;
    };

    enum class EVDiskQueueIdBS : ui8 {
        Unknown = 0,
        // EPutHandleClass
        PutTabletLog = 1,
        PutAsyncBlob = 2,
        PutUserData = 3,
        // EGetHandleClass
        GetAsyncRead = 4,
        GetFastRead = 5,
        GetDiscover = 6,
        GetLowRead = 7,
    };

    enum EVDiskInternalQueueIdBS {
        IntUnknown = 0,
        IntBegin = 1,
        IntGetAsync = 1,
        IntGetFast = 2,
        IntPutLog = 3,
        IntPutHugeForeground = 4,
        IntPutHugeBackground = 5,
        IntGetDiscover = 6,
        IntLowRead = 7,
        IntEnd = 8,
    };

    struct TVDiskCostSettingsBS {
        ui64 SeekTimeUs;
        ui64 ReadSpeedBps;
        ui64 WriteSpeedBps;
        ui64 ReadBlockSize;
        ui64 WriteBlockSize;
        ui32 MinREALHugeBlobInBytes;
    };

    struct TWindowFeedbackBS {
        enum EStatus : ui8 {
            Unknown = 0,
            Success = 1,         // successful operation
            WindowUpdate = 2,    // window boundaries update because global
                                    // state has changed
            Processed = 3,       // request was processed and this is status
                                    // update after processed item
            IncorrectMsgId = 4,  // client sent incorrect client id
            HighWatermarkOverflow =
                5,  // message is rejected because of queue overflow
        };

        EStatus Status;
        ui64 ActualWindowSize;
        ui64 MaxWindowSize;
        TMessageIdBS ExpectedMsgId;
        TMessageIdBS FailedMsgId;
    };

    struct TExecTimeStatsBS {
        // dsproxy (i.e. sender) stats
        ui64 SubmitTimestamp;  // local timestamp of request submission
        ui64
            InSenderQueue;  // time spent in BS_QUEUE or something like that

        // vdisk (i.e. executor) stats
        ui64 ReceivedTimestamp;  // local (to vdisk node) timestamp of
                                    // request reception
        ui64 Total;    // total time since reception of query until
                        // transmission of reply
        ui64 InQueue;  // time spent in queue (time spent since reception of
                        // query until it was transferred to executor)
        ui64 Execution;  // time spent while actually executing request

        // detailed stats
        ui64 HugeWriteTime;  // time spent while writing Huge Blob (included
                                // in Execution time)
    };

    struct TActorIdBS {
        ui64 RawX1;
        ui64 RawX2;
    };

    struct TMsgQoSBS {
        ui32 DeadlineSeconds;
        TMessageIdBS MsgId;
        ui64 Cos;
        NKikimrBlobStorage::EVDiskQueueId ExtQueueId;
        EVDiskInternalQueueIdBS IntQueueId;
        TVDiskCostSettingsBS CostSettings;
        bool SendMeCostSettings;
        TWindowFeedbackBS Window;
        /* oneof ClientId {
            uint32 ProxyNodeId = 10; // set when client is DS Proxy from
        specific node uint32 ReplVDiskId = 11; // set when client is
        replication actor from specific vdisk uint64 VDiskLoadId = 13; //
        set when client is load test with specific tag uint32 VPatchVDiskId
        = 14; // set when client is vpatch actor from specific vdisk uint32
        BalancingVDiskId = 17; // set when client is balancing actor from
        specific vdisk
        } */
        TExecTimeStatsBS ExecTimeStats;
        TActorIdBS SenderActorId;
        ui64 InternalMessageId;  // for in-process use
    };

    struct TTimestampsBS {
        ui64 SentByDSProxyUs;
        ui64 ReceivedByVDiskUs;
        ui64 SentByVDiskUs;
        ui64 ReceivedByDSProxyUs;
    };

    struct TEvVPutBS {
        struct TExtraBlockCheck {
            ui64 TabletId1;
            ui32 Generation;
        };

        TLogoBlobIDBS BlobID;

        TVDiskIDBS VDiskID;

        ui64 FullDataSize;
        bool IgnoreBlock;
        bool NotifyIfNotReady;
        ui64 Cookie;
        NKikimrBlobStorage::EPutHandleClass HandleClass;
        TMsgQoSBS MsgQoS;
        TTimestampsBS Timestamps;

        //std::vector<TExtraBlockCheck> ExtraBlockChecks;
    };
}  // namespace BSRecords
namespace NActorsBinarySerialization {
    using namespace BSRecords;

    template<>
    struct TBinarySerializer<TLogoBlobIDBS> : public TStructSerializer<
        TFieldSerializer<&TLogoBlobIDBS::RawX1>,
        TFieldSerializer<&TLogoBlobIDBS::RawX2>,
        TFieldSerializer<&TLogoBlobIDBS::RawX3>
    >{};

    template<>
    struct TBinarySerializer<TVDiskIDBS> : public TStructSerializer<
        TFieldSerializer<&TVDiskIDBS::GroupID>,
        TFieldSerializer<&TVDiskIDBS::GroupGeneration>,
        TFieldSerializer<&TVDiskIDBS::Ring>,
        TFieldSerializer<&TVDiskIDBS::Domain>,
        TFieldSerializer<&TVDiskIDBS::VDisk>
    >{};

    template<>
    struct TBinarySerializer<TMessageIdBS> : public TStructSerializer<
        TFieldSerializer<&TMessageIdBS::SequenceId>,
        TFieldSerializer<&TMessageIdBS::MsgId>
    >{};
    
    template<>
    struct TBinarySerializer<TVDiskCostSettingsBS> : public TStructSerializer<
        TFieldSerializer<&TVDiskCostSettingsBS::SeekTimeUs>,
        TFieldSerializer<&TVDiskCostSettingsBS::ReadSpeedBps>,
        TFieldSerializer<&TVDiskCostSettingsBS::WriteSpeedBps>,
        TFieldSerializer<&TVDiskCostSettingsBS::ReadBlockSize>,
        TFieldSerializer<&TVDiskCostSettingsBS::WriteBlockSize>,
        TFieldSerializer<&TVDiskCostSettingsBS::MinREALHugeBlobInBytes>
    >{};

    template<>
    struct TBinarySerializer<TWindowFeedbackBS> : public TStructSerializer<
        TFieldSerializer<&TWindowFeedbackBS::Status>,
        TFieldSerializer<&TWindowFeedbackBS::ActualWindowSize>,
        TFieldSerializer<&TWindowFeedbackBS::MaxWindowSize>,
        TFieldSerializer<&TWindowFeedbackBS::ExpectedMsgId>,
        TFieldSerializer<&TWindowFeedbackBS::FailedMsgId>
    >{};

    template<>
    struct TBinarySerializer<TExecTimeStatsBS> : public TStructSerializer<
        TFieldSerializer<&TExecTimeStatsBS::SubmitTimestamp>,
        TFieldSerializer<&TExecTimeStatsBS::InSenderQueue>,
        TFieldSerializer<&TExecTimeStatsBS::ReceivedTimestamp>,
        TFieldSerializer<&TExecTimeStatsBS::Total>,
        TFieldSerializer<&TExecTimeStatsBS::InQueue>,
        TFieldSerializer<&TExecTimeStatsBS::Execution>,
        TFieldSerializer<&TExecTimeStatsBS::HugeWriteTime>
    >{};

    template<>
    struct TBinarySerializer<TActorIdBS> : public TStructSerializer<
        TFieldSerializer<&TActorIdBS::RawX1>,
        TFieldSerializer<&TActorIdBS::RawX2>
    >{};

    template<>
    struct TBinarySerializer<TMsgQoSBS> : public TStructSerializer<
        TFieldSerializer<&TMsgQoSBS::DeadlineSeconds>,
        TFieldSerializer<&TMsgQoSBS::MsgId>,
        TFieldSerializer<&TMsgQoSBS::Cos>,
        TFieldSerializer<&TMsgQoSBS::ExtQueueId>,
        TFieldSerializer<&TMsgQoSBS::CostSettings>,
        TFieldSerializer<&TMsgQoSBS::SendMeCostSettings>,
        TFieldSerializer<&TMsgQoSBS::Window>,
        TFieldSerializer<&TMsgQoSBS::ExecTimeStats>,
        TFieldSerializer<&TMsgQoSBS::SenderActorId>,
        TFieldSerializer<&TMsgQoSBS::InternalMessageId>
    >{};

    template<>
    struct TBinarySerializer<TTimestampsBS> : public TStructSerializer<
        TFieldSerializer<&TTimestampsBS::SentByDSProxyUs>,
        TFieldSerializer<&TTimestampsBS::ReceivedByDSProxyUs>,
        TFieldSerializer<&TTimestampsBS::SentByVDiskUs>,
        TFieldSerializer<&TTimestampsBS::ReceivedByDSProxyUs>
    >{};

    template<>
    struct TBinarySerializer<TEvVPutBS::TExtraBlockCheck> : public TStructSerializer<
        TFieldSerializer<&TEvVPutBS::TExtraBlockCheck::TabletId1>,
        TFieldSerializer<&TEvVPutBS::TExtraBlockCheck::Generation>
    >{};

    template<>
    struct TBinarySerializer<TEvVPutBS> : public TStructSerializer<
        TFieldSerializer<&TEvVPutBS::BlobID>,
        TFieldSerializer<&TEvVPutBS::VDiskID>,
        TFieldSerializer<&TEvVPutBS::FullDataSize>,
        TFieldSerializer<&TEvVPutBS::IgnoreBlock>,
        TFieldSerializer<&TEvVPutBS::NotifyIfNotReady>,
        TFieldSerializer<&TEvVPutBS::Cookie>,
        TFieldSerializer<&TEvVPutBS::HandleClass>,
        TFieldSerializer<&TEvVPutBS::MsgQoS>,
        TFieldSerializer<&TEvVPutBS::Timestamps>
        //TFieldSerializer<&TEvVPutBS::ExtraBlockChecks>

    >{};
    /* template<>
    struct TBinarySerializer<TEvVPutBS> {
        static bool Serialize(const TEvVPutBS& value, TBinaryChunkSerializer& ser) {
            return ser.Append(value);
        }

        static bool Deserialize(TEvVPutBS& value, TBinaryChunkDeserializer& ser) {
            return ser.Load(value);
        }

        static size_t SerializedSize(const TEvVPutBS& value) {
            Y_UNUSED(value);
            return sizeof(TEvVPutBS);
        }

    }; */


}
