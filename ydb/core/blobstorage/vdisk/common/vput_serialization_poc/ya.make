PROGRAM(vput_serialization_poc)

ALLOCATOR(LF)

SRCS(
    main.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/blobstorage/groupinfo
    ydb/core/erasure
    ydb/core/blobstorage/vdisk/common
    ydb/library/actors/protos
    library/cpp/monlib/service/pages
    ydb/core/base
    ydb/core/blobstorage/vdisk/hulldb/base
    ydb/core/blobstorage/vdisk/protos
    ydb/core/protos
)

END()
