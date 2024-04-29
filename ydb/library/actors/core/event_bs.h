#pragma once

#include <google/protobuf/arena.h>
#include <google/protobuf/io/zero_copy_stream.h>
#include <library/cpp/containers/stack_vector/stack_vec.h>
#include <util/generic/deque.h>
#include <util/string/builder.h>
#include <util/system/context.h>
#include <util/system/filemap.h>
#include <util/thread/lfstack.h>

#include <array>
#include <concepts>
#include <cstdint>
#include <iterator>
#include <span>
#include <type_traits>
#include <utility>

#include "event_pb.h"
#include "ydb/library/actors/util/rope.h"

namespace NActorsBinarySerialization {
    //========== Forward declaration ==========

    class TBinaryChunkSerializer;
    class TBinaryChunkDeserializer;

    class TBinaryOutBuffer;
    class TBinaryInBuffer;

    template <typename T>
    struct TBinarySerializer;

    template <auto FPtr>
    struct TUnpackMemberPtr;

    template <typename... Fields>
    struct TOwnerExtractor;

    template <typename T>
    struct TUnpackOptional;

    //========= Concepts =========

    template <typename T>
    struct TIsStdArray;

    template <typename T, auto N>
    struct TIsStdArray<std::array<T, N>> {
        constexpr static bool Value = true;
    };

    template <typename T>
    struct TIsStdArray {
        constexpr static bool Value = false;
    };

    template <typename T>
    static bool constexpr TIsStdArrayV = TIsStdArray<T>::Value;

    template <typename T>
    concept CBSStruct = std::default_initializable<T>;

    template <typename TS, typename T>
    concept CSerializer =
        requires(T& t, TBinaryOutBuffer& dest, TBinaryInBuffer& src) {
            TS::Serialize(static_cast<const T&>(t), dest);
            TS::Deserialize(t, src);
            TS::SerializedSize(t);
        };

    template <typename T>
    concept CFieldSerializer = CSerializer<T, typename T::TOwner>;

    template <typename T>
    concept CDefaultSerializable = requires { TBinarySerializer<T>(); };

    template <typename T>
    concept CTriviaSerializableImpl =
        std::is_arithmetic_v<T> || std::is_enum_v<T> || std::same_as<T, bool>;

    template <typename T>
    concept CTriviaSerializable =
        CTriviaSerializableImpl<std::remove_cvref_t<T>>;

    template <typename T>
    concept CStaticContainerWithTriviaSerializable =
        (std::is_bounded_array_v<T> || TIsStdArrayV<T>) && requires(T& t) {
            { *std::begin(t) } -> CTriviaSerializable;
        };

    template <typename T>
    concept CContainerWithSerializable =
        CDefaultSerializable<typename T::value_type> &&
        CDefaultSerializable<typename T::size_type> &&
        requires(T& t, T::size_type size) {
            t.resize(size);
            t.begin();
            t.end();
        };

    //============== Binary Buffers =============================

    class TBinaryOutBuffer {
        TString Buffer;
        char* Ptr;

    public:
        TBinaryOutBuffer(size_t size) : Buffer(size, 0), Ptr(Buffer.begin()) {
        }
        template <typename T>
            requires std::is_trivially_copyable_v<T>
        void Append(const T& value) {
            memcpy(Ptr, &value, sizeof(T));
            Ptr += sizeof(T);
        }
        char* Skip(size_t size) {
            auto res = Ptr;
            Ptr += size;
            return res;
        }
        const TString& GetBuffer() const {
            return Buffer;
        }
    };

    class TBinaryInBuffer {
        const char* Ptr;

    public:
        TBinaryInBuffer(const char* ptr) : Ptr(ptr) {
        }
        template <typename T>
            requires std::is_trivially_copyable_v<T>
        void Load(T& value) {
            memcpy(&value, Ptr, sizeof(T));
            Ptr += sizeof(T);
        }
        const char* Skip(size_t size) {
            auto res = Ptr;
            Ptr += size;
            return res;
        }
    };
    //========= Fields aka struct member serialization ============

    template <CBSStruct T, typename FT, FT T::*Ptr, CSerializer<FT> TS>
    struct TFieldSerializerImpl {
        using TOwner = T;
        using TField = std::remove_reference_t<FT>;
        using TSer = TS;
        constexpr static FT T::*FPtr = Ptr;
        constexpr static bool IsOptional = false;

        static bool Serialize(const TOwner& owner, TBinaryOutBuffer& dest) {
            return TSer::Serialize(owner.*FPtr, dest);
        }

        static bool Deserialize(TOwner& owner, TBinaryInBuffer& src) {
            return TSer::Deserialize(owner.*FPtr, src);
        }

        static size_t SerializedSize(const TOwner& owner) {
            return TSer::SerializedSize(owner.*FPtr);
        }
    };

    // Optional field
    template <CBSStruct T, typename FT, std::optional<FT> T::*Ptr, CSerializer<FT> TS>
    struct TOptionalFieldSerializerImpl {
        using TOwner = T;
        using TField = std::remove_reference_t<FT>;
        using TSer = TS;
        constexpr static bool IsOptional = true;

        static bool Serialize(const TOwner& owner, TBinaryOutBuffer& dest) {
            return TSer::Serialize((owner.*Ptr).value(), dest);
        }

        static bool Deserialize(TOwner& owner, TBinaryInBuffer& src) {
            FT v;

            TSer::Deserialize(v, src);
            owner.*Ptr = v;
            return true;
        }

        static bool IsNullopt(const TOwner& owner) {
            return owner.*Ptr == std::nullopt;
        }

        static void SetDefault(TOwner& owner) {
            owner.*Ptr = std::nullopt;
        }

        static size_t SerializedSize(const TOwner& owner) {
            return TSer::SerializedSize((owner.*Ptr).value());
        }
    };

    template <auto FPrt,
              typename TS =
                  typename TUnpackMemberPtr<FPrt>::TDefaultFieldSerializer>
    using TFieldSerializer =
        typename TUnpackMemberPtr<FPrt>::template TSerializer<TS>;

    //========= Structs serialization ==================

    template <CBSStruct TOwner, typename... Fields>
    struct TStructSerializerImpl;

    template <CBSStruct TOwner, typename Field, typename... Fields>
    struct TStructSerializerImpl<TOwner, Field, Fields...> {
        using TTail = TStructSerializerImpl<TOwner, Fields...>;

        static consteval size_t OptionalFieldsSize() {
            size_t v = 0;
            if constexpr (Field::IsOptional) {
                v = 1;
            }
            if constexpr (sizeof...(Fields) > 0) {
                v += TTail::OptionalFieldsSize();
            }
            return v;
        }

        static consteval std::pair<size_t, uint8_t> OptionalBitOffsetMask() {
            size_t size = OptionalFieldsSize();
            size_t bitN = size > 0 ? size - 1 : 0;
            size_t offset = bitN / 8;
            uint8_t mask = 1 << (bitN % 8);
            return std::make_pair(offset, mask);
        }

        static constexpr size_t OptionalFieldsSizeV =
            OptionalFieldsSize();

        static void SerializeWithOptionalBuffer(const TOwner& owner,
                                          TBinaryOutBuffer& dest,
                                          char* optionalBuffer) {
            if constexpr (Field::IsOptional) {
                static constexpr auto indexMask = OptionalBitOffsetMask();
                if (Field::IsNullopt(owner)) {
                    optionalBuffer[indexMask.first] |= indexMask.second;
                } else {
                    Field::Serialize(owner, dest);
                }
            } else {
                Field::Serialize(owner, dest);
            }

            if constexpr (sizeof...(Fields) > 0) {
                TTail::SerializeWithOptionalBuffer(owner, dest, optionalBuffer);
            }
        }

        static bool Serialize(const TOwner& owner, TBinaryOutBuffer& dest) {
            Field::Serialize(owner, dest);
            if constexpr (sizeof...(Fields) != 0) {
                return TTail::Serialize(owner, dest);
            } else {
                return true;
            }
        }
        static void DeserializeWithOptionalBuff(TOwner& owner, TBinaryInBuffer& src,
                                            const char* optionalBuffer) {
            if constexpr (Field::IsOptional) {
                static constexpr auto indexMask = OptionalBitOffsetMask();
                if (!(optionalBuffer[indexMask.first] & indexMask.second)) {
                    Field::Deserialize(owner, src);
                } else {
                    Field::SetDefault(owner);
                }
            } else {
                Field::Deserialize(owner, src);
            }
            if constexpr (sizeof...(Fields) > 0) {
                TTail::DeserializeWithOptionalBuff(owner, src, optionalBuffer);
            }
        }

        static bool Deserialize(TOwner& owner, TBinaryInBuffer& src) {
            Field::Deserialize(owner, src);
            if constexpr (sizeof...(Fields) != 0) {
                TTail::Deserialize(owner, src);
            } else {
                return true;
            }
            return true;
        }

        static size_t SerializedSize(const TOwner& owner) {
            size_t res = 0;
            if constexpr (Field::IsOptional) {
                res += Field::IsNullopt(owner) ? 0 : Field::SerializedSize(owner);
            } else {
                res += Field::SerializedSize(owner);
            }

            if constexpr (sizeof...(Fields) != 0) {
                res += TTail::SerializedSize(owner);
            }
            return res;
        }
    };

    template <CFieldSerializer... Fields>
    struct TStructSerializer {
        using TOwner = TOwnerExtractor<Fields...>::TOwner;

    private:
        using TImpl = TStructSerializerImpl<TOwner, Fields...>;
        static constexpr size_t OptionalFieldsSize =
            TImpl::OptionalFieldsSize();

    public:
        static bool Serialize(const TOwner& owner, TBinaryOutBuffer& dest) {
            if constexpr (OptionalFieldsSize > 0) {
                static constexpr size_t optionalBufferSize =
                    OptionalFieldsSize / 8 + 1;
                char* optionalBuffer = dest.Skip(optionalBufferSize);
                TImpl::SerializeWithOptionalBuffer(owner, dest, optionalBuffer);

            } else {
                TImpl::Serialize(owner, dest);
            }
            return true;
        }

        static bool Deserialize(TOwner& owner, TBinaryInBuffer& src) {
            if constexpr (OptionalFieldsSize > 0) {
                static constexpr size_t optionalBufferSize =
                    OptionalFieldsSize / 8 + 1;
                const char* optionalBuffer = src.Skip(optionalBufferSize);
                TImpl::DeserializeWithOptionalBuff(owner, src, optionalBuffer);

            } else {
                TImpl::Deserialize(owner, src);
            }
            return true;
        }

        static size_t SerializedSize(const TOwner& owner) {
            size_t res = 0;
            if constexpr (OptionalFieldsSize > 0) {
                static constexpr size_t optionalBufferSize =
                    OptionalFieldsSize / 8 + 1;
                res += optionalBufferSize;

            } else {
            }
            return res + TImpl::SerializedSize(owner);
        }
    };

    //============= Functions =================
    template <typename T>
        requires CDefaultSerializable<T>
    bool Serialize(const T& value, TBinaryOutBuffer& dest) {
        return TBinarySerializer<T>::Serialize(value, dest);
    }

    template <typename T>
        requires CDefaultSerializable<T>
    bool Deserialize(T& value, TBinaryInBuffer& src) {
        return TBinarySerializer<T>::Deserialize(value, src);
    }

    template <typename T>
        requires CDefaultSerializable<T>
    size_t SerializedSize(const T& value) {
        return TBinarySerializer<T>::SerializedSize(value);
    }

    //============ Tools =======================
    //
    template <typename T, typename FT, FT T::*Ptr>
    struct TUnpackMemberPtr<Ptr> {
        using TOwner = T;
        using TField = std::remove_reference_t<FT>;
        constexpr static FT T::*FPtr = Ptr;
        using TDefaultFieldSerializer = TBinarySerializer<TField>;
        template <typename TS>
        using TSerializer = TFieldSerializerImpl<T, TField, FPtr, TS>;
    };

    template <typename T, typename FT, FT T::*Ptr>
        requires TUnpackOptional<FT>::IsOptional
    struct TUnpackMemberPtr<Ptr> {
        using TOwner = T;
        using TField = std::remove_reference_t<FT>;
        using TOrigin = TUnpackOptional<TField>::TOrigin;
        constexpr static FT T::*FPtr = Ptr;
        using TDefaultFieldSerializer = TBinarySerializer<TOrigin>;
        template <typename TS>
        using TSerializer = TOptionalFieldSerializerImpl<T, TOrigin, FPtr, TS>;
    };

    template <typename... Fields>
    struct TOwnerExtractor;

    template <typename Field, typename... Fields>
    struct TOwnerExtractor<Field, Fields...> {
        using TTail = TOwnerExtractor<Fields...>;
        using TOwner = Field::TOwner;
        constexpr static bool CheckTOwner() {
            if constexpr (sizeof...(Fields) == 0) {
                return true;
            } else {
                return std::is_same_v<TOwner, typename TTail::TOwner>;
            }
        }
        static_assert(CheckTOwner(), "Members of single class expected");
    };

    template <typename T>
    struct TUnpackOptional {
        static constexpr bool IsOptional = false;
    };

    template <typename T>
    struct TUnpackOptional<std::optional<T>> {
        static constexpr bool IsOptional = true;
        using TOrigin = T;
    };

    class TNullBuff {
    public:
        struct TNullVal {
            template <typename T>
            TNullVal& operator=(const T&) {
                return *this;
            }
        };

    private:
        size_t Size = 0;
        TNullVal Null;

    public:
        TNullBuff() = default;

        TNullBuff& operator++() {
            ++Size;
            return *this;
        }
        TNullBuff operator++(int) {
            TNullBuff res(*this);
            ++Size;
            return res;
        }
        TNullVal& operator*() {
            return Null;
        }

        const TNullVal& operator*() const {
            return Null;
        }

        ssize_t operator-(const TNullBuff& other) const {
            return Size - other.Size;
        }
    };

    //============= Chunk Serializer/Deserializer buffers =========
    //
    struct TChunkSerializerSettings {
        static constexpr size_t MaxNumberBytes =
            (sizeof(size_t) * CHAR_BIT + 6) / 7;
    };

    class TBinaryChunkSerializer {
        NActors::TChunkSerializer* Chunker;
        int CurrentChunkSize = 0;
        void* CurrentDest = nullptr;

    private:
        bool Append(const char* data, size_t len) {
            while (len) {
                if (CurrentChunkSize > 0) {
                    auto bytesToCopy = std::min<size_t>(len, CurrentChunkSize);
                    memcpy(CurrentDest, data, bytesToCopy);
                    CurrentDest = static_cast<char*>(CurrentDest) + bytesToCopy;
                    CurrentChunkSize -= bytesToCopy;
                    data += bytesToCopy;
                    len -= bytesToCopy;
                } else if (!Chunker->Next(&CurrentDest, &CurrentChunkSize)) {
                    return false;
                }
            }
            return true;
        }
        //
        // FIXME copy-paste from TEventPBBase, should be refactored
        template <typename T>
        static size_t SerializeNumberImpl(size_t num, T buffer) {
            auto begin = buffer;
            do {
                *buffer++ = (num & 0x7F) | (num >= 128 ? 0x80 : 0x00);
                num >>= 7;
            } while (num);
            return buffer - begin;
        }

    public:
        TBinaryChunkSerializer(NActors::TChunkSerializer* chunker)
            : Chunker(chunker) {
        }

        TBinaryChunkSerializer(NActors::TChunkSerializer* chunker,
                               int CurrentChunkSize, void* CurrentDest)
            : Chunker(chunker),
              CurrentChunkSize(CurrentChunkSize),
              CurrentDest(CurrentDest) {
        }

        template <typename T>
            requires std::is_trivially_copyable_v<T>
        bool Append(const T& value) {
            size_t len = sizeof(value);
            auto data = reinterpret_cast<const char*>(&value);
            return Append(data, len);
        }

        static size_t SerializeNumber(size_t num, char* buffer) {
            return SerializeNumberImpl(num, buffer);
        }

        static size_t SerializedNumberSize(size_t num) {
            return SerializeNumberImpl(num, TNullBuff());
        }

        bool AppendNumber(size_t number) {
            char buf[TChunkSerializerSettings::MaxNumberBytes];
            return Append(buf, SerializeNumber(number, buf));
        }

        bool WriteRope(const TRope& rope) {
            return Chunker->WriteRope(&rope);
        }
        bool WriteString(const TString& s) {
            return Chunker->WriteString(&s);
        }

        void Finish() {
            if (CurrentChunkSize > 0) {
                Chunker->BackUp(CurrentChunkSize);
                CurrentChunkSize = 0;
            }
        }
    };

    class TBinaryChunkDeserializer {
        TRope::TConstIterator Iter;
        TRope::TConstIterator End;

    public:
        TBinaryChunkDeserializer(TRope::TConstIterator iter,
                                 TRope::TConstIterator end)
            : Iter(iter), End(end) {
        }

        std::optional<size_t> DeserializeNumber() {
            size_t res = 0;
            size_t offset = 0;
            for (;;) {
                if (!Iter.Valid()) {
                    return std::nullopt;
                }
                const char byte = *Iter.ContiguousData();
                Iter += 1;
                res |= (static_cast<size_t>(byte) & 0x7F) << offset;
                offset += 7;
                if (!(byte & 0x80)) {
                    break;
                }
            }
            return res;
        }

        template <typename T>
            requires std::is_trivially_copyable_v<T>
        bool Load(T& value) {
            size_t len = sizeof(value);
            auto data = reinterpret_cast<char*>(&value);
            while (len) {
                if (!Iter.Valid()) {
                    return false;
                }
                const size_t bytesToCopy = std::min(Iter.ContiguousSize(), len);
                memcpy(data, Iter.ContiguousData(), bytesToCopy);
                data += bytesToCopy;
                len -= bytesToCopy;
                Iter += bytesToCopy;
            }
            return true;
        }

        std::optional<TRope> FetchRope() {
            const auto len = DeserializeNumber();
            if (!len) {
                return std::nullopt;
            }

            auto begin = Iter;

            Iter += len.value();
            if (!Iter.Valid()) {
                return std::nullopt;
            }
            return TRope(begin, Iter);
        }

        bool Check(char marker) {
            if (Iter.Valid() && *Iter.ContiguousData() == marker) {
                Iter += 1;
                return true;
            } else {
                return false;
            }
        }

        TString TailToString() {
            return TRope(Iter, End).ConvertToString();
        }
    };

    //============= Serializer realizations =============

    template <typename T>
        requires CTriviaSerializable<T> ||
                 CStaticContainerWithTriviaSerializable<T>
    struct TBinarySerializer<T> {
        static bool Serialize(const T& value, TBinaryOutBuffer& dest) {
            dest.Append(value);
            return true;
        }

        static bool Deserialize(T& value, TBinaryInBuffer& src) {
            src.Load(value);
            return true;
        }

        static size_t SerializedSize(const T& value) {
            Y_UNUSED(value);
            return sizeof(T);
        }
    };

    template <typename T>
        requires CContainerWithSerializable<T>
    struct TBinarySerializer<T> {
    private:
        using TSerializer = TBinarySerializer<typename T::value_type>;
        using TSize = T::size_type;

    public:
        static bool Serialize(const T& value, TBinaryOutBuffer& dest) {
            TSize size = value.size();
            dest.Append(size);

            for (auto& v : value) {
                TSerializer::Serialize(v, dest);
            }
            return true;
        }

        static bool Deserialize(T& value, TBinaryChunkDeserializer& ser) {
            TSize size;
            if (!ser.Load(size)) {
                return false;
            }
            value.resize(size);
            for (auto& v : value) {
                if (!TSerializer::Deserialize(v, ser)) {
                    return false;
                }
            }
            return true;
        }

        static size_t SerializedSize(const T& value) {
            return sizeof(typename T::value_type) * value.size() +
                   sizeof(TSize);
        }
    };

}  // namespace NActorsBinarySerialization

namespace NActors {
    using namespace NActorsBinarySerialization;

    template <typename TEv, CDefaultSerializable TRecord, ui32 TEventType>
    class TEventBS : public TEventBase<TEv, TEventType> {
        // a vector of data buffers referenced by record; if filled, then
        // extended serialization mechanism applies
        TVector<TRope> Payload;
        size_t TotalPayloadSize = 0;

    public:
        TRecord Record;

    public:
        TEventBS() = default;

        explicit TEventBS(const TRecord& rec) : Record(rec) {
        }

        explicit TEventBS(TRecord&& rec) : Record(rec) {
        }

        TString ToStringHeader() const override {
            return TypeName<TRecord>();
        }

        bool IsSerializable() const override {
            return true;
        }

        bool SerializeToArcadiaStream(
            TChunkSerializer* chunker) const override {
            return SerializeToArcadiaStreamImpl(chunker, TString());
        }

        ui32 CalculateSerializedSize() const override {
            size_t result = SerializedSize(Record);
            if (Payload) {
                ++result;  // marker
                result += TBinaryChunkSerializer::SerializedNumberSize(
                    Payload.size());
                for (const TRope& rope : Payload) {
                    result += TBinaryChunkSerializer::SerializedNumberSize(
                        rope.GetSize());
                }
                result += TotalPayloadSize;
            }
            return result;
        }

        static IEventBase* Load(TEventSerializedData* input) {
            THolder<TEventBS> ev(new TEv());
            if (input->GetSize()) {
                TBinaryChunkDeserializer desializer(input->GetBeginIter(),
                                                    input->GetEndIter());

                if (const auto& info = input->GetSerializationInfo();
                    info.IsExtendedFormat) {
                    // check marker
                    if (!desializer.Check(PayloadMarker)) {
                        Y_ABORT("invalid event");
                    }

                    auto numRopesOpt = desializer.DeserializeNumber();
                    if (!numRopesOpt) {
                        Y_ABORT("invalid event");
                    }

                    auto numRopes = *numRopesOpt;

                    while (numRopes--) {
                        auto rope = desializer.FetchRope();
                        if (rope) {
                            ev->Payload.push_back(std::move(rope).value());

                        } else {
                            Y_ABORT("invalid event");
                        }
                    }
                }

                // FIXME Temporary we assume here what tail of input is
                // Contiguous Data
                auto recordString = desializer.TailToString();
                TBinaryInBuffer recordBuffer(recordString.data());

                Deserialize(ev->Record, recordBuffer);
                ev->CachedByteSize = input->GetSize();
            }
            return ev.Release();
        }

        size_t GetCachedByteSize() const {
            if (CachedByteSize == 0) {
                CachedByteSize = CalculateSerializedSize();
            }
            return CachedByteSize;
        }

        ui32 CalculateSerializedSizeCached() const override {
            return GetCachedByteSize();
        }

        void InvalidateCachedByteSize() {
            CachedByteSize = 0;
        }

        TEventSerializationInfo CreateSerializationInfo() const override {
            return CreateSerializationInfoImpl(0);
        }

    public:
        void ReservePayload(size_t size) {
            Payload.reserve(size);
        }

        ui32 AddPayload(TRope&& rope) {
            const ui32 id = Payload.size();
            TotalPayloadSize += rope.size();
            Payload.push_back(std::move(rope));
            InvalidateCachedByteSize();
            return id;
        }

        const TRope& GetPayload(ui32 id) const {
            Y_ABORT_UNLESS(id < Payload.size());
            return Payload[id];
        }

        ui32 GetPayloadCount() const {
            return Payload.size();
        }

        void StripPayload() {
            Payload.clear();
            TotalPayloadSize = 0;
        }

        bool AllowExternalDataChannel() const {
            return TotalPayloadSize >= 4096;
        }

    protected:
        TEventSerializationInfo CreateSerializationInfoImpl(
            size_t preserializedSize) const {
            TEventSerializationInfo info;
            info.IsExtendedFormat = static_cast<bool>(Payload);

            if (AllowExternalDataChannel()) {
                if (Payload) {
                    char temp[MaxNumberBytes];
                    info.Sections.push_back(TEventSectionInfo{
                        0,
                        1 + TBinaryChunkSerializer::SerializeNumber(
                                Payload.size(), temp),
                        0, 0, true});  // payload marker and rope count
                    for (const TRope& rope : Payload) {
                        const size_t ropeSize = rope.GetSize();
                        info.Sections.back().Size +=
                            TBinaryChunkSerializer::SerializeNumber(ropeSize,
                                                                    temp);
                        info.Sections.push_back(TEventSectionInfo{
                            0, ropeSize, 0, 0,
                            false});  // data as a separate section
                    }
                }

                const size_t byteSize =
                    SerializedSize(Record) + preserializedSize;
                info.Sections.push_back(TEventSectionInfo{
                    0, byteSize, 0, 0, true});  // protobuf itself
            }

            return info;
        }

        bool SerializeToArcadiaStreamImpl(TChunkSerializer* chunker,
                                          const TString& preserialized) const {
            TBinaryChunkSerializer serializer(chunker);
            // serialize payload first
            if (Payload) {
                char marker = PayloadMarker;

                serializer.Append(marker);

                if (!serializer.AppendNumber(Payload.size())) {
                    return false;
                }

                for (const TRope& rope : Payload) {
                    if (!serializer.AppendNumber(rope.GetSize())) {
                        return false;
                    }
                    if (rope) {
                        serializer.Finish();
                        if (!serializer.WriteRope(rope)) {
                            return false;
                        }
                    }
                }

                serializer.Finish();
            }

            if (preserialized && !serializer.WriteString(preserialized)) {
                return false;
            }

            TBinaryOutBuffer buffer(SerializedSize(Record));
            Serialize(Record, buffer);
            serializer.WriteString(buffer.GetBuffer());

            serializer.Finish();
            return true;
        }

    protected:
        mutable size_t CachedByteSize = 0;

        static constexpr char ExtendedPayloadMarker = 0x06;
        static constexpr char PayloadMarker = 0x07;
        static constexpr size_t MaxNumberBytes =
            (sizeof(size_t) * CHAR_BIT + 6) / 7;
    };
}  // namespace NActors
