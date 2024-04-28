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

    template <typename T>
    struct TBinarySerializer;

    template <auto FPtr>
    struct TUnpackMemberPtr;

    template <typename... Fields>
    struct TOwnerExtractor;

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
        requires(T& t, TBinaryChunkSerializer& s, TBinaryChunkDeserializer& d) {
            TS::Serialize(static_cast<const T&>(t), s);
            TS::Deserialize(t, d);
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

    //========= Fields aka struct member serialization ============

    template <CBSStruct T, typename FT, FT T::*Ptr, CSerializer<FT> TS>
    struct TFieldSerializerImpl {
        using TOwner = T;
        using TField = std::remove_reference_t<FT>;
        using TSer = TS;
        constexpr static FT T::*FPtr = Ptr;

        static bool Serialize(const TOwner& owner,
                              TBinaryChunkSerializer& ser) {
            return TSer::Serialize(owner.*FPtr, ser);
        }

        static bool Deserialize(TOwner& owner, TBinaryChunkDeserializer& ser) {
            return TSer::Deserialize(owner.*FPtr, ser);
        }

        static size_t SerializedSize(const TOwner& owner) {
            return TSer::SerializedSize(owner.*FPtr);
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

        static bool Serialize(const TOwner& owner,
                              TBinaryChunkSerializer& ser) {
            if (Field::Serialize(owner, ser)) {
                if constexpr (sizeof...(Fields) != 0) {
                    return TTail::Serialize(owner, ser);
                } else {
                    return true;
                }
            } else {
                return false;
            }
        }

        static bool Deserialize(TOwner& owner, TBinaryChunkDeserializer& ser) {
            if (Field::Deserialize(owner, ser)) {
                if constexpr (sizeof...(Fields) != 0) {
                    return TTail::Deserialize(owner, ser);
                } else {
                    return true;
                }
            } else {
                return false;
            }
        }

        static size_t SerializedSize(const TOwner& owner) {
            size_t res = Field::SerializedSize(owner);

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

    public:
        static bool Serialize(const TOwner& owner,
                              TBinaryChunkSerializer& ser) {
            return TImpl::Serialize(owner, ser);
        }

        static bool Deserialize(TOwner& owner, TBinaryChunkDeserializer& ser) {
            return TImpl::Deserialize(owner, ser);
        }

        static size_t SerializedSize(const TOwner& owner) {
            return TImpl::SerializedSize(owner);
        }
    };

    //============= Functions =================
    template <typename T>
        requires CDefaultSerializable<T>
    bool Serialize(const T& value, TBinaryChunkSerializer& ser) {
        return TBinarySerializer<T>::Serialize(value, ser);
    }

    template <typename T>
        requires CDefaultSerializable<T>
    bool Deserialize(T& value, TBinaryChunkDeserializer& ser) {
        return TBinarySerializer<T>::Deserialize(value, ser);
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
        TBinaryChunkDeserializer(TRope::TConstIterator iter, TRope::TConstIterator end)
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
                Iter+=1;
                return true;
            } else {
                return false;
            }

        }
    };

    //============= Serializer realizations =============

    template <typename T>
        requires CTriviaSerializable<T> ||
                 CStaticContainerWithTriviaSerializable<T>
    struct TBinarySerializer<T> {
        static bool Serialize(const T& value, TBinaryChunkSerializer& ser) {
            return ser.Append(value);
        }

        static bool Deserialize(T& value, TBinaryChunkDeserializer& ser) {
            return ser.Load(value);
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
        static bool Serialize(const T& value, TBinaryChunkSerializer& ser) {
            TSize size = value.size();
            if (!ser.Append(size)) {
                return false;
            }
            for (auto& v : value) {
                if (!TSerializer::Serialize(v, ser)) {
                    return false;
                }
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
                TBinaryChunkDeserializer desializer(input->GetBeginIter(), input->GetEndIter());

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

                if (!Deserialize(ev->Record, desializer)) {
                    Y_ABORT("Failed to parse protobuf event type %" PRIu32 "class %s", TEventType, TypeName(ev->Record).data());
                }
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
                        0, 1 + TBinaryChunkSerializer::SerializeNumber(Payload.size(), temp), 0, 0,
                        true});  // payload marker and rope count
                    for (const TRope& rope : Payload) {
                        const size_t ropeSize = rope.GetSize();
                        info.Sections.back().Size +=
                            TBinaryChunkSerializer::SerializeNumber(ropeSize, temp);
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

            if (!Serialize(Record, serializer)) {
                return false;
            }

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
