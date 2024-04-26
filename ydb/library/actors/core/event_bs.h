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
#include <span>
#include <type_traits>
#include <utility>

#include "event.h"
#include "event_load.h"
#include "event_pb.h"

namespace NActorsBinarySerialization {
    //============= Record definition =============
    template <typename T>
    concept CField = requires {
        typename T::FTag;
        typename T::FT;
        typename T::FTSerializer;
    };

    template <typename T>
    struct TBinarySerializer;

    template <typename Tag, typename T, typename TSerializer = TBinarySerializer<T>>
    struct TField {
        using FTag = Tag;
        using FT = T;
        using FTSerializer = TSerializer;
    };

    template <CField... Fields>
    class TRecord {};

    template <CField Field, CField... Fields>
    class TRecord<Field, Fields...> : public TRecord<Fields...> {
        using TTailRecord = TRecord<Fields...>;
        using FTag = Field::FTag;
        using FT = Field::FTag;
        constexpr static bool IsLastField = (sizeof...(Fields) == 0);

    private:
        Field::FT Value;
        friend TBinarySerializer<TRecord>;

    public:
        template <typename Tag>
        inline auto& GetValue() const {
            if constexpr (std::is_same_v<Tag, FTag>) {
                return Value;
            } else if constexpr (!IsLastField) {
                return TTailRecord::template GetValue<Tag>();
            } else {
                static_assert(!IsLastField, "Field not found");
            }
        }

        template <typename Tag, typename T>
        inline void SetValue(T&& value) {
            if constexpr (std::is_same_v<Tag, FTag>) {
                Value = std::forward<T>(value);
            } else if constexpr (!IsLastField) {
                TTailRecord::template SetValue<Tag>(std::forward<T>(value));
            } else {
                static_assert(!IsLastField, "Field not found");
            }
        }
    };

    //============= End of record definition =============

    //============= Serializer =============
    class TBinaryChunkSerializer {
        NActors::TChunkSerializer* Chunker;
        int CurrentChunkSize = 0;
        void* CurrentDest = nullptr;

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
    };
    class TBinaryChunkDeserializer {
        NActors::TRopeStream* Chunker;
        int CurrentChunkSize = 0;
        const void* CurrentSrc = nullptr;

    public:
        TBinaryChunkDeserializer(NActors::TRopeStream* chunker)
            : Chunker(chunker) {
        }

        TBinaryChunkDeserializer(NActors::TRopeStream* chunker,
                                 int CurrentChunkSize, const void* CurrentSrc)
            : Chunker(chunker),
              CurrentChunkSize(CurrentChunkSize),
              CurrentSrc(CurrentSrc) {
        }

        template <typename T>
            requires std::is_trivially_copyable_v<T>
        bool Load(T& value) {
            size_t len = sizeof(value);
            auto data = reinterpret_cast<char*>(&value);
            while (len) {
                if (CurrentChunkSize > 0) {
                    auto bytesToCopy = std::min<size_t>(len, CurrentChunkSize);
                    memcpy(data, CurrentSrc, bytesToCopy);
                    CurrentSrc = static_cast<const char*>(CurrentSrc) + bytesToCopy;
                    CurrentChunkSize -= bytesToCopy;
                    data += bytesToCopy;
                    len -= bytesToCopy;
                } else if (!Chunker->Next(&CurrentSrc, &CurrentChunkSize)) {
                    return false;
                }
            }
            return true;
        }
    };

    template <typename T>
        requires std::is_arithmetic_v<T>
    struct TBinarySerializer<T> {
        bool Serialize(const T& value, TBinaryChunkSerializer& ser) {
            return ser.Append(&value);
        }

        bool Deserialize(T& value, TBinaryChunkDeserializer& ser) {
            return ser.Load(&value);
        }
    };

    template <CField Field, CField ...Fields>
    struct TBinarySerializer<TRecord<Field, Fields...>> {
        using TCurrRecord = TRecord<Field, Fields...>;
        using TTailRecord = TRecord<Fields...>;
        using TFieldSerializer = Field::FTSerializer;

        bool Serialize(const TCurrRecord& value, TBinaryChunkSerializer& ser) {
            if (!TFieldSerializer::Serialize(value.Value, ser)) {
                return false;
            }
            return TTailRecord::Serialize(value, ser);
        }

        bool Deserialize(TCurrRecord& value, TBinaryChunkDeserializer& ser) {
            if (!TFieldSerializer::Deserialize(value.Value, ser)) {
                return false;
            }
            return TTailRecord::Deserialize(value, ser);
        }
    };

}  // namespace NActorsBinarySerialization

namespace NActors {}
