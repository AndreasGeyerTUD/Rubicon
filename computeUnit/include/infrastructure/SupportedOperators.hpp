#pragma once

#include <array>
#include <bitset>
#include <cstddef>
#include <cstdint>
#include <stdexcept>

/* Helper Function to create an array with all values from the SupportedClass enum */
template <class EnumT, typename base_type = typename std::underlying_type<EnumT>, base_type... N>
static constexpr auto create_array_from_enum(std::integer_sequence<base_type, N...>) {
    return std::array<EnumT, sizeof...(N)>{static_cast<EnumT>(N)...};
}

/**
 * @brief A class wrapper for flags, that indicate if a unit supports specific operator classes.
 *
 */
class SupportedOperators {
   public:
    using flags_t = uint32_t;

    /* Be sure to adapt the supportedClassToString method when changing Enum values */
    enum class SupportedClass : flags_t {
        FilterOperations = 0,
        SetOperations,
        MaterializeOperations,
        FetchOperations,
        AggregateOperations,
        JoinOperations,
        GroupOperations,
        SortOperations,
        MapOperations,
        TestDataGeneration,
        DataTransfer,
        _size
    };

    static std::string supportedClassToString(SupportedClass flag) {
        switch (flag) {
            case SupportedClass::FilterOperations:
                return "FilterOperations";
            case SupportedClass::SetOperations:
                return "SetOperations";
            case SupportedClass::MaterializeOperations:
                return "MaterializeOperations";
            case SupportedClass::FetchOperations:
                return "FetchOperations";
            case SupportedClass::AggregateOperations:
                return "AggregateOperations";
            case SupportedClass::GroupOperations:
                return "GroupOperations";
            case SupportedClass::SortOperations:
                return "SortOperations";
            case SupportedClass::JoinOperations:
                return "JoinOperations";
            case SupportedClass::MapOperations:
                return "MapOperations";
            case SupportedClass::TestDataGeneration:
                return "TestDataGeneration";
            case SupportedClass::DataTransfer:
                return "DataTransfer";
            default:
                throw std::invalid_argument("Unimplemented SupportedClass flag");
        }
    };

    static const size_t ClassEnumSize = static_cast<flags_t>(SupportedClass::_size);
    static constexpr std::array<SupportedClass, ClassEnumSize> supportedClassArray = create_array_from_enum<SupportedClass>(std::make_integer_sequence<flags_t, ClassEnumSize>{});

    // Creating an std::bitset with the size of SupportedClass flags, excluding the _size flag
    using flagset_t = std::bitset<ClassEnumSize>;
    flagset_t flagset;

    SupportedOperators() = default;

    bool isSet(SupportedOperators::SupportedClass flag) const;

    template <typename... OpFlags>
    SupportedOperators(OpFlags&&... flags) {
        (flagset.set(static_cast<flags_t>(flags), true), ...);
    }

    template <typename... OpFlags>
    void set(OpFlags&&... flags) {
        (flagset.set(static_cast<flags_t>(flags), true), ...);
    }

    template <typename... OpFlags>
    void unset(OpFlags&&... flags) {
        (flagset.set(static_cast<flags_t>(flags), false), ...);
    }

    bool toggle(SupportedOperators::SupportedClass flag);

    std::string to_string() const;
};