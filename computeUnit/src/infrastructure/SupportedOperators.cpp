#include <infrastructure/SupportedOperators.hpp>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

bool SupportedOperators::isSet(SupportedOperators::SupportedClass flag) const {
    const size_t index = static_cast<std::underlying_type_t<SupportedClass>>(flag);
    return flagset.test(index);
}

bool SupportedOperators::toggle(SupportedOperators::SupportedClass flag) {
    const size_t index = static_cast<std::underlying_type_t<SupportedClass>>(flag);
    flagset.flip(index);
    return flagset.test(index);
}

std::string SupportedOperators::to_string() const {
    std::stringstream ss;
    auto appendIfSet = [this](SupportedClass flag, const std::string& info, std::vector<std::string>& infos) {
        if (isSet(flag)) {
            infos.push_back(info);
        }
    };

    std::vector<std::string> infos;
    for (const auto& flag : supportedClassArray) {
       appendIfSet(flag, supportedClassToString(flag), infos);
    }

    for ( size_t i = 0; i < infos.size(); ++i) {
        ss << infos[i];
        if ( i < infos.size()-1) {
            ss << ", ";
        }
    }

    return ss.str();
}
