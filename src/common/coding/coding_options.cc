// SPDX-License-Identifier: Apache-2.0

#include "coding_options.hh"
#include "../config.hh"

CodingOptions::CodingOptions() {
    Config &config = Config::getInstance();
    _static.k = config.getK();
    _static.n = config.getN();
    _runtime.useCarRepair = config.isRepairUsingCAR();
}

CodingOptions::~CodingOptions() {
}

void CodingOptions::setRepairUsingCAR() {
    _runtime.useCarRepair = true;
}

bool CodingOptions::repairUsingCAR() {
    return _runtime.useCarRepair;
}

bool CodingOptions::setK(coding_param_t k) {
    if (k == 0)
        return false;

    _static.k = k;

    return true;
}

coding_param_t CodingOptions::getK() {
    return _static.k;
}

bool CodingOptions::setN(coding_param_t n) {
    if (n == 0)
        return false;

    _static.n = n;

    return true;
}

coding_param_t CodingOptions::getN() {
    return _static.n;
}

std::string CodingOptions::str(bool withRuntimeOptions) {
    std::string options;
    options.append(std::to_string(_static.n)).append("-")
            .append(std::to_string(_static.k));

    if (withRuntimeOptions) {
        options.append(std::to_string(_runtime.useCarRepair));
    }

    return options;
}
