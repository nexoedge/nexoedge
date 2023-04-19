// SPDX-License-Identifier: Apache-2.0

#ifndef __CODING_OPTIONS_HH__
#define __CODING_OPTIONS_HH__

#include "../define.hh"
#include <string>

class CodingOptions {
public:
    CodingOptions();
    ~CodingOptions();

    /**
     * Set option "repair using CAR" as true
     **/
    void setRepairUsingCAR();

    /**
     * Get current value of option "repair using CAR"
     *
     * @return current value of the option
     **/
    bool repairUsingCAR();

    /**
     * Set the parameter N
     *
     * @return whether parameter N is set to the input value
     **/
    bool setN(coding_param_t n);

    /**
     * Set the parameter K
     *
     * @return whether parameter k is set to the input value
     **/
    bool setK(coding_param_t k);

    /**
     * Get the parameter N
     *
     * @return current value of parameter N
     **/
    coding_param_t getN();

    /**
     * Get the parameter N
     *
     * @return current value of parameter N
     **/
    coding_param_t getK();

    /**
     * Get a string representation of the option
     *
     * @return a string reppresentation of the option
     **/
    std::string str(bool withRuntimeOptions = false);

private:
    struct {
        coding_param_t n;                              /**< number of units in a stripe */
        coding_param_t k;                              /**< number of data units in a stripe */
    } _static;

    struct {
        bool useCarRepair;                  /**< whether to use cross-rack-aware repair */
    } _runtime;

};

#endif // define __CODING_OPTION_HH__
