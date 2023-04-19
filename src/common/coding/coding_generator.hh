// SPDX-License-Identifier: Apache-2.0

#ifndef __CODING_GENERATOR_HH__
#define __CODING_GENERATOR_HH__

#include "all.hh"
#include "coding_options.hh"

class CodingGenerator {
public:

    /* generate coding instance for a particular n, k, coding scheme, and repair method */
    static Coding *genCoding(int codingScheme, CodingOptions options) {
        try {
            switch (codingScheme) {
            case CodingScheme::RS:
                return new RSCode(options);
            }
        } catch (std::exception &e) {
            LOG(ERROR) << "Failed to init coding, " << e.what();
            return 0;
        }
        return 0;
    }

protected:

};

#endif // define __CODING_GENERATOR_HH__
