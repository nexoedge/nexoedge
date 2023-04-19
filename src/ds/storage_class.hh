// SPDX-License-Identifier: Apache-2.0

#ifndef __STORAGE_CLASS_HH__
#define __STORAGE_CLASS_HH__

#include <string>
#include <typeinfo>

#include "../common/define.hh"
#include "../common/coding/all.hh"

struct StorageClass {
public:
    StorageClass(std::string name, int f, int maxChunkSize, int coding, Coding *codingInstance) : 
            _name(name),
            _codingMeta(coding, codingInstance->getN(), codingInstance->getK(), maxChunkSize, f),
            _codingInstance(codingInstance)
    {
    }

    ~StorageClass() {}

    std::string getName() const {
        return _name;
    }

    Coding* getCodingInstance() const {
        return _codingInstance;
    }

    int getCodingScheme() const {
        return _codingMeta.coding;
    }

    CodingMeta getCodingMeta() const {
        return _codingMeta;
    }

    bool operator == (const StorageClass &y) {
        return _codingMeta == y._codingMeta;
    }

private:
    std::string _name;                          /**< storage class name */
    CodingMeta _codingMeta;                     /**< coding metadata */
    Coding *_codingInstance;                    /**< coding instance */
};

#endif // ifdef __STORAGE_CLASS_HH__
