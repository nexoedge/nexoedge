// SPDX-License-Identifier: Apache-2.0

#ifndef __BLOCK_LOCATION_HH__
#define __BLOCK_LOCATION_HH__

#include <string>
#include <nlohmann/json.hpp>
#include <glog/logging.h>

#include <climits>

class BlockLocation {
public:

    class InObjectLocation {
    public:
        InObjectLocation() {
            reset();
        }

        InObjectLocation(unsigned long int ofs, unsigned int len) {
            set(ofs, len);
        }

        void reset() {
            set(ULONG_MAX, UINT_MAX);
        }

        void set(unsigned long int ofs, unsigned int len) {
            _offset = ofs;
            _length = len;
        }

        bool isInvalid() const {
            return _offset == ULONG_MAX && _length == UINT_MAX;
        }

        bool operator< (const BlockLocation::InObjectLocation &rhs) const {
            return _offset < rhs._offset;
        }

        bool operator== (const BlockLocation::InObjectLocation &rhs) const {
            return _offset == rhs._offset && _length == rhs._length;
        }

        unsigned long int _offset;                        /**< Block offset in the object **/
        unsigned int _length;                             /**< Block length **/

        unsigned long int getOfs() const {
            return _offset;
        }

        unsigned int getLen() const {
            return _length;
        }
    };

    BlockLocation() {
        reset();
    }

    BlockLocation (unsigned char namespaceId, std::string name, int version, unsigned long int ofs, unsigned int len) {
        setObjectID(namespaceId, name, version);
        setBlockRange(ofs, len);
    }

    ~BlockLocation() {
    }

    void reset() {
        setObjectID(UCHAR_MAX, "", -1);
        _inObjectLocation.reset();
    }

    bool operator== (const BlockLocation &rhs) const {
        return 
            _inObjectLocation == rhs._inObjectLocation
            && _object.namespaceId == rhs._object.namespaceId
            && _object.name == rhs._object.name
            && _object.version == rhs._object.version
        ;
    }

    // setters

    void setObjectID (unsigned char namespaceId, std::string name, int version) {
        _object.namespaceId = namespaceId;
        _object.name = name;
        _object.version = version;
    }

    void setBlockRange (unsigned long int ofs, unsigned int len) {
        _inObjectLocation.set(ofs, len);
    }

    void setBlockRange (const BlockLocation::InObjectLocation loc) {
        _inObjectLocation = loc;
    }

    // getters

    inline unsigned char getObjectNamespaceId() const {
        return _object.namespaceId;
    }

    inline std::string getObjectName() const {
        return _object.name;
    }

    inline int getObjectVersion() const {
        return _object.version;
    }

    inline unsigned long int getBlockOffset() const {
        return _inObjectLocation._offset;
    }

    inline unsigned int getBlockLength() const {
        return _inObjectLocation._length;
    }

    inline InObjectLocation getBlockRange() const {
        return _inObjectLocation;
    }

    inline std::string getObjectID() const {
        return std::to_string(_object.namespaceId).append("_").append(_object.name).append("_").append(std::to_string(_object.version));
    }

    bool isInvalid() const {
        return _object.namespaceId == UCHAR_MAX && _object.name == "" && _object.version == -1
            && _inObjectLocation.isInvalid();
    }

    std::string print() const {
        std::string output;
        return output
              .append("namespaceId = ").append(std::to_string(_object.namespaceId)).append("; ")
              .append("name = ").append(_object.name).append("; ")
              .append("version = ").append(std::to_string(_object.version)).append("; ")
              .append("offset = ").append(std::to_string(_inObjectLocation._offset)).append("; ")
              .append("length = ").append(std::to_string(_inObjectLocation._length)).append("; ")
        ;
    }

    // serializer and deseralizer

    std::string toString() const {
        return toJson().dump();
    }

    nlohmann::json toJson() const {
        nlohmann::json j;
        j["obj_name"] = _object.name;
        j["obj_nsid"] = _object.namespaceId;
        j["obj_ver"] = _object.version;
        j["obj_ofs"] = _inObjectLocation._offset;
        j["obj_len"] = _inObjectLocation._length;
        return j;
    }

    bool fromString(std::string s) {
        nlohmann::json j;
        try {
            j = nlohmann::json::parse(s);
            return fromObject(j);
        } catch (std::exception &e) {
            return false;
        }
        return true;
    }

    bool fromObject(const nlohmann::json j) {
        try {
            _object.name = j["obj_name"].get<std::string>();
            _object.namespaceId = j["obj_nsid"].get<unsigned char>();
            _object.version = j["obj_ver"].get<int>();
            _inObjectLocation._offset = j["obj_ofs"].get<unsigned long int>();
            _inObjectLocation._length = j["obj_len"].get<unsigned int>();
        } catch (std::exception &e) {
            DLOG(INFO) << "Failed to parse object location " << e.what();
            return false;
        }
        return true;
    }

    bool ifEqual(const BlockLocation& rhs){
        return *this == rhs;
    }

private:

    struct {
        unsigned char namespaceId;                    /**< Object namespace ID **/
        std::string name;                             /**< Object name **/
        int version;                                  /**< Object version **/
    } _object;

    InObjectLocation _inObjectLocation;               /**< Block location inside the object */

};

#endif // define __BLOCK_LOCATION_HH__
