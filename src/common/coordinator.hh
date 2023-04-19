// SPDX-License-Identifier: Apache-2.0

#ifndef __COORDINATOR_HH__
#define __COORDINATOR_HH__

#include <zmq.hpp>

#include "../ds/coordinator_event.hh"

#define NUM_SYSINFO_HIST 3

class Coordinator {
public:
    Coordinator();
    virtual ~Coordinator();

    static unsigned long int sendEventMessage(zmq::socket_t &socket, const CoordinatorEvent &event);
    static unsigned long int getEventMessage(zmq::socket_t &socket, CoordinatorEvent &event);

protected:
    static void* updateSysInfoBg(void *arg);
    void updateSysInfo();

    unsigned char checkHostType();
    /**
     * Check the host type (main action)
     * 
     * @param[in] address         address to probe
     * @param[out] type           pointer to host type (return value)
     * @param[in] expectedType    expected type of host for this check
     * @remark if expectedType == HOST_TYPE_UNKNOWN, the function should not set type to expectedType
     **/
    virtual bool checkHostTypeAction(const char *address, unsigned char *type, unsigned char expectedType = HostType::HOST_TYPE_UNKNOWN);

    /**
     * Check the Agent host type using response headers (callback for curl)
     * 
     * @param[in] buffer          header field
     * @param[in] unitSize        buffer unit size (always 1)
     * @param[in] length          buffer length
     * @param[out] userdata       pointer to host type (return value)
     **/
    static size_t checkHostTypeByHeader(char *buffer, size_t unitSize, size_t length, void *userdata);

    /**
     * Process the Agent host output (callback for curl)
     * 
     * @param[in] buffer          header field
     * @param[in] unitSize        buffer unit size (always 1)
     * @param[in] length          buffer length
     * @param[out] userdata       pointer to host type (return value)
     **/
    static size_t checkHostTypeOutput(char *buffer, size_t unitSize, size_t length, void *userdata);

    SysInfo _sysinfo[NUM_SYSINFO_HIST];
    int _latestInfoIdx;
    pthread_t _sysInfoT;

    unsigned char _hostType;                        /**< host type */

    bool _running;

private:
    static bool hasData(unsigned short opcode);

};

#endif // define __COORDINATOR_HH__
