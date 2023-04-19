// SPDX-License-Identifier: Apache-2.0

#include "container.hh"
#include "../../common/config.hh"

Container::Container (int id, unsigned long int capacity) {
    _id = id;
    _capacity = capacity;
    // create thread to update usage in the background
    _running = true;
    pthread_cond_init(&_usageUpdate.cond, NULL);
    pthread_mutex_init(&_usageUpdate.lock, NULL);
    pthread_create(&_usageUpdate.t, NULL, Container::backgroundUsageUpdate, (void *) this);
}

Container::~Container() {
    // signal the background thread for updating usage to terminate now
    _running = false;
    pthread_cond_signal(&_usageUpdate.cond);
    pthread_join(_usageUpdate.t, NULL);
    pthread_cond_destroy(&_usageUpdate.cond);
    pthread_mutex_destroy(&_usageUpdate.lock);
}

int Container::getId() {
    return _id;
}

unsigned long int Container::getUsage(bool updateNow) {
    if (updateNow)
        updateUsage();
    return _usage;
}

unsigned long int Container::getCapacity() {
    return _capacity;
}

void Container::getUsageAndCapacity(unsigned long int &usage, unsigned long int &capacity, bool updateNow) {
    if (updateNow)
        updateUsage();
    usage = _usage;
    capacity = _capacity;
}

void Container::bgUpdateUsage() {
    pthread_cond_signal(&_usageUpdate.cond);
}

void* Container::backgroundUsageUpdate(void *arg) {
    Container *c = (Container *) arg;
    while (true) {
        pthread_cond_wait(&c->_usageUpdate.cond, &c->_usageUpdate.lock);
        if (!c->_running)
            break;
        c->updateUsage();
    }
    return 0;
}
