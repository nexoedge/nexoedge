// SPDX-License-Identifier: Apache-2.0

/*
 * BasicRingBuffer.hh
 * - a textbook-based ring buffer design
 *   based on William Stallings, "Operating Systems", 4 Ed, Pretice Hall.
 */

#ifndef __BasicRingBuffer_h__
#define __BasicRingBuffer_h__

#include <pthread.h>
#include <stdio.h>

template <class T> class RingBuffer {
    typedef struct {
        int len;
        T data;        
    } /* __attribute((packed)) */Buffer_t; 
    Buffer_t* buffer; 
	volatile int readIndex;
	volatile int writeIndex;
	volatile int count;				// current number of occupied elements
	volatile int max;				// capacity of the buffer
	volatile int run;
	volatile int blockOnEmpty;
	pthread_mutex_t mAccess;
	pthread_cond_t cvEmpty;
	pthread_cond_t cvFull;

public:
   
	RingBuffer(int size, bool block=true, int wSyncMinCount=1) {
		// printf("BasicRingBuffer.hh is used\n");
		if (size < 2) size = 2; // shouldn't allow value less than 2.
		buffer = new Buffer_t[size];
        //buffer = (Buffer_t*)malloc(sizeof(Buffer_t)*size);
		readIndex = 0; 
		writeIndex = 0;
		count = 0;
		max = size;
		run = true;
		blockOnEmpty = block;
		pthread_mutex_init(&mAccess, NULL);
		pthread_cond_init(&cvEmpty, NULL);
		pthread_cond_init(&cvFull, NULL);
	}

    ~RingBuffer() {
        //delete [] buffer;
    }

	inline int nextVal(int x) { return (x+1) >= max ? 0 : x+1; }
	// inline int nextVal(int x) { return (x+1)%max; }

	int Insert(T* data, int len) {
		pthread_mutex_lock(&mAccess);
		if (count == max) {
			pthread_cond_wait(&cvFull, &mAccess);
		}
        buffer[writeIndex].len = len;
		//memcpy(&(buffer[writeIndex].data), data, len);
		buffer[writeIndex].data = *data;
		writeIndex = nextVal(writeIndex);
		count++;
		pthread_cond_signal(&cvEmpty);
		pthread_mutex_unlock(&mAccess);
		return 0;
	}

	int Extract(T* data) {
		pthread_mutex_lock(&mAccess);
		if (count == 0) {
			if (!blockOnEmpty) {
				pthread_cond_signal(&cvFull);
				pthread_mutex_unlock(&mAccess);
				return -1;
			}
			pthread_cond_wait(&cvEmpty, &mAccess);
		}
		//memcpy(data, &(buffer[readIndex].data), buffer[readIndex].len);
		*data = buffer[readIndex].data;
		readIndex = nextVal(readIndex);
		count--;
		pthread_cond_signal(&cvFull);
		pthread_mutex_unlock(&mAccess);
		return 0;
	}
   
	void StopWhenEmptied() {
		pthread_mutex_lock(&mAccess);
		run = false;
		pthread_mutex_unlock(&mAccess);
		pthread_cond_signal(&cvEmpty);
	}

    bool highWaterMark() {
        return count + 1 >= max;
    }
};

#endif 
