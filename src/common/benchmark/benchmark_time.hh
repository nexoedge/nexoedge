// SPDX-License-Identifier: Apache-2.0

#ifndef __BENCHMARK_TIME_HH__
#define __BENCHMARK_TIME_HH__

#include <time.h>
#include <iostream>
#include <iomanip>
#include <vector>

#define INVALID_TV -777

/**
 * wrapper for timespec
 */
class TimeVal {

private:
    struct timespec _tv;

public:
    TimeVal();
    TimeVal(__time_t tv_sec, __time_t tv_nsec);
    TimeVal(const TimeVal &tv);
    TimeVal(const timespec &ts);

    /**
     * set TimeVal
     * @param tv_sec
     * @param tv_nsec
     *
     **/
    void set(__time_t tv_sec, __time_t tv_nsec);

    /**
     * get _tv
     *
     **/
    struct timespec &get();

    /**
     * get const _tv
     *
     **/
    const struct timespec &get_const() const;

    /**
     * check whether Timeval is valid
     *
     **/
    bool isValid();
    
    /**
     * get time in seconds
     *
     * @return                             (double) time in seconds
     **/
    double sec() const;

    /**
     * mark time
     *
     **/
    void mark();

    // operator overload
    friend std::ostream& operator<< (std::ostream& os, const TimeVal& tv);
    template <typename T> 
    friend std::ostream& operator<< (std::ostream& os, const std::vector<T>& tvs);

    TimeVal& operator=(const TimeVal &tv);
    TimeVal& operator=(const timespec &ts);
    TimeVal& operator-=(const TimeVal &tv);
    TimeVal& operator-=(const timespec &ts);
    TimeVal& operator+=(const TimeVal &tv);
    TimeVal& operator+=(const timespec &ts);
    TimeVal operator-(const TimeVal &tv) const;
    TimeVal operator-(const timespec &ts) const;
    TimeVal operator+(const TimeVal &tv) const;
    TimeVal operator+(const timespec &ts) const;
    bool operator>(const double v) const;
    bool operator>(const TimeVal &tv) const;
    bool operator>=(const double v) const;
    bool operator>=(const TimeVal &tv) const;
    bool operator<(const double v) const;
    bool operator<(const TimeVal &tv) const;
    bool operator<=(const double v) const;
    bool operator<=(const TimeVal &tv) const;
    bool operator==(const double v) const;
    bool operator==(const TimeVal &tv) const;
};


/**
 * Tag Point for one event
 * 
 * records the event's start time and end time
 * 
 **/
class TagPt {

private:
    TimeVal _startTv;                       /**< start timespec */
    TimeVal _endTv;                         /**< end timespec */

public:
    TagPt();
    TagPt(TimeVal startTv, TimeVal endTv);
    TagPt(const TagPt &tp);

    TagPt& operator=(const TagPt &tagPt);

    /**
     * mark start time
     **/
    void markStart();
    /**
     * mark end time
     **/
    void markEnd();
    /**
     * get start time
     **/
    TimeVal &getStart();
    /**
     * get end time
     **/
    TimeVal &getEnd();
    /**
     * get const start time
     **/
    const TimeVal &getStart_const() const;
    /**
     * get const end time
     **/
    const TimeVal &getEnd_const() const;
    /**
     * set start time
     **/
    void setStart(const TimeVal &tv);
    /**
     * set end time
     **/
    void setEnd(const TimeVal &tv);

    /**
     * get interval (seconds) of two TagPt
     * e.g. input [1, 5], [2, 6], return [1,6]
     * 
     * @params                             (TagPt) tag point
     * @return                             (double) time in seconds
     **/
    double interval(const TagPt &tagPt) const;
    /**
     * get interval (seconds) of two TagPt
     *
     * @params                             (TagPt) tag point 1
     * @params                             (TagPt) tag point 2
     * @return                             (double) time in seconds
     **/
    static double interval(const TagPt &tagPt1, const TagPt &tagPt2);
    /**
     * get usedTime (seconds)
     * e.g. TagPt [1, 5] return 4
     * 
     * @params                             (TagPt) tag point
     * @return                             (double) time in seconds
     **/
    double usedTime();

    /**
     * get usedTime (TimeVal)
     * e.g. TagPt [1.0, 5.0] return 4.0
     * 
     * @params                             (TagPt) tag point
     * @return                             (TimeVal) time in TimeVal
     **/
    TimeVal usedTimeTv();

    /**
     * check tagPt start time and end time is valid
     *
     **/
    bool isValid();

    friend std::ostream& operator<< (std::ostream& os, const TagPt& tagPt);
};

#endif // __BENCHMARK_TIME_HH__
