// SPDX-License-Identifier: Apache-2.0

#include "benchmark_time.hh"

/******************************************* TimeVal ******************************/

TimeVal::TimeVal() {
    _tv.tv_sec = INVALID_TV;
    _tv.tv_nsec = INVALID_TV;
}

TimeVal::TimeVal(__time_t tv_sec, __time_t tv_nsec) {
    _tv.tv_sec = tv_sec;
    _tv.tv_nsec = tv_nsec;
}

TimeVal::TimeVal(const TimeVal &tv) {
    _tv.tv_sec = tv.get_const().tv_sec;
    _tv.tv_nsec = tv.get_const().tv_nsec;
}

TimeVal::TimeVal(const timespec &ts) {
    _tv.tv_sec = ts.tv_sec;
    _tv.tv_nsec = ts.tv_nsec;
}

void TimeVal::set(__time_t tv_sec, __time_t tv_nsec) {
    _tv.tv_sec = tv_sec;
    _tv.tv_nsec = tv_nsec;
}

struct timespec &TimeVal::get() {
    return _tv;
}

const struct timespec &TimeVal::get_const() const {
    return _tv;
}

bool TimeVal::isValid() {
    return (_tv.tv_sec != INVALID_TV && _tv.tv_nsec != INVALID_TV);
}

double TimeVal::sec() const {
    return (double)(_tv.tv_sec + _tv.tv_nsec / 1000000000.0);
}

void TimeVal::mark() {
    clock_gettime(CLOCK_REALTIME, &_tv); // mark time
}

TimeVal& TimeVal::operator=(const TimeVal &tv) {
    _tv.tv_sec = tv.get_const().tv_sec;
    _tv.tv_nsec = tv.get_const().tv_nsec;
    return *this;
}

TimeVal& TimeVal::operator=(const timespec &ts) {
    _tv.tv_sec = ts.tv_sec;
    _tv.tv_nsec = ts.tv_nsec;
    return *this;
}

TimeVal& TimeVal::operator-=(const TimeVal &tv) {
    *this -= tv.get_const();
    return *this;
}

TimeVal& TimeVal::operator+=(const TimeVal &tv) {
    *this += tv.get_const();
    return *this;
}

TimeVal& TimeVal::operator-=(const timespec &ts) {
    _tv.tv_sec -= ts.tv_sec;
    _tv.tv_nsec -= ts.tv_nsec;
    if (_tv.tv_nsec < 0) {
        _tv.tv_sec -= 1;
        _tv.tv_nsec += 1000000000;
    }
    return *this;
}

TimeVal& TimeVal::operator+=(const timespec &ts) {
    _tv.tv_sec += ts.tv_sec;
    _tv.tv_nsec += ts.tv_nsec;
    if (_tv.tv_nsec > 1000000000) {
        _tv.tv_sec += 1;
        _tv.tv_nsec -= 1000000000;
    }
    return *this;
}

TimeVal TimeVal::operator-(const TimeVal &tv) const {
    TimeVal _ret = *this;
    _ret -= tv;
    return _ret;
}

TimeVal TimeVal::operator-(const timespec &ts) const {
    TimeVal _ret = *this;
    _ret -= ts;
    return _ret;
}

TimeVal TimeVal::operator+(const TimeVal &tv) const {
    TimeVal _ret = *this;
    _ret += tv;
    return _ret;
}

TimeVal TimeVal::operator+(const timespec &ts) const {
    TimeVal _ret = *this;
    _ret += ts;
    return _ret;
}

std::ostream& operator<< (std::ostream& os, const TimeVal& tv) {
    return os << std::setprecision (15) << tv.sec();
}

template <typename T> 
std::ostream& operator<< (std::ostream& os, const std::vector<T>& tvs) { 
    int size = static_cast<int>(tvs.size());
    os << "["; 
    for (int i = 0; i < size; ++i) { 
        os << tvs[i]; 
        if (i != size - 1) 
            os << ", "; 
    } 
    os << "]\n"; 
    return os; 
}

// >
bool TimeVal::operator>(const double v) const {
    return sec() > v;
}

bool TimeVal::operator>(const TimeVal &tv) const {
    return (*this - tv) > 0;
}

// >=
bool TimeVal::operator>=(const double v) const {
    return sec() >= v;
}

bool TimeVal::operator>=(const TimeVal &tv) const {
    return (*this - tv) >= 0;
}

// <
bool TimeVal::operator<(const double v) const {
    return sec() < v;
}

bool TimeVal::operator<(const TimeVal &tv) const {
    return (*this - tv) < 0;
}

// <=
bool TimeVal::operator<=(const double v) const {
    return sec() <= v;
}

bool TimeVal::operator<=(const TimeVal &tv) const {
    return (*this - tv) <= 0;
}

// ==
bool TimeVal::operator==(const double v) const {
    return sec() == v;
}

bool TimeVal::operator==(const TimeVal &tv) const {
    return (*this - tv) == 0;
}


/******************************************* TagPt ******************************/

TagPt::TagPt() {
    _startTv = TimeVal();
    _endTv = TimeVal();
}

TagPt::TagPt(TimeVal startTv, TimeVal endTv) {
    _startTv = startTv;
    _endTv = endTv;
}

TagPt::TagPt(const TagPt &tagPt) {
    _startTv = tagPt._startTv;
    _endTv = tagPt._endTv;
}

TagPt& TagPt::operator=(const TagPt &tagPt) {
    this->setStart(tagPt.getStart_const());
    this->setEnd(tagPt.getEnd_const());
    return *this;
}

void TagPt::markStart() {
    _startTv.mark();
}

void TagPt::markEnd() {
    _endTv.mark();
}

TimeVal &TagPt::getStart() {
    return _startTv;
}

TimeVal &TagPt::getEnd() {
    return _endTv;
}

const TimeVal &TagPt::getStart_const() const {
    return _startTv;
}

const TimeVal &TagPt::getEnd_const() const {
    return _endTv;
}

void TagPt::setStart(const TimeVal &tv) {
    _startTv = tv;
}

void TagPt::setEnd(const TimeVal &tv) {
    _endTv = tv;
}

double TagPt::interval(const TagPt &tagPt) const {
    TimeVal start_time = _startTv < tagPt._startTv ? _startTv : tagPt._startTv;
    TimeVal end_time = _endTv > tagPt._endTv ? _endTv : tagPt._endTv;
    return (end_time - start_time).sec(); 
}

double TagPt::interval(const TagPt &tagPt1, const TagPt &tagPt2) {
    return tagPt1.interval(tagPt2);
}

double TagPt::usedTime() {
    return (_endTv - _startTv).sec();
}

TimeVal TagPt::usedTimeTv() {
    return _endTv - _startTv;
}

bool TagPt::isValid() {
    if (!_startTv.isValid() || !_endTv.isValid())
        return false;
    if (usedTime() < 0) {
        return false;
    }
    return true;
}

std::ostream& operator<< (std::ostream& os, const TagPt& tagPt) {
    return os << std::setprecision (15) << "[TagPt] _startTv: " << tagPt._startTv << " _endTv: " << tagPt._endTv;
}
