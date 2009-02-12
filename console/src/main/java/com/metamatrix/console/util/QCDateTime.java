/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
 */

package com.metamatrix.console.util;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

/**
 * Class representing a date and time.  Developed for charting but useful here.
 */
public class QCDateTime implements Serializable {
    private QCDate date;
    private QCTime time;

    public QCDateTime(QCDate aDate, QCTime aTime) {
        super();
        date = aDate;
        time = aTime;
    }

    public QCDateTime(GregorianCalendar dt, boolean ignoreTime) {
        this(new QCDate(dt), ignoreTime?null:new QCTime(dt.get(Calendar.HOUR_OF_DAY), 
                dt.get(Calendar.MINUTE), dt.get(Calendar.SECOND)));
    }

    public QCDateTime(QCDate aDate) {
        this(aDate, null);
    }

    public QCDateTime(QCTime aTime) {
        this(null, aTime);
    }

    public QCDateTime(Date dt, boolean ignoreTime) {
        super();
        QCGregorianCalendar greg = new QCGregorianCalendar();
        greg.setTimeInMillis(dt.getTime());
        this.date = new QCDate(dt);
        this.time = null;
        if (!ignoreTime) {
            time = new QCTime(greg.get(Calendar.HOUR_OF_DAY),
                    greg.get(Calendar.MINUTE),
                    greg.get(Calendar.SECOND));
        }
    }

    public QCDateTime(Date dt, boolean ignoreTime, boolean ignoreSeconds) {
        this(dt, ignoreTime);
        if ((!ignoreTime) && ignoreSeconds) {
            this.time.zeroOutSeconds();
        }
    }
    
    public QCDate getDate() {
        return date;
    }

    public QCTime getTime() {
        return time;
    }

    public boolean equals(QCDateTime anotherDateTime) {
        if ((time == null) && (anotherDateTime.time != null)) {
            return false;
        }
        if ((time != null) && (anotherDateTime.time == null)) {
            return false;
        }
        if ((time == null) && (anotherDateTime.time == null)) {
            return (date.equals(anotherDateTime.date));
        }
        return (date.equals(anotherDateTime.date) &&
                time.equals(anotherDateTime.time));
    }

    public boolean isGreaterThan(QCDateTime anotherDateTime) {
        if ((time == null) && (anotherDateTime.time != null)) {
            return date.isGreaterThan(anotherDateTime.date);
        }
        if ((time != null) && (anotherDateTime.time == null)) {
            return date.isGreaterThan(anotherDateTime.date);
        }
        if ((time == null) && (anotherDateTime.time == null)) {
            return (date.isGreaterThan(anotherDateTime.date));
        }
        return (date.isGreaterThan(anotherDateTime.date) ||
                (date.equals(anotherDateTime.date) &&
                time.isGreaterThan(anotherDateTime.time)));
    }

    public boolean isLessThan(QCDateTime anotherDateTime) {
        if ((time == null) && (anotherDateTime.time != null)) {
            return date.isLessThan(anotherDateTime.date);
        }
        if ((time != null) && (anotherDateTime.time == null)) {
            return date.isLessThan(anotherDateTime.date);
        }
        if ((time == null) && (anotherDateTime.time == null)) {
            return (date.isLessThan(anotherDateTime.date));
        }
        return (date.isLessThan(anotherDateTime.date) ||
                (date.equals(anotherDateTime.date) &&
                time.isLessThan(anotherDateTime.time)));
    }

    public static int intervalNumber(QCDateTime startingDT, QCDateTime curDT, 
            int blockInt) {
        //If curDT and startingDT are the same, will assign 1, not 0.
        if ((startingDT.getTime() == null) || (curDT.getTime() == null)) {
            return -1;
        }
        if ((60 % blockInt) != 0) {
            return -1;
        }
        int dayDiff = 0;
        if ((startingDT.getDate() != null) && (curDT.getDate() != null)) {
            dayDiff = QCDate.dayNumber(curDT.getDate()) - QCDate.dayNumber(startingDT.
                    getDate());
        }
        int startingTimeMinutes = 60 * startingDT.getTime().getHour() + 
                startingDT.getTime().getMinute();
        int intervalOfStartingDay = (startingTimeMinutes / blockInt) + 1;
        int curTimeMinutes = 60 * curDT.getTime().getHour() + curDT.getTime().getMinute();
        int intervalOfCurDay = (curTimeMinutes / blockInt) + 1;
        int intervalsPerDay = 24 * (60 / blockInt);
        return (1 + (intervalOfCurDay - intervalOfStartingDay) + (intervalsPerDay * dayDiff));
    }

    public QCDateTime addSeconds(int numSeconds) {
        int NUM_SECONDS_PER_DAY = 24 * 60 * 60;
        int numSecondsCurDay = time.getSecond() + (60 * time.getMinute()) + (60 * 60 *
                time.getHour());
        int dayDelta = 0;
        numSecondsCurDay += numSeconds;
        if (numSeconds >= 0) {
            while (numSecondsCurDay >= NUM_SECONDS_PER_DAY) {
                dayDelta++;
                numSecondsCurDay -= NUM_SECONDS_PER_DAY;
            }
        } else {
            while (numSecondsCurDay < 0) {
                dayDelta--;
                numSecondsCurDay += NUM_SECONDS_PER_DAY;
            }
        }
        int newDayNumber = QCDate.dayNumber(date) + dayDelta;
        QCDate newDate = QCDate.dateOfDayNumber(newDayNumber);
        QCTime newTime = new QCTime(numSecondsCurDay);
        return new QCDateTime(newDate, newTime);
    }
   
    public GregorianCalendar toGregorian() { 
        GregorianCalendar cal = null;
        if (date == null) {
            //If date is null we will return null
        } else {
            if (time == null) {
                cal = new GregorianCalendar(date.getYear(), date.getMonth() - 1, date.getDay());
            } else {
                cal = new GregorianCalendar(date.getYear(), date.getMonth() - 1, date.getDay(),
                        time.getHour(), time.getMinute(), time.getSecond());
            }
        }
        return cal;
    }

    public GregorianCalendar setGregorian() {
        return toGregorian();
    }
    
    public Timestamp toTimestamp() {
        GregorianCalendar cal = toGregorian();
        QCGregorianCalendar gCal = new QCGregorianCalendar(cal);
        //Fraction of a second is truncated in forming a Gregorian, so add it back in
        long millis = gCal.getTimeInMillis() + getTime().fractionalSecondAsMillis();
        Timestamp ts = new Timestamp(millis);
        return ts;
    }
    
    public QCDateTime adjustForStartOfSpan() {
        //Adjusts according to these rules:
        //  if date and time are both null, do nothing
        //  else if date is null, use today for date 
        //  else if time is null, use start of day (new QCTime(0, 0)) for time
        QCDate newDate = null;
        QCTime newTime = null;
        if (!((date == null) && (time == null))) {
            if (date == null) {
                newTime = time;
                newDate = new QCDate(new Date());
            } else {
                if (time == null) {
                    newDate = date;
                    newTime = new QCTime(0, 0);
                } else {
                    newTime = time;
                    newDate = date;
                }
            }
        }
        QCDateTime newDateTime = new QCDateTime(newDate, newTime);
        return newDateTime;
    }
    
    public QCDateTime adjustForEndOfSpan() {
        //Adjusts according to these rules:
        //  if date and time are both null, do nothing
        //  else if date is null, use today for date 
        //  else if time is null, use end of day (new QCTime(23, 59, 59.999)) for time
        QCDate newDate = null;
        QCTime newTime = null;
        if (!((date == null) && (time == null))) {
            if (date == null) {
                newTime = time;
                newDate = new QCDate(new Date());
            } else {
                if (time == null) {
                    newDate = date;
                    newTime = new QCTime(23, 59, 59.999);
                } else {
                    newTime = time;
                    newDate = date;
                }
            }
        }
        QCDateTime newDateTime = new QCDateTime(newDate, newTime);
        return newDateTime;
    }
 
    public static QCDateTime dateTimeOfInterval(QCDateTime startingDT, int blockInt,
            int intervalNum) {
        if ((60 % blockInt) != 0) {
            return null;
        }
        return startingDT.addSeconds((intervalNum - 1) * (blockInt * 60));
    }

    public static int minutesNumber(QCDateTime aDateTime) {
        int dayNumber = QCDate.dayNumber(aDateTime.getDate());
        int minutesNumberInDay = QCTime.minutesNumber(aDateTime.getTime());
        int totalMinutes = (dayNumber - 1) * (24 * 60) + minutesNumberInDay;
        return totalMinutes;
    }

    public static int roundedMinutesNumber(QCDateTime aDateTime) {
        int dayNumber = QCDate.dayNumber(aDateTime.getDate());
        int minutesNumberInDay = QCTime.roundedMinutesNumber(aDateTime.getTime());
        int totalMinutes = (dayNumber - 1) * (24 * 60) + minutesNumberInDay;
        return totalMinutes;
    }
    
    public static int secondsNumber(QCDateTime aDateTime) {
        int dayNumber = QCDate.dayNumber(aDateTime.getDate());
        int secondsNumberInDay = QCTime.iSecondsNumber(aDateTime.getTime());
        int totalSeconds = (dayNumber - 1) * (24 * 60 * 60) + secondsNumberInDay;
        return totalSeconds;
    }

    public String toString() {
        String str = "QCDateTime: date=" + date + ",time=" + time;
        return str;
    }

    public String toMonthDayYearHourMinuteString() {
        String str = "";
        if (date != null) {
            str += date.toMonthDayYearString();
            if (time != null) {
                str += " ";
            }
        }
        if (time != null) {
            str += time.toHourMinuteString();
        }
        return str;
    }
}
