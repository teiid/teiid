/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

/**
 * Class representing a time of day with no consideration of timezones, etc.
 * Just an hour, minute, and second.  Developed for charting but useful here.
 */
public class QCTime implements Serializable {
    private int hour;
    private int minute;
    private int second;
    private double secondsWithFraction;

    public QCTime(int anHour, int aMinute, int aSecond) {
        super();
        if ((anHour < 0) || (anHour > 23) || (aMinute < 0) || (aMinute > 59) || (aSecond < 0)
                || (aSecond > 59)) {
            throw new RuntimeException("illegal param passed into QCTime constructor");
        }
        hour = anHour;
        minute = aMinute;
        second = aSecond;
        secondsWithFraction = aSecond;
    }

    public QCTime(int anHour, int aMinute, double aSecond) {
        super();
        if ((anHour < 0) || (anHour > 23) || (aMinute < 0) || (aMinute > 59) || (aSecond < 0)
                || (aSecond >= 60)) {
            throw new RuntimeException("illegal param passed into QCTime constructor");
        }

        hour = anHour;
        minute = aMinute;
        secondsWithFraction = aSecond;
        second = (int)secondsWithFraction;
    }

    public QCTime(int anHour, int aMinute) {
        this(anHour, aMinute, 0);
    }

    public QCTime(int totSeconds) {
        super();
        int NUM_SECONDS_PER_DAY = 24 * 60 * 60;
        if ((totSeconds >= NUM_SECONDS_PER_DAY) || (totSeconds < 0)) {
            throw new RuntimeException("illegal param passed into QCTime constructor");
        }
        hour = totSeconds / (60 * 60);
        totSeconds -= (hour * 60 * 60);
        minute = totSeconds / 60;
        totSeconds -= (minute * 60);
        secondsWithFraction = totSeconds;
        second = totSeconds;
    }

    public int getHour() {
        return hour;
    }

    public int getMinute() {
        return minute;
    }

    public int getSecond() {
        return second;
    }

    public double getSecondsWithFraction() {
        return secondsWithFraction;
    }

    public void zeroOutSeconds() {
        second = 0;
        secondsWithFraction = 0.0;
    }
    
    public boolean equals(QCTime anotherTime) {
        return ((hour == anotherTime.hour) && (minute == anotherTime.minute) && (
                secondsWithFraction == anotherTime.secondsWithFraction));
    }

    public boolean isGreaterThan(QCTime anotherTime) {
        double temp1 = QCTime.dSecondsNumber(this);
        double temp2 = QCTime.dSecondsNumber(anotherTime);
        return (temp1 > temp2);
    }

    public boolean isLessThan(QCTime anotherTime) {
        double temp1 = QCTime.dSecondsNumber(this);
        double temp2 = QCTime.dSecondsNumber(anotherTime);
        return (temp1 < temp2);
    }

    public static int iSecondsNumber(QCTime aTime) {
        return (aTime.hour * 60 * 60 + aTime.minute * 60 + aTime.second);
    }

    public static double dSecondsNumber(QCTime aTime) {
        return (aTime.hour * 60 * 60 + aTime.minute * 60 + aTime.secondsWithFraction);
    }

    public static int minutesNumber(QCTime aTime) {
        return (aTime.hour * 60 + aTime.minute);
    }

    public static int roundedMinutesNumber(QCTime aTime) {
        int min = aTime.hour * 60 + aTime.minute;
        if (aTime.second >= 30) {
            min += 1;
        }
        return min;
    }

    /**
     * hh:mm
     */
    public String toHourMinuteString() {
        String hourString = (new Integer(hour)).toString();
        if (hour < 10) {
            hourString = "0" + hourString;
        }
        String minuteString = (new Integer(minute)).toString();
        if (minute < 10) {
            minuteString = "0" + minuteString;
        }
        return hourString + ":" + minuteString;
    }

    /**
     * hh:mmAM
     */
    public String toHourMinuteAMPMString() {
        int ourHour = hour;
        String suffix = "AM";
        if (ourHour == 0) {
            ourHour = 12;
        } else {
            if (ourHour >= 12) {
                suffix = "PM";
            }
            if (ourHour >= 13) {
                ourHour -= 12;
            }
        }
        String minuteString = (new Integer(minute)).toString();
        if (minute < 10) {
            minuteString = "0" + minuteString;
        }
        String str = ourHour + ":" + minuteString + suffix;
        return str;
    }

    /**
     * hh:mm:ss.sss
     */
    public String toHourMinuteSecondMillisecondString() {
        String str = "";
        if (hour < 10) {
            str += "0";
        }
        str += hour + ":";
        if (minute < 10) {
            str += "0";
        }
        str += minute + ":";
        if (second < 10) {
            str += "0";
        }
        str += StaticUtilities.roundToNumDecimalPlaces(secondsWithFraction, 3);
        return str;
    }

    /**
     * Return a number 0 through 999 representing the fractional second as
     * a number of milliseconds.
     */
    public int fractionalSecondAsMillis() {
        double fraction = secondsWithFraction - second;
        int millis = (int)(fraction * 1000);
        if (millis > 999) {
            millis = 999;
        } else if (millis < 0) {
            millis = 0;
        }
        return millis;
    }

    public String toString() {
        String str = "QCTime: " + hour + ":" + minute + ":" + second + "(" +
                secondsWithFraction + ")";
        return str;
    }
}
