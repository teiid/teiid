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
import java.util.Date;

/**
 * Class which holds a time amount as a number of days, hours, minutes, and seconds.
 */
public class DaysHoursMinutesSeconds implements Serializable {

    public static void printMethodCallDurationMessage(String methodName,
            Date startingTime, Date endingTime) {
        long startingTimeAsLong = startingTime.getTime();
        long endingTimeAsLong = endingTime.getTime();
        long timeDiffInMilliseconds = endingTimeAsLong - startingTimeAsLong;
        DaysHoursMinutesSeconds dhms = new DaysHoursMinutesSeconds(
                timeDiffInMilliseconds);
        String timeAsString = dhms.toDisplayString(true);
        String message = ">>>>Call to " + methodName + " took " + timeAsString;
        System.err.println(message);
    }

    //Number of days
    private int days;

    //Number of hours
    private int hours;

    //Number of minutes
    private int minutes;

    //Number of seconds
    private int seconds;

    //Number of seconds with decimal
    private float secondsWithDecimal;

    /**
     * Constructor.
     *
     * @param d     number of days
     * @param h     number of hours
     * @param m     number of minutes
     * @param s     number of seconds
     */
    public DaysHoursMinutesSeconds(int d, int h, int m, int s) {
        super();
        days = d;
        hours = h;
        minutes = m;
        seconds = s;
        secondsWithDecimal = seconds;
    }

    /**
     * Constructor using 0 for seconds.
     *
     * @param d     number of days
     * @param h     number of hours
     * @param m     number of minutes
     */
    public DaysHoursMinutesSeconds(int d, int h, int m) {
        this(d, h, m, 0);
    }

    /**
     * Constructor converting a number of seconds to a number of days, hours,
     * minutes, and seconds.
     *
     * @param seconds   total number of seconds
     */
    public DaysHoursMinutesSeconds(int seconds) {
        super();
        //Convert into days, hours, minutes, and seconds
        int numDays = (seconds / (24 * 60 * 60));
        int remainingSeconds = seconds - (numDays * (24 * 60 * 60));
        int numHours = (remainingSeconds / (60 * 60));
        remainingSeconds -= numHours * (60 * 60);
        int numMinutes = (remainingSeconds / 60);
        remainingSeconds -= numMinutes * 60;
        days = numDays;
        hours = numHours;
        minutes = numMinutes;
        seconds = remainingSeconds;
        secondsWithDecimal = seconds;
    }

    /**
     * Constructor converting a number of milliseconds to a number of days, hours,
     * minutes, and seconds.  Seconds will have a decimal portion.
     *
     * @param milliseconds   total number of milliseconds
     */
    public DaysHoursMinutesSeconds(long milliseconds) {
        super();
        //Convert into days, hours, minutes, and seconds
        int numDays = (int)(milliseconds / (24 * 60 * 60 * 1000));
        long remainingMilliseconds = milliseconds - (numDays * (24 * 60 * 60 * 1000));
        int numHours = (int)(remainingMilliseconds / (60 * 60 * 1000));
        remainingMilliseconds -= numHours * (60 * 60 * 1000);
        int numMinutes = (int)(remainingMilliseconds / (60 * 1000));
        remainingMilliseconds -= numMinutes * (60 * 1000);
        days = numDays;
        hours = numHours;
        minutes = numMinutes;
        secondsWithDecimal = (((float)remainingMilliseconds) / 1000);
        seconds = (int)secondsWithDecimal;
    }

    /**
     * Return number of days.
     *
     * @return      number of days
     */
    public int getDays() {
        return days;
    }

    /**
     * Return number of hours.
     *
     * @return      number of hours
     */
    public int getHours() {
        return hours;
    }

    /**
     * Return number of minutes.
     *
     * @return      number of minutes
     */
    public int getMinutes() {
        return minutes;
    }

    /**
     * Return number of seconds.
     *
     * @return      number of seconds
     */
    public int getSeconds() {
        return seconds;
    }

    /**
     * Return number of seconds including decimal portion.
     *
     * @return      number of seconds including decimal portion
     */
    public float getSecondsWithDecimal() {
        return secondsWithDecimal;
    }

    /**
     * Display the time amount in the form of:
     * <PRE>
     *   "d days, h hours, m minutes"
     * or
     *   "d days, h hours, m minutes, s seconds"
     * where:
     *   leading time units whose value is 0 are not displayed
     *    ex:  "5 hours, 10 minutes" rather than "0 days, 5 hours, 10 minutes"
     * and:
     *   a value of 1 is given as a singular.  ("1 day", not "1 days").
     * </PRE>
     *
     * @param includeSeconds        true if including seconds
     * @return                      the String representation as described
     */
    public String toDisplayString(boolean includeSeconds) {
        String str = "";
        if (days > 0) {
            str += days;
            if (days == 1) {
                str += " day, ";
            } else {
                str += " days, ";
            }
        }
        if ((days > 0) || (hours > 0)) {
            str += hours;
            if (hours == 1) {
                str += " hour, ";
            } else {
                str += " hours, ";
            }
        }
        if ((days > 0) || (hours > 0) || (minutes > 0) || (!includeSeconds)) {
            str += minutes;
            if (minutes == 1) {
                str += " minute";
            } else {
                str += " minutes";
            }
            if (includeSeconds) {
                str += ", ";
            }
        }
        if (includeSeconds) {
            float intSecondsAsFloat = seconds;
            if (intSecondsAsFloat == secondsWithDecimal) {
                str += seconds;
                if (seconds == 1) {
                    str += " second";
                } else {
                    str += " seconds";
                }
            } else {
                str += secondsWithDecimal + " seconds";
            }
        }
        return str;
    }

//Overridden methods:

    /**
     * Overridden toString().
     *
     * @return      String representation of the object
     */
    public String toString() {
        String str = "DaysHoursMinutesSeconds: days="+days+",hours="+hours+",minutes="+minutes+
                ",seconds="+seconds;
        return str;
    }
}
