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

/*
 * Date: Jun 16, 2004
 * Time: 1:47:51 PM
 */
package org.teiid.query.unittest;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.TimeZone;

/**
 * TimestampUtil
 *
 * <p>Allows creation of <code>java.sql.Timestamp</code>, <code>java.sql.Date</code>, <code>java.sql.Time</code>
 * by specifying year, month, day, etc. without deprecation errors.</p>
 */
public class TimestampUtil {

    private static ThreadLocal<Calendar> CAL = new ThreadLocal<Calendar>() {
    	@Override
    	protected Calendar initialValue() {
    		return Calendar.getInstance();
    	}
    };

    /**
     * Replaces deprecated <code>java.sql.Timestamp</code> constructor.
     *
     * @param year   year-1900
     * @param month  0 to 11
     * @param date   1 to 31
     * @param hour   0 to 23
     * @param minute 0 to 59
     * @param second 0 to 59
     * @param nano   0 to 999,999,999
     * @see java.sql.Timestamp
     * @return A <code>Timestamp</code>
     */
    public static Timestamp createTimestamp(int year, int month, int date, int hour, int minute, int second, int nano) {
        primeCalendar();
        CAL.get().set(year + 1900, month, date, hour, minute, second);
        long millis = CAL.get().getTimeInMillis();
        Timestamp ts = new Timestamp(millis);
        ts.setNanos(nano);
        return ts;
    }

    /**
     * Replaces deprecated <code>java.sql.Date</code> constructor.
     *
     * @param   year    the year minus 1900.
     * @param   month   the month between 0-11.
     * @param   date    the day of the month between 1-31.
     * @see java.sql.Date
     * @return A <code>Date</code>
     */
   public static Date createDate(int year, int month, int date) {
        primeCalendar();
        CAL.get().set(year + 1900, month, date);
        return new Date(CAL.get().getTimeInMillis());
   }

    /**
     * Replaces deprecated <code>java.sql.Time</code> constructor.
     *
     * @param hour 0 to 23
     * @param minute 0 to 59
     * @param second 0 to 59
     * @see java.sql.Time
     * @return A <code>Time</code>
     */
    public static Time createTime(int hour, int minute, int second) {
        primeCalendar();
        CAL.get().set(Calendar.HOUR_OF_DAY, hour);
        CAL.get().set(Calendar.MINUTE, minute);
        CAL.get().set(Calendar.SECOND, second);
        return new Time(CAL.get().getTimeInMillis());
    }

    private static void primeCalendar() {
        CAL.get().setTimeZone(TimeZone.getDefault());
        CAL.get().clear();
    }

}
