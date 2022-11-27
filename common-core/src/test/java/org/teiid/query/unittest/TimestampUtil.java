/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
 * by specifying year, month, day, etc. without deprecation errors.
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
