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

package org.teiid.core.util;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.TimeZone;

import org.teiid.core.types.DataTypeManager;


/**
 * Utility methods for SQL Timestamps, Time, and Dates with time zones as UTC
 *
 * This is intended to take incoming Strings or Dates that have accurate
 * Calendar fields and give the UTC time by interpretting those fields
 * in the target time zone.
 *
 * Use of the Calendar object passed in will not be thread safe, but
 * it will not alter the contents of the Calendar.
 *
 * Note that normalization occurs only for the transition from one type to another.
 *
 */
public class TimestampWithTimezone {

    public static final String ISO8601_WEEK_PROP = "org.teiid.iso8601Week"; //$NON-NLS-1$
    public static final boolean ISO8601_WEEK = PropertiesUtils.getHierarchicalProperty(ISO8601_WEEK_PROP, true, Boolean.class);

    private static ThreadLocal<Calendar> CALENDAR = new ThreadLocal<Calendar>() {
        protected Calendar initialValue() {
            return initialCalendar();
        }
    };

    public static Calendar getCalendar() {
        return CALENDAR.get();
    }

    public static void resetCalendar(TimeZone tz) {
        TimeZone.setDefault(tz);
        CALENDAR.set(initialCalendar());
    }

    static Calendar initialCalendar() {
        Calendar result = Calendar.getInstance();
        if (ISO8601_WEEK) {
            result.setMinimalDaysInFirstWeek(4);
            result.setFirstDayOfWeek(Calendar.MONDAY);
        }
        return result;
    }

    public static Object create(java.util.Date date, TimeZone initial, Calendar target, Class<?> type) {
        if (type.equals(DataTypeManager.DefaultDataClasses.TIME)) {
            return type.cast(createTime(date, initial, target));
        }
        if (type.equals(DataTypeManager.DefaultDataClasses.DATE)) {
            return type.cast(createDate(date, initial, target));
        }
        return type.cast(createTimestamp(date, initial, target));
    }

    public static Timestamp createTimestamp(java.util.Date date, TimeZone initial, Calendar target) {
        if (target == null) {
            target = getCalendar();
        }

        long time = target.getTimeInMillis();

        adjustCalendar(date, initial, target);

        target.set(Calendar.MILLISECOND, 0);

        Timestamp tsInTz = new Timestamp(target.getTimeInMillis());

        if(date instanceof Timestamp) {
            tsInTz.setNanos(((Timestamp)date).getNanos());
        }

        target.setTimeInMillis(time);
        return tsInTz;
    }

    public static Time createTime(java.util.Date date, TimeZone initial, Calendar target) {
        if (target == null) {
            target = getCalendar();
        }

        long time = target.getTimeInMillis();

        adjustCalendar(date, initial, target);

        Time result = normalizeTime(date, target);

        target.setTimeInMillis(time);
        return result;
    }

    public static Date createDate(java.util.Date date, TimeZone initial, Calendar target) {
        if (target == null) {
            target = getCalendar();
        }

        long time = target.getTimeInMillis();

        adjustCalendar(date, initial, target);

        Date result = normalizeDate(date, target);

        target.setTimeInMillis(time);
        return result;
    }

    /**
     * Creates normalized SQL Time Object
     *
     * @return Time
     * @since 4.3
     */
    public static Time createTime(java.util.Date date) {
        if (date instanceof Time) {
            return (Time)date;
        }
        Calendar cal = getCalendar();
        cal.setTime(date);
        return normalizeTime(date, cal);
    }

    /**
     * Creates normalized SQL Date Object
     *
     * @return Date
     * @since 4.3
     */
    public static Date createDate(java.util.Date date) {
        if (date instanceof Date) {
            return (Date)date;
        }
        Calendar cal = getCalendar();
        cal.setTime(date);
        return normalizeDate(date, cal);
    }

    public static Timestamp createTimestamp(java.util.Date date) {
        if (date instanceof Timestamp) {
            return (Timestamp)date;
        }
        return new Timestamp(date.getTime());
    }

    private static Date normalizeDate(java.util.Date date, Calendar target) {
        if (!(date instanceof Date)) {
            target.set(Calendar.HOUR_OF_DAY, 0);
            target.set(Calendar.MINUTE, 0);
            target.set(Calendar.SECOND, 0);
            target.set(Calendar.MILLISECOND, 0);
        }
        Date result = new Date(target.getTimeInMillis());
        return result;
    }

    private static Time normalizeTime(java.util.Date date, Calendar target) {
        if (!(date instanceof Time)) {
            target.set(Calendar.YEAR, 1970);
            target.set(Calendar.MONTH, Calendar.JANUARY);
            target.set(Calendar.DAY_OF_MONTH, 1);
            target.set(Calendar.MILLISECOND, 0);
        }
        Time result = new Time(target.getTimeInMillis());
        return result;
    }

    private static void adjustCalendar(java.util.Date date,
                                       TimeZone initial,
                                       Calendar target) {
        assert initial != null;
        if (initial.hasSameRules(target.getTimeZone())) {
            target.setTime(date);
            return;
        }

        //start with base time
        long time = date.getTime();

        Calendar cal = Calendar.getInstance(initial);
        cal.setTimeInMillis(time);

        target.clear();
        for (int i = 0; i <= Calendar.MILLISECOND; i++) {
            target.set(i, cal.get(i));
        }
    }
}