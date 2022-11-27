/*-------------------------------------------------------------------------
*
* Copyright (c) 2003-2011, PostgreSQL Global Development Group
*
*
*-------------------------------------------------------------------------
*/
package org.teiid.transport.pg;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.TimeZone;

/**
 * Slimmed down from org.postgresql.jdbc2.TimestampUtils
 */
public class TimestampUtils {

    private static final int ONEDAY = 24 * 3600 * 1000;
    public static final long DATE_POSITIVE_INFINITY = 9223372036825200000L;
    public static final long DATE_NEGATIVE_INFINITY = -9223372036832400000L;
    public static final long DATE_POSITIVE_SMALLER_INFINITY = 185543533774800000L;
    public static final long DATE_NEGATIVE_SMALLER_INFINITY = -185543533774800000L;

    /**
     * Returns the SQL Date object given the timezone and number of days
     *
     * @param tz The timezone used.
     * @return The parsed date object.
     */
    public static Date toDate(TimeZone tz, int days) {
        long secs = toJavaSecs(days * 86400L);
        long millis = secs * 1000L;
        int offset = tz.getOffset(millis);
        if (millis <= DATE_NEGATIVE_SMALLER_INFINITY) {
            millis = DATE_NEGATIVE_INFINITY;
            offset = 0;
        } else if (millis >= DATE_POSITIVE_SMALLER_INFINITY) {
            millis = DATE_POSITIVE_INFINITY;
            offset = 0;
        }
        return new Date(millis - offset);
    }

    /**
     * Converts the given postgresql seconds to java seconds.
     * Reverse engineered by inserting varying dates to postgresql
     * and tuning the formula until the java dates matched.
     * See {@link #toPgSecs} for the reverse operation.
     *
     * @param secs Postgresql seconds.
     * @return Java seconds.
     */
    private static long toJavaSecs(long secs) {
        // postgres epoc to java epoc
        secs += 946684800L;

        // Julian/Gregorian calendar cutoff point
        if (secs < -12219292800L) { // October 4, 1582 -> October 15, 1582
            secs += 86400 * 10;
            if (secs < -14825808000L) { // 1500-02-28 -> 1500-03-01
                int extraLeaps = (int) ((secs + 14825808000L) / 3155760000L);
                extraLeaps--;
                extraLeaps -= extraLeaps / 4;
                secs += extraLeaps * 86400L;
            }
        }
        return secs;
    }

    /**
     * Converts the given java seconds to postgresql seconds.
     * See {@link #toJavaSecs} for the reverse operation.
     * The conversion is valid for any year 100 BC onwards.
     *
     * @param secs Postgresql seconds.
     * @return Java seconds.
     */
     public static long toPgSecs(long secs) {
        // java epoc to postgres epoc
        secs -= 946684800L;

        // Julian/Greagorian calendar cutoff point
        if (secs < -13165977600L) { // October 15, 1582 -> October 4, 1582
            secs -= 86400 * 10;
            if (secs < -15773356800L) { // 1500-03-01 -> 1500-02-28
                int years = (int) ((secs + 15773356800L) / -3155823050L);
                years++;
                years -= years/4;
                secs += years * 86400;
            }
        }

        return secs;
    }

    /**
     * @param time in microseconds
     * @param tz
     * @return
     */
    public static Timestamp toTimestamp(long time, TimeZone tz) {
        if (time == Long.MAX_VALUE) {
            return new Timestamp(DATE_POSITIVE_INFINITY);
        } else if (time == Long.MIN_VALUE) {
            return new Timestamp(DATE_NEGATIVE_INFINITY);
        }
        long secs = time / 1000000;
        int nanos = (int) (time - secs * 1000000);
        if (nanos < 0) {
            secs--;
            nanos += 1000000;
        }
        nanos *= 1000;

        secs = toJavaSecs(secs);
        long millis = secs * 1000L;
        millis -= tz.getRawOffset();
        Timestamp result = new Timestamp(millis);
        result.setNanos(nanos);
        return result;
    }

    /**
     * Extracts the time part from a timestamp. This method ensures the date part of output timestamp
     * looks like 1970-01-01 in given timezone.
     *
     * @param millis The timestamp from which to extract the time.
     * @param tz timezone to use.
     * @return The extracted time.
     */
    public static Time convertToTime(long millis, TimeZone tz) {
        // Leave just time part of the day.
        // Suppose the input date is 2015 7 Jan 15:40 GMT+02:00 (that is 13:40 UTC)
        // We want it to become 1970 1 Jan 15:40 GMT+02:00
        // 1) Make sure millis becomes 15:40 in UTC, so add offset
        int offset = tz.getRawOffset();
        millis += offset;
        // 2) Truncate year, month, day. Day is always 86400 seconds, no matter what leap seconds are
        millis = millis % ONEDAY;
        // 2) Now millis is 1970 1 Jan 15:40 UTC, however we need that in GMT+02:00, so subtract some
        // offset
        millis -= offset;
        // Now we have brand-new 1970 1 Jan 15:40 GMT+02:00
        return new Time(millis);
    }

}
