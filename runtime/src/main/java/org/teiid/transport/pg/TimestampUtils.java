/*-------------------------------------------------------------------------
*
* Copyright (c) 2003-2011, PostgreSQL Global Development Group
*
*
*-------------------------------------------------------------------------
*/
package org.teiid.transport.pg;

import java.sql.Date;
import java.util.TimeZone;

/**
 * Slimmed down from org.postgresql.jdbc2.TimestampUtils
 */
public class TimestampUtils {

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

}
