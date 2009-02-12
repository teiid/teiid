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

package com.metamatrix.core.util;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

import com.metamatrix.core.CorePlugin;

public final class DateUtil {

    public static final String COMPLETE_FORMAT  = "yyyy-MM-dd'T'HH:mm:ss.SSS-ZZ:zz"; //$NON-NLS-1$
    public static final String TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS"; //$NON-NLS-1$
    // String indexes (useful for the logic in 'convertStringToDate')
    //      Example  "2001-08-07T20:44:22.911-06:00"
    //               "yyyy-MM-ddTHH:mm:ss.SSS-ZZ:zz"
    //                01234567890123456789012345678
    //                          1         2

	private static final ThreadLocal<DateFormat> DATE_FORMATTER = new ThreadLocal<DateFormat>() {
		@Override
		protected DateFormat initialValue() {
	         DateFormat df = new SimpleDateFormat(TIMESTAMP_FORMAT);
	         df.setLenient(false);
	         return df;
		}
	};

    /**
     * Method to convert a string in the standard MetaMatrix pattern into a Date object.
     * The pattern is <i>yyyy-MM-dd'T'HH:mm:ss.SSS-ZZ:zz<i> where
     * <blockquote>
     * <pre>
     * Symbol   Meaning                 Presentation        Example
     * ------   -------                 ------------        -------
     * y        year                    (Number)            1996
     * M        month in year           (Number)            07
     * d        day in month            (Number)            10
     * h        hour in am/pm (1~12)    (Number)            12
     * H        hour in day (0~23)      (Number)            0
     * m        minute in hour          (Number)            30
     * s        second in minute        (Number)            55
     * S        millisecond             (Number)            978
     * Z        zone offset hours       (Number)            6
     * z        zone offset minutes     (Number)            30
     * '        escape for text         (Delimiter)
     * ''       single quote            (Literal)           '
     * </pre>
     * </blockquote>
     * The milliseconds, zone hour, and zone minutes are not required for parsing.
     * @param dateString the stringified date in the format described above
     * @return the Date instance
     * @throws ParseException if the string is not of the expected format
     */
    public static final Date convertStringToDate(String dateString) throws ParseException {
    	ArgCheck.isNotEmpty(dateString);
        int badIndex=0;
        int millis      = 0;
        try {
            // Find the year ...
            int year        = Integer.parseInt( dateString.substring(0,4) );
            badIndex=5;
            int month       = Integer.parseInt( dateString.substring(5,7) );
            badIndex=8;
            int day         = Integer.parseInt( dateString.substring(8,10) );
            badIndex=11;
            int hours       = Integer.parseInt( dateString.substring(11,13) );
            badIndex=14;
            int minutes     = Integer.parseInt( dateString.substring(14,16) );
            badIndex=17;
            int seconds     = Integer.parseInt( dateString.substring(17,19) );
            int len = dateString.length();
            if ( len > 19 ) {
                badIndex=20;
                millis      = Integer.parseInt( dateString.substring(20,23) );
            }
            if ( len > 23 ) {
                badIndex=24;
                Integer.parseInt( dateString.substring(24,26) );
            }
            if ( len > 26 ) {
                badIndex=27;
                Integer.parseInt( dateString.substring(27,29) );
            }

            // Note the month is zero-based!!!!
            GregorianCalendar calendar = new GregorianCalendar(year,month-1,day,hours,minutes,seconds);
            calendar.add(Calendar.MILLISECOND,millis);
            // TODO: Unable to support different time zones!!!!
            //calendar.add(Calendar.ZONE_OFFSET,millis);    throws IllegalArgument
            return calendar.getTime();

            // Use a java.sql.Timestamp since java.util.Date doesn't have a way to set millis!!!
            //return new Timestamp(year,month,day,hours,minutes,seconds,millis*1000);
        } catch ( IndexOutOfBoundsException ioobe ) {
            final Object[] params = new Object[]{dateString, COMPLETE_FORMAT};
            throw new ParseException(CorePlugin.Util.getString("DateUtil.The_string_is_not_of_the_expected_format_1",params),badIndex); //$NON-NLS-1$
        } catch ( NumberFormatException nfe ) {
            final Object[] params = new Object[]{dateString, COMPLETE_FORMAT};
            throw new ParseException(CorePlugin.Util.getString("DateUtil.The_string_is_not_of_the_expected_format_2",params),badIndex); //$NON-NLS-1$
        }
    }

    public static final String getDateAsString( Date timestamp ) {
        return formatString(timestamp);
    }
    
    public static final String getDateAsString( long timestamp ) {
        return formatString(new Date(timestamp));
    }    

	public static final Date getDate( long timestamp ) {
		return new Date(timestamp);
	}

    public static final String getCurrentDateAsString() {
        return formatString(new Date());
    }

    public static final Date getCurrentDate() {
        return new Date();
    }

    private static final String formatString( Date timestamp ) {
    	StringBuffer sb = new StringBuffer( DATE_FORMATTER.get().format(timestamp) );
        long offsetMillis = Calendar.getInstance().get( Calendar.ZONE_OFFSET );
        if ( offsetMillis == 0 ) {
            sb.append("+00:00"); //$NON-NLS-1$
        } else if ( offsetMillis > 0 ) {
            sb.append('+');
        } else {
            sb.append('-');
        }
        int seconds = (int)(Math.abs(offsetMillis) / 1000);
        int minutes = seconds / 60;
        int hours = minutes / 60;
        minutes = minutes % 60;
        if ( hours < 10 ) {
            sb.append('0');
        }
        sb.append( hours );
        sb.append(':');
        if ( minutes < 10 ) {
            sb.append('0');
        }
        sb.append( minutes );
        return sb.toString();
    }

}
