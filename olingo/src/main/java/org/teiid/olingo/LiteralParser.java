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
package org.teiid.olingo;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.codec.DecoderException;
import org.apache.olingo.commons.api.edm.EdmParameter;
import org.apache.olingo.commons.api.edm.EdmPrimitiveType;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeException;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.EdmProperty;
import org.apache.olingo.commons.api.edm.EdmType;
import org.apache.olingo.commons.core.edm.primitivetype.EdmPrimitiveTypeFactory;
import org.teiid.core.TeiidException;

@SuppressWarnings("nls")
public class LiteralParser {

    private static final Pattern INT_PATTERN = Pattern.compile("[\\+|-]?\\d+");
    private static final Pattern DOUBLE_PATTERN = Pattern.compile("[\\+|-]?\\d+\\.\\d+((e|E) [\\+|-]? \\d+)?");
    private static final Pattern BOOLEAN_PATTERN = Pattern.compile("true|false|TRUE|FALSE");
    private static final Pattern BINARY_PATTERN = Pattern.compile("^binary\\s'([a-fA-f0-9]*)'$");
    private static final Pattern DATE_PATTERN = Pattern.compile("^(\\d{4})-(\\d{2})-(\\d{2})$");
    private static final Pattern TIME_PATTERN = Pattern.compile("^(\\d{2}):(\\d{2}):(\\d{2})$");

    // dateTimeOffsetValue = year "-" month "-" day "T" hour ":" minute [ ":"
    // second [ "." fractionalSeconds ] ] ( "Z" / sign hour ":" minute )
    private static final Pattern DATETIME_PATTERN = Pattern.compile("" + "^"
            + "(\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2})" + // group 1 (datetime)
            "(:\\d{2})?" + // group 2 (seconds)
            "(\\.\\d{1,7})?" + // group 3 (nanoseconds)
            "((Z)|([\\+|-]?\\d{2}:\\d{2}))?" + // group 4 (tz, ignored - handles
                                               // bad services)
            "$");

    private static final Pattern DATETIMEOFFSET_PATTERN = Pattern.compile(""
            + "^" + "(\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2})" + // group 1
                                                                    // (datetime)
            "(\\.\\d{1,7})?" + // group 2 (nanoSeconds)
            "(((\\+|-)\\d{2}:\\d{2})|(Z))" + // group 3 (offset) / group 6 (utc)
            "$");

    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");
    private static final SimpleDateFormat DATETIME_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm");
    private static final SimpleDateFormat DATETIME_WITH_SECONDS_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
    private static final SimpleDateFormat DATETIME_WITH_MILLIS_FORMAT = new SimpleDateFormat(
            "yyyy-MM-dd'T'HH:mm:ss.SSS");
    private static final SimpleDateFormat DATETIMEOFFSET_XML = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZZ");
    private static final SimpleDateFormat DATETIMEOFFSET_WITH_MILLIS_FORMAT = new SimpleDateFormat(
            "yyyy-MM-dd'T'HH:mm:ss.SSSZZ");
    private static final SimpleDateFormat TIME_FORMAT = new SimpleDateFormat("HH:mm:ss");

    public static Object parseLiteral(String text) {
        if (text.equals("null")) {
            return null;
        }

        text = text.trim();

        if (INT_PATTERN.matcher(text).matches()) {
            try {
                return Integer.parseInt(text);
            } catch (NumberFormatException e) {
                return Long.parseLong(text);
            }
        } else if (DOUBLE_PATTERN.matcher(text).matches()) {
            return Double.parseDouble(text);
        } else if (BOOLEAN_PATTERN.matcher(text).matches()) {
            return Boolean.parseBoolean(text);
        } else if (BINARY_PATTERN.matcher(text).matches()) {
            try {
                return org.apache.commons.codec.binary.Hex.decodeHex(text.toCharArray());
            } catch (DecoderException e) {
                // fall through and returned as text
            }
        } else if (DATE_PATTERN.matcher(text).matches()) {
            try {
                return new Date(DATE_FORMAT.parse(text).getTime());
            } catch (ParseException e) {
                // fall through and returned as text
            }
        } else if (DATETIME_PATTERN.matcher(text).matches()) {
            try {
                return parseDateTime(text);
            } catch (ParseException e) {
            }
        } else if (TIME_PATTERN.matcher(text).matches()) {
            try {
                return parseTime(text);
            } catch (ParseException e) {
            }
        }

        return unquote(text);
    }

    private static String unquote(String str) {
        if (str.startsWith("'") && str.endsWith("'")) {
            return str.substring(1, str.length() - 1).replace("''", "'");
        }
        return str;
    }

    private static Timestamp parseDateTime(String value) throws ParseException {
        Matcher matcher = DATETIME_PATTERN.matcher(value);
        if (!matcher.matches()) {
            return null;
        }
        String dateTime = matcher.group(1);
        String seconds = matcher.group(2);
        String nanoSeconds = matcher.group(3);
        String offset = matcher.group(6);
        String utc = matcher.group(5);

        if (utc != null && utc.equals("Z")) {
            offset = "+00:00";
        }

        if (seconds == null) {
            return new Timestamp(DATETIME_FORMAT.parse(dateTime).getTime());
        }

        if (nanoSeconds == null) {
            return new Timestamp(DATETIME_WITH_SECONDS_FORMAT.parse(
                    dateTime + seconds).getTime());
        }

        if (nanoSeconds.length() <= 4) {
            if (offset == null) {
                return new Timestamp(DATETIME_WITH_MILLIS_FORMAT.parse(dateTime + seconds + nanoSeconds).getTime());
            }
            return new Timestamp(DATETIMEOFFSET_WITH_MILLIS_FORMAT.parse(dateTime + seconds + nanoSeconds + offset)
                    .getTime());
        }
        if (offset == null) {
            return new Timestamp(adjustMillis(DATETIME_WITH_MILLIS_FORMAT
                    .parse(dateTime + seconds + nanoSeconds.substring(0, 4))
                    .getTime(), nanoSeconds));
        }
        return new Timestamp(adjustMillis(
                DATETIME_WITH_MILLIS_FORMAT.parse(dateTime + seconds + nanoSeconds.substring(0, 4)
                                + offset).getTime(), nanoSeconds));
    }

    private static long adjustMillis(long dateTime, final String nanoSeconds) {
        return Math.round(Double.parseDouble("0." + nanoSeconds.substring(4))) == 0 ? dateTime : dateTime + 1;
    }

    private static Time parseTime(String value) throws ParseException {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(TIME_FORMAT.parse(value));
        StringBuilder sb = new StringBuilder();
        sb.append(calendar.get(Calendar.HOUR_OF_DAY)).append(":") //$NON-NLS-1$
                .append(calendar.get(Calendar.MINUTE)).append(":") //$NON-NLS-1$
                .append(calendar.get(Calendar.SECOND));
        return java.sql.Time.valueOf(sb.toString());
    }
    
    
}
