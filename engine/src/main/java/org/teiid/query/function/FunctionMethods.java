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

package org.teiid.query.function;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.charset.Charset;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Properties;
import java.util.TimeZone;
import java.util.UUID;

import org.teiid.api.exception.query.ExpressionEvaluationException;
import org.teiid.api.exception.query.FunctionExecutionException;
import org.teiid.core.types.BlobImpl;
import org.teiid.core.types.BlobType;
import org.teiid.core.types.ClobImpl;
import org.teiid.core.types.ClobType;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.TransformationException;
import org.teiid.core.types.InputStreamFactory.BlobInputStreamFactory;
import org.teiid.core.types.InputStreamFactory.ClobInputStreamFactory;
import org.teiid.core.util.TimestampWithTimezone;
import org.teiid.language.SQLConstants.NonReserved;
import org.teiid.query.QueryPlugin;
import org.teiid.query.util.CommandContext;

/**
 * Static method hooks for most of the function library.
 */
public final class FunctionMethods {

	// ================== Function = plus =====================

	public static int plus(int x, int y) {
		return x + y;
	}
	
	public static long plus(long x, long y) {
		return x + y;
	}
	
	public static float plus(float x, float y) {
		return x + y;
	}
	
	public static double plus(double x, double y) {
		return x + y;
	}
	
	public static Object plus(BigInteger x, BigInteger y) {
		return x.add(y);
	}
	
	public static Object plus(BigDecimal x, BigDecimal y) {
		return x.add(y);
	}

	// ================== Function = minus =====================

	public static int minus(int x, int y) {
		return x - y;
	}
	
	public static long minus(long x, long y) {
		return x - y;
	}
	
	public static float minus(float x, float y) {
		return x - y;
	}
	
	public static double minus(double x, double y) {
		return x - y;
	}
	
	public static Object minus(BigInteger x, BigInteger y) {
		return x.subtract(y);
	}
	
	public static Object minus(BigDecimal x, BigDecimal y) {
		return x.subtract(y);
	}

	// ================== Function = multiply =====================

	public static int multiply(int x, int y) {
		return x * y;
	}
	
	public static long multiply(long x, long y) {
		return x * y;
	}
	
	public static float multiply(float x, float y) {
		return x * y;
	}
	
	public static double multiply(double x, double y) {
		return x * y;
	}
	
	public static Object multiply(BigInteger x, BigInteger y) {
		return x.multiply(y);
	}
	
	public static Object multiply(BigDecimal x, BigDecimal y) {
		return x.multiply(y);
	}

	// ================== Function = divide =====================

	public static int divide(int x, int y) {
		return x / y;
	}
	
	public static long divide(long x, long y) {
		return x / y;
	}
	
	public static float divide(float x, float y) {
		return x / y;
	}
	
	public static double divide(double x, double y) {
		return x / y;
	}
	
	public static Object divide(BigInteger x, BigInteger y) {
		return x.divide(y);
	}
	
	public static Object divide(BigDecimal x, BigDecimal y) {
		BigDecimal bd = x.divide(y, Math.max(16, x.scale() + y.precision() + 1), RoundingMode.HALF_UP).stripTrailingZeros();
		return bd.setScale(Math.max(x.scale(), bd.scale()));
	}

	// ================== Function = abs =====================

	public static int abs(int x) {
		return Math.abs(x);
	}
	
	public static long abs(long x) {
		return Math.abs(x);
	}
	
	public static float abs(float x) {
		return Math.abs(x);
	}
	
	public static double abs(double x) {
		return Math.abs(x);
	}
	
	public static Object abs(BigInteger x) {
		return x.abs();
	}
	
	public static Object abs(BigDecimal x) {
		return x.abs();
	}

	// ================== Function = ceiling =====================

	public static Object ceiling(Number x) {
		return new Double(Math.ceil(x.doubleValue()));
	}

	// ================== Function = exp =====================

	public static Object exp(Number x) {
		return new Double(Math.exp(x.doubleValue()));
	}

	// ================== Function = floor =====================

	public static  Object floor(Number x) {
		return new Double(Math.floor(x.doubleValue()));
	}

	// ================== Function = log =====================

	public static  Object log(Number x) {
		return new Double(Math.log(x.doubleValue()));
	}

	// ================== Function = log10 =====================

	private static final double log10baseE = Math.log(10);

	public static Object log10(Number x) {
		return new Double( Math.log(x.doubleValue()) / log10baseE);
	}
    
    // ================== Function = rand=====================
    
    public static Object rand(CommandContext context, Object seed) throws FunctionExecutionException {        
        if(context != null) {
            if(seed == null) {
                return new Double(context.getNextRand());
            } else if(seed instanceof Integer) {
                return new Double(context.getNextRand(((Integer)seed).longValue()));
            }
        }
        throw new FunctionExecutionException("ERR.015.001.0069", QueryPlugin.Util.getString("ERR.015.001.0069", "rand", seed)); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$        
    }
        
    public static Object rand(CommandContext context) throws FunctionExecutionException {
        if(context != null) {
            return new Double(context.getNextRand());
        }
        throw new FunctionExecutionException("ERR.015.001.0069", QueryPlugin.Util.getString("ERR.015.001.0069", "rand")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }
    
	// ================== Function = mod =====================

	public static int mod(int x, int y) {
		return x % y;
	}
	
	public static long mod(long x, long y) {
		return x % y;
	}
	
	public static float mod(float x, float y) {
		return x % y;
	}
	
	public static double mod(double x, double y) {
		return x % y;
	}
	
	public static Object mod(BigInteger x, BigInteger y) {
		return x.remainder(y);
	}
	
	public static Object mod(BigDecimal x, BigDecimal y) {
		return x.remainder(y);
	}
    
	// ================== Function = power =====================
	
	public static double power(double x, double y) {
		return Math.pow(x, y);
	}
	
	public static BigInteger power(BigInteger x, int y) {
		return x.pow(y);
	}
	
	public static BigDecimal power(BigDecimal x, int y) {
		return x.pow(y);
	}

    public static int round(int number, int places) {
        if(places < 0){
        	return round(new BigDecimal(number), places).intValue();
        }
        return number;
    }
    
    public static float round(float number, int places) {
    	return round(new BigDecimal(number), places).floatValue();
    }
    
    public static double round(double number, int places) {
    	return round(new BigDecimal(number), places).doubleValue();
    }
    
    public static BigDecimal round(BigDecimal bigDecimalNumber, int places) {
        int scale = bigDecimalNumber.scale();
        if (scale <= places) {
        	return bigDecimalNumber;
        }
        bigDecimalNumber = bigDecimalNumber.setScale(places,BigDecimal.ROUND_HALF_UP);
        return bigDecimalNumber.setScale(scale,BigDecimal.ROUND_HALF_UP);
    }

	// ================== Function = sign =====================

	public static Object sign(int x) {
		return Integer.signum(x);
	}
	
	public static Object sign(long x) {
		return Long.signum(x);
	}

	public static Object sign(float x) {
		return Math.signum(x);
	}
	
	public static Object sign(double x) {
		return Math.signum(x);
	}
	
	public static Object sign(BigInteger x) {
		return new Integer(x.signum());
	}
	
	public static Object sign(BigDecimal x) {
		return new Integer(x.signum());
	}

	// ================== Function = sqrt =====================


	public static  Object sqrt(Number x) {
		return new Double( Math.sqrt(x.doubleValue()));
	}

	// ================== Function = currentDate =====================

	public static  Object currentDate() {
		return TimestampWithTimezone.createDate(new Date());
	}

	// ================== Function = currentTime =====================

	public static  Object currentTime() {
		return TimestampWithTimezone.createTime(new Date());
	}

	// ================== Function = currentTimestamp =====================

	public static  Object currentTimestamp() {
		return new Timestamp(System.currentTimeMillis());
	}

	// ================== Helper for a bunch of date functions =====================

	private static int getField(java.util.Date date, int field) {
		Calendar cal = TimestampWithTimezone.getCalendar();
		cal.setTime(date);

		return cal.get(field);
	}

	// ================== Function = dayname =====================

	static final String[] dayNames = new String[] {
		"Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday" }; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$ //$NON-NLS-7$

	public static Object dayName(Date x) {
		return dayNames[getField(x, Calendar.DAY_OF_WEEK) - 1];
	}

	// ================== Function = dayofmonth =====================

	public static  Object dayOfMonth(Date x) {
		return Integer.valueOf(getField(x, Calendar.DATE));
	}

	// ================== Function = dayofweek =====================

	public static Object dayOfWeek(Date x) {
		return Integer.valueOf(getField(x, Calendar.DAY_OF_WEEK));
	}

	// ================== Function = dayofyear =====================

	public static Object dayOfYear(Date x) {
		return Integer.valueOf(getField(x, Calendar.DAY_OF_YEAR));
	}

	// ================== Function = hour =====================

	public static Object hour(Date x) {
		return Integer.valueOf(getField(x, Calendar.HOUR_OF_DAY));
	}

	// ================== Function = minute =====================

	public static Object minute(Date x) {
		return Integer.valueOf(getField(x, Calendar.MINUTE));
	}

	// ================== Function = month =====================

	public static Object month(Date x) {
		return Integer.valueOf(getField(x, Calendar.MONTH)+1);
	}

	// ================== Function = monthname =====================

	static final String[] monthNames = new String[] {
		"January", "February", "March", "April", "May", "June", //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$
		"July", "August", "September", "October", "November", "December" }; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$

	public static Object monthName(Date x) {
		return monthNames[getField(x, Calendar.MONTH)];
	}

	// ================== Function = second =====================

	public static Object second(Date x) {
		return Integer.valueOf(getField(x, Calendar.SECOND));
	}

	// ================== Function = week =====================

	public static Object week(Date x) {
		return Integer.valueOf(getField(x, Calendar.WEEK_OF_YEAR));
	}

	// ================== Function = year =====================

	public static Object year(Date x) {
		return Integer.valueOf(getField(x, Calendar.YEAR));
	}

	// ================== Function = quarter =====================

	public static Object quarter(Date date)
		throws FunctionExecutionException {
		int month = getField(date, Calendar.MONTH);
		
		if (month > 11) {
			throw new FunctionExecutionException("ERR.015.001.0066", QueryPlugin.Util.getString("ERR.015.001.0066", //$NON-NLS-1$ //$NON-NLS-2$
					new Object[] {"quarter", date.getClass().getName()})); //$NON-NLS-1$
		}
		return Integer.valueOf(month/3 + 1);
	}

	//	================== Function = timestampadd =====================

	public static Object timestampAdd(String intervalType, Integer count, Timestamp timestamp) {
		Calendar cal = TimestampWithTimezone.getCalendar();

		int nanos = timestamp.getNanos();
		cal.setTime(timestamp);

		// case of interval = 1, fractional seconds (nanos), don't go to branches of addField()
		if (intervalType.equalsIgnoreCase(NonReserved.SQL_TSI_FRAC_SECOND)) {
			int countValue = count.intValue();
			nanos += countValue;

			// Handle the case of nanos > 999,999,999 and increase the second.
			// Since the count number is an interger, so the maximum is definite,
			// and nanos/999,999,999 can at most be added to second
			if ( nanos > 999999999) {
				int addSecond = nanos / 999999999;
				int leftNanos = nanos % 999999999;
				cal.add(Calendar.SECOND, addSecond);

				Timestamp ts = new Timestamp(cal.getTime().getTime());
				ts.setNanos(leftNanos);
				return ts;
			} 
            // nanos <= 999,999,999
			Timestamp ts = new Timestamp(cal.getTime().getTime());
			ts.setNanos(nanos);
			return ts;
		}
        // for interval from 2 to 9
		addField(intervalType, count, cal);
		Timestamp ts = new Timestamp(cal.getTime().getTime());

		//rectify returned timestamp with original nanos
		ts.setNanos(nanos);
		return ts;
	}
	
	/** Helper method for timestampAdd method
	 * @param interval Integer
	 * @param count Integer
	 * @param cal Calendar instance
	 */
	private static void addField(String interval, Integer count, Calendar cal) {
		int countValue = count.intValue();

        if(interval.equalsIgnoreCase(NonReserved.SQL_TSI_FRAC_SECOND)) {
            //nano seconds - should never get into this branch
        } else if(interval.equalsIgnoreCase(NonReserved.SQL_TSI_SECOND)) {
            cal.add(Calendar.SECOND, countValue);
        } else if(interval.equalsIgnoreCase(NonReserved.SQL_TSI_MINUTE)) {
            cal.add(Calendar.MINUTE, countValue);
        } else if(interval.equalsIgnoreCase(NonReserved.SQL_TSI_HOUR)) {
            cal.add(Calendar.HOUR_OF_DAY, countValue);
        } else if(interval.equalsIgnoreCase(NonReserved.SQL_TSI_DAY)) {
            cal.add(Calendar.DAY_OF_YEAR, countValue);
        } else if(interval.equalsIgnoreCase(NonReserved.SQL_TSI_WEEK)) {
            cal.add(Calendar.WEEK_OF_YEAR, countValue);
        } else if(interval.equalsIgnoreCase(NonReserved.SQL_TSI_MONTH)) {
            cal.add(Calendar.MONTH, countValue);
        } else if(interval.equalsIgnoreCase(NonReserved.SQL_TSI_QUARTER)) {
            cal.add(Calendar.MONTH, countValue*3);
        } else if(interval.equalsIgnoreCase(NonReserved.SQL_TSI_YEAR)) {
            cal.add(Calendar.YEAR, countValue);
        }
	}

	//	================== Function = timestampdiff =====================

	/**
     * This method truncates (ignores) figures
     * @param interval
     * @param timestamp1
     * @param timestamp2
     * @return
     * @throws FunctionExecutionException
     */
    public static Object timestampDiff(String intervalType, Timestamp ts1Obj, Timestamp ts2Obj)  {
        long ts1 = ts1Obj.getTime() / 1000 * 1000000000 + ts1Obj.getNanos();
        long ts2 = ts2Obj.getTime() / 1000 * 1000000000 + ts2Obj.getNanos();
        
        long tsDiff = ts2 - ts1;

        long count = 0;
        if(intervalType.equalsIgnoreCase(NonReserved.SQL_TSI_FRAC_SECOND)) {
            count = tsDiff;
        } else { 
        	tsDiff = tsDiff / 1000000; //convert to milliseconds
            if(intervalType.equalsIgnoreCase(NonReserved.SQL_TSI_SECOND)) {
                count = tsDiff / 1000;
            } else if(intervalType.equalsIgnoreCase(NonReserved.SQL_TSI_MINUTE)) {
                count = (tsDiff / 1000) / 60;
            } else if(intervalType.equalsIgnoreCase(NonReserved.SQL_TSI_HOUR)) {
                count = (tsDiff / 1000) / (60*60);
            } else if(intervalType.equalsIgnoreCase(NonReserved.SQL_TSI_DAY)) {
                count = (tsDiff / 1000) / (60*60*24);
            } else if(intervalType.equalsIgnoreCase(NonReserved.SQL_TSI_WEEK)) {
                count = (tsDiff / 1000) / (60*60*24*7);
            } else if(intervalType.equalsIgnoreCase(NonReserved.SQL_TSI_MONTH)) {
                count = (tsDiff / 1000) / (60*60*24*30);
            } else if(intervalType.equalsIgnoreCase(NonReserved.SQL_TSI_QUARTER)) {
                count = (tsDiff / 1000) / (60*60*24*91);
            } else if(intervalType.equalsIgnoreCase(NonReserved.SQL_TSI_YEAR)) {
                count = (tsDiff / 1000) / (60*60*24*365);
            }    
        }
        return new Long(count);
	}

    //  ================== Function = timestampcreate =====================

    /**
     * This method truncates (ignores) figures
     * @param interval
     * @param timestamp1
     * @param timestamp2
     * @return
     * @throws FunctionExecutionException
     */
    public static Object timestampCreate(java.sql.Date date, Time time) {
        Calendar tsCal = TimestampWithTimezone.getCalendar();
        tsCal.setTime(time);
        int hour = tsCal.get(Calendar.HOUR_OF_DAY);
        int minute = tsCal.get(Calendar.MINUTE);
        int second = tsCal.get(Calendar.SECOND);
        
        tsCal.setTime(date);
        
        tsCal.set(Calendar.HOUR_OF_DAY, hour);
        tsCal.set(Calendar.MINUTE, minute);
        tsCal.set(Calendar.SECOND, second);

        return new Timestamp(tsCal.getTime().getTime());
    }

	// ================== Function = length =====================

	public static Object length(String str) {
		return new Integer(str.length());
	}

	// ================== Function = concat =====================

	public static Object concat(String str1, String str2) {
		return str1 + str2;
	}

	// ================== Function = substring =====================

	public static Object substring(String string, Integer startVal, Integer lengthVal) {
		if (startVal < 0) {
        	startVal = string.length() + startVal;
        } else if (startVal > 0){
            startVal--;     // Adjust to 1-based
        }
		
		if(startVal < 0 || startVal >= string.length()) {
		    return null;
		}

		if(lengthVal < 0) {
		    return null;
		}

		int endVal = Math.min(startVal+lengthVal, string.length());

		return new String(string.substring(startVal, endVal));
	}

    public static Object substring(String string, Integer start) {
        int startVal = start.intValue();
        return substring(string, startVal, string.length());
    }

	// ================== Function = left =====================

	public static Object left(String string, Integer count)
		throws FunctionExecutionException {
		int countValue = count.intValue();
        if(countValue < 0) {
            throw new FunctionExecutionException("ERR.015.001.0017", QueryPlugin.Util.getString("ERR.015.001.0017", countValue)); //$NON-NLS-1$ //$NON-NLS-2$
        } 
        if(string.length() < countValue) {
            return string;
        }
        return new String(string.substring(0, countValue));
	}

	// ================== Function = right =====================

	public static Object right(String string, Integer count) 
		throws FunctionExecutionException {
		int countValue = count.intValue();
        if(countValue < 0) {
            throw new FunctionExecutionException("ERR.015.001.0017", QueryPlugin.Util.getString("ERR.015.001.0017", countValue)); //$NON-NLS-1$ //$NON-NLS-2$
        } else if(string.length() < countValue) {
            return string;
		} else {
			return new String(string.substring(string.length() - countValue));
        }
	}

	// ================== Function = lowercase =====================

	public static Object lowerCase(String str) {
		return str.toLowerCase();
	}

	// ================== Function = uppercase =====================

	public static Object upperCase(String str) {
		return str.toUpperCase();
	}

	// ================== Function = locate =====================

	public static Object locate(String sub, String str) {
		return locate(sub, str, 1);
	}

	/**
	 * TODO: The treatment of negative start indexes is inconsistent here.
	 * We're treating the null value like Derby, but not throwing an
	 * exception if the value is less than 1 (less than 0 in DB2).
	 */
	public static Object locate(String sub, String str, Integer start) {
		if(str == null || sub == null) {
			return null;
		} 
		if (start == null) {
			start = 1;
		}
		return new Integer(str.indexOf(sub, start.intValue() - 1) + 1);
	}

	// ================== Function = lefttrim =====================

	private static final char SPACE = ' ';

	public static Object leftTrim(String string) {
		for(int i=0; i<string.length(); i++) {
			if(string.charAt(i) != SPACE) {
				// end of trim, return what's left
				return new String(string.substring(i));
			}
		}

		// All spaces, so trim it all
		return ""; //$NON-NLS-1$
	}

	// ================== Function = righttrim =====================

	public static Object rightTrim(String string) {
		for(int i=string.length()-1; i>=0; i--) {
			if(string.charAt(i) != SPACE) {
				// end of trim, return what's left
				return new String(string.substring(0, i+1));
			}
		}

		// All spaces, so trim it all
		return ""; //$NON-NLS-1$
	}

	// ================== Function = replace =====================

	public static Object replace(String string, String subString, String replaceString) {
		// Check some simple cases that require no work
		if(subString.length() > string.length() || string.length() == 0 || subString.length() == 0) {
			return string;
		}

		StringBuffer result = new StringBuffer();
		int index = 0;

		while(true) {
			int newIndex = string.indexOf(subString, index);
			if(newIndex < 0) {
				// No more replacement sections, grab from old index to end of string
				result.append( string.substring(index));

				// Break out of loop
				break;

			}
			// Matched the substring at newIndex

			// First append section from old index to new
			result.append( string.substring( index, newIndex));

			// Then append replacement section for sub
			result.append( replaceString );

			// Then move the index counter forward
			index = newIndex + subString.length();
		}

		return result.toString();
	}

	// ================== Function = insert =====================

	public static Object insert(String string1, Integer start, Integer length, String str2)
		throws FunctionExecutionException {
		int startValue = start.intValue();
		int len = length.intValue();

		// Check some invalid cases
		if(startValue < 1 || (startValue-1) > string1.length()) {
			throw new FunctionExecutionException("ERR.015.001.0061", QueryPlugin.Util.getString("ERR.015.001.0061", start, string1)); //$NON-NLS-1$ //$NON-NLS-2$
		} else if (len < 0) {
			throw new FunctionExecutionException("ERR.015.001.0062", QueryPlugin.Util.getString("ERR.015.001.0062", len)); //$NON-NLS-1$ //$NON-NLS-2$
		} else if (string1.length() == 0 && (startValue > 1 || len >0) ) {
			throw new FunctionExecutionException("ERR.015.001.0063", QueryPlugin.Util.getString("ERR.015.001.0063")); //$NON-NLS-1$ //$NON-NLS-2$
		}

		StringBuffer result = new StringBuffer();
		result.append(string1.substring(0, startValue-1));
		int endValue = startValue + len - 1;

		// str2.length() = 0 is a valid case
		if (endValue > string1.length()) {
			result.append(str2);
		} else {
			result.append(str2);
			result.append(string1.substring( endValue ));
		}

		return result.toString();
	}

	// ================== Function = repeat =====================
	public static Object repeat(String str, Integer count) {
		int repeatCount = count.intValue();
		StringBuffer result = new StringBuffer();

		for (int i = 0; i < repeatCount && result.length() <= DataTypeManager.MAX_STRING_LENGTH; i++) {
			result.append(str);
		}
		return result.toString();
	}

    // ================== Function = ascii =====================

    public static Integer ascii(String ch) {
        if(ch.length() == 0) {
        	return null;
        } 
        return (int)ch.charAt(0);
    }
    
    public static Integer ascii(Character ch) {
        return (int)ch.charValue();
    }

    // ================== Function = chr =====================

    public static Object chr(int intValue) {
        return new Character((char) intValue);
    }

    // ================== Function = initCap =====================

    public static Object initCap(String s) {
        StringBuffer cap = new StringBuffer();

        boolean checkCap = true;
        for(int i=0; i<s.length(); i++) {
            char c = s.charAt(i);

            // Decide whether to upper case
            if(checkCap) {
                cap.append(Character.toUpperCase(c));
            } else {
                cap.append(Character.toLowerCase(c));
            }

            // Reset flag for next character
            checkCap = Character.isWhitespace(c);
        }
        return cap.toString();
    }

    // ================== Function = lpad =====================

    public static Object lpad(String inputString, Integer padLength, String padStr)
        throws FunctionExecutionException {

    	return pad(inputString, padLength, padStr, true);
    }

    public static Object pad(String str, Integer padLength, String padStr, boolean left)
    throws FunctionExecutionException {
	    int length = padLength.intValue();
	    if(length < 1) {
	        throw new FunctionExecutionException("ERR.015.001.0025", QueryPlugin.Util.getString("ERR.015.001.0025")); //$NON-NLS-1$ //$NON-NLS-2$
	    }
	    if(length < str.length()) {
	        return new String(str.substring(0, length));
	    }
	    if(length > DataTypeManager.MAX_STRING_LENGTH) {
	    	length = DataTypeManager.MAX_STRING_LENGTH;
	    }
	    // Get pad character
	    if(padStr.length() == 0) {
	        throw new FunctionExecutionException("ERR.015.001.0027", QueryPlugin.Util.getString("ERR.015.001.0027")); //$NON-NLS-1$ //$NON-NLS-2$
	    }
	    // Pad string
	    StringBuffer outStr = new StringBuffer(str);
	    while(outStr.length() < length) {
	    	if (left) {
	    		outStr.insert(0, padStr);
	    	} else {
	    		outStr.append(padStr);
	    	}
	    }
	    if (left) {
	    	return new String(outStr.substring(outStr.length() - length));
	    }
	    return new String(outStr.substring(0, length));
	}

    
    public static final String SPACE_CHAR = " "; //$NON-NLS-1$

    public static Object lpad(String inputString, Integer padLength)
        throws FunctionExecutionException {

        return lpad(inputString, padLength, SPACE_CHAR);
    }

    // ================== Function = rpad =====================

    public static Object rpad(String inputString, Integer padLength, String padStr)
        throws FunctionExecutionException {

    	return pad(inputString, padLength, padStr, false);
    }

    public static Object rpad(String inputString, Integer padLength)
        throws FunctionExecutionException {

        return rpad(inputString, padLength, SPACE_CHAR);
    }

    // ================== Function = translate =====================

    public static Object translate(String str, String in, String out)
        throws FunctionExecutionException {
        if(in.length() != out.length()) {
            throw new FunctionExecutionException("ERR.015.001.0031", QueryPlugin.Util.getString("ERR.015.001.0031")); //$NON-NLS-1$ //$NON-NLS-2$
        }

        if(in.length() == 0 || str.length() == 0) {
            return str;
        }

        StringBuffer translated = new StringBuffer(str.length());
        for(int i=0; i<str.length(); i++) {
            char c = str.charAt(i);
            int j = in.indexOf(c);
            if (j >= 0) {
                translated.append(out.charAt(j));
            } else {
                translated.append(c);
            }
        }
        return translated.toString();
    }

	// ================== Function = convert =====================

	@SuppressWarnings("unchecked")
	public static Object convert(Object src, String type)
		throws FunctionExecutionException {
		try {
			return DataTypeManager.transformValue(src, DataTypeManager.getDataTypeClass(type));
		} catch(TransformationException e) {
			throw new FunctionExecutionException(e, "ERR.015.001.0033", QueryPlugin.Util.getString("ERR.015.001.0033", new Object[]{src, DataTypeManager.getDataTypeName(src.getClass()), type})); //$NON-NLS-1$ //$NON-NLS-2$
		}
	}

    // ================== Function = context and rowlimit =====================

    /**
     * This function should never actually be called - it is here solely so the
     * xml context function can be resolved properly.  The actual function is
     * implemented in the XML planner.
     * @param context The context to apply the criteria in
     * @param expression The expression on the left side of the criteria
     * @return Same as expression
     */
    public static Object context(Object context, Object expression)
        throws FunctionExecutionException {

        throw new FunctionExecutionException("ERR.015.001.0035", QueryPlugin.Util.getString("ERR.015.001.0035")); //$NON-NLS-1$ //$NON-NLS-2$
    }

    /**
     * This pseudo-function should never actually be called - it is here solely so the
     * xml rowlimit function can be resolved properly.  The actual functionality is
     * implemented in the XML planner/processor.
     * @param expression The expression on the left side of the criteria, an xml node
     * @return doesn't really return anything; this pseudo-function is used to control
     * the number of rows returned from a mapping class.
     */
    public static Object rowlimit(Object expression)
        throws FunctionExecutionException {
    
        throw new FunctionExecutionException("ERR.015.001.0035a", QueryPlugin.Util.getString("ERR.015.001.0035a")); //$NON-NLS-1$ //$NON-NLS-2$
    }    

    /**
     * This pseudo-function should never actually be called - it is here solely so the
     * xml rowlimitexception function can be resolved properly.  The actual functionality is
     * implemented in the XML planner/processor.
     * @param expression The expression on the left side of the criteria, an xml node
     * @return doesn't really return anything; this pseudo-function is used to control
     * the number of rows returned from a mapping class.
     */
    public static Object rowlimitexception(Object expression)
        throws FunctionExecutionException {
    
        throw new FunctionExecutionException("ERR.015.001.0035a", QueryPlugin.Util.getString("ERR.015.001.0035a")); //$NON-NLS-1$ //$NON-NLS-2$
    }      
    
    // ================== Function = lookup =====================

    /**
     * This function should never actually be called - it is here solely so the
     * lookup function can be resolved properly.  The actual function is
     * implemented in the ExpresionEvaluator
     * @param context The context to apply the criteria in
     * @param expression The expression on the left side of the criteria
     * @return Same as expression
     */
    public static Object lookup(Object codeTable, Object returnElement, Object keyElement, Object keyValue) {

        throw new UnsupportedOperationException("This method should never be called."); //$NON-NLS-1$
    }
	
    // ================== Function = nvl =====================
    
    public static Object ifnull(Object value, Object ifNullValue) {
    	return coalesce(value, ifNullValue);
    }
    
    public static Object coalesce(Object value, Object value1, Object... other) {
    	if (value != null) {
    		return value;
    	}
    	if (value1 != null) {
    		return value1;
    	}
        for (Object object : other) {
			if (object != null) {
				return object;
			}
		}
        return null;
    }

	// ================== Format date/time/timestamp TO String ==================
	public static String format(Date date, String format)
		throws FunctionExecutionException {
		try {
            SimpleDateFormat sdf = new SimpleDateFormat(format);
            return sdf.format(date);
		} catch (IllegalArgumentException iae) {
			throw new FunctionExecutionException("ERR.015.001.0042", QueryPlugin.Util.getString("ERR.015.001.0042" , //$NON-NLS-1$ //$NON-NLS-2$
				iae.getMessage()));
		}
	}

	//	================== Parse String TO date/time/timestamp  ==================
	private static Date parseDateHelper(String date, String format)
			throws FunctionExecutionException {
		DateFormat df = new SimpleDateFormat(format);
		try {
			return df.parse(date);
		} catch (ParseException e) {
			throw new FunctionExecutionException("ERR.015.001.0043", QueryPlugin.Util.getString("ERR.015.001.0043" , //$NON-NLS-1$ //$NON-NLS-2$
					date, format));
		}
	}
	
	public static Timestamp parseTimestamp(String timestamp, String format)
		throws FunctionExecutionException {
        return new Timestamp(parseDateHelper(timestamp, format).getTime());
	}

	//	================== Format number TO String ==================
	public static String format(Number number, String format)
	throws FunctionExecutionException {
		try {
	        DecimalFormat df = new DecimalFormat(format);
	        return df.format(number);
		} catch (IllegalArgumentException iae) {
			throw new FunctionExecutionException("ERR.015.001.0042", QueryPlugin.Util.getString("ERR.015.001.0042" , //$NON-NLS-1$ //$NON-NLS-2$
			iae.getMessage()));
		}
	}

	//	================== Parse String TO numbers ==================
	public static Object parseInteger(String number, String format)
		throws FunctionExecutionException {
		Number intNum = parseNumberHelper(number, format);
		return new Integer(intNum.intValue());
	}

	public static Object parseLong(String number, String format)
		throws FunctionExecutionException {
		Number longNum = parseNumberHelper(number, format);
		return new Long(longNum.longValue());
	}

	public static Object parseDouble(String number, String format)
		throws FunctionExecutionException {
		Number doubleNum = parseNumberHelper(number, format);
		return new Double(doubleNum.doubleValue());
	}

	public static Object parseFloat(String number, String format)
		throws FunctionExecutionException {
		Number longNum = parseNumberHelper(number, format);
		return new Float(longNum.floatValue());
	}

	public static Object parseBigInteger(String number, String format)
		throws FunctionExecutionException {
		Number bigIntegerNum = parseNumberHelper(number, format);
		return new BigInteger(bigIntegerNum.toString());
	}

	public static Object parseBigDecimal(String number, String format)
		throws FunctionExecutionException {
		Number bigDecimalNum = parseNumberHelper(number, format);
		return new BigDecimal(bigDecimalNum.toString());
	}

	// ============== Helper Function for format/parse numbers ==================

	private static Number parseNumberHelper(String number, String format)
		throws FunctionExecutionException {
		DecimalFormat df= new DecimalFormat(format);
		try {
			return df.parse(number);
		} catch (ParseException e) {
			throw new FunctionExecutionException("ERR.015.001.0043", QueryPlugin.Util.getString("ERR.015.001.0043" , //$NON-NLS-1$ //$NON-NLS-2$
					number,format));
		}
	}

	// ================== Function - ACOS =====================
	public static Object acos(Number number) {
		return new Double(Math.acos(number.doubleValue()));
	}

	// ================== Function - ASIN =====================
	public static Object asin(Number number) {
		return new Double(Math.asin(number.doubleValue()));
	}

	// ================== Function - ATAN =====================
	public static Object atan(Number number) {
		return new Double(Math.atan(number.doubleValue()));
	}

	// ================== Function - ATAN2 =====================
	public static Object atan2(Number number1, Number number2) {
		return new Double(Math.atan2(number1.doubleValue(), number2.doubleValue()));
	}

	// ================== Function - COS =====================
	public static Object cos(Number number) {
		return new Double(Math.cos(number.doubleValue()));
	}

	// ================== Function - COT =====================
	public static Object cot(Number number) {
		return new Double(1/Math.tan(number.doubleValue()));
	}

	// ================== Function - DEGREES =====================
	public static Object degrees(Number number) {
		return new Double(Math.toDegrees(number.doubleValue()));
	}

	// ================== Function - PI =====================
	public static Object pi() {
		return new Double(Math.PI);
	}

	// ================== Function - RADIANS =====================
	public static Object radians(Number number) {
		return new Double(Math.toRadians(number.doubleValue()));
	}

	// ================== Function - SIN =====================
	public static Object sin(Number number) {
		return new Double(Math.sin(number.doubleValue()));
	}

	// ================== Function - TAN =====================
	public static Object tan(Number number) {
		return new Double(Math.tan(number.doubleValue()));
	}

    // ================== Function - BITAND =====================
	public static Object bitand(int x, int y) {
        return x & y;
	}

    // ================== Function - BITOR =====================
    public static Object bitor(int x, int y) {
        return x | y;
    }

    // ================== Function - BITXOR =====================
    public static Object bitxor(int x, int y) {
        return x ^ y;
    }

    // ================== Function - BITNOT =====================
    public static int bitnot(int x) {
        return x ^ 0xFFFFFFFF;
    }

    // ================= Function - USER ========================
    public static Object user(CommandContext context) {
        return context.getUserName();
    }
    
    public static Object current_database(CommandContext context) {
    	return context.getVdbName();
    }

    // ================= Function - COMMANDPAYLOAD ========================
    public static Object commandPayload(CommandContext context) {
        Serializable payload = context.getCommandPayload();
        if(payload == null) {
            return null;
        }
        // 0-arg form - just return payload as a string if it exists
        return payload.toString();
    }

    public static Object commandPayload(CommandContext context, String param) 
        throws ExpressionEvaluationException, FunctionExecutionException{
        Serializable payload = context.getCommandPayload();
        if(payload == null) {
            return null;
        }
        
        // 1-arg form - assume payload is a Properties object
        if(payload instanceof Properties) {
            return ((Properties)payload).getProperty(param);
        }            
        // Payload was bad
        throw new ExpressionEvaluationException(QueryPlugin.Util.getString("ExpressionEvaluator.Expected_props_for_payload_function", "commandPayload", payload.getClass().getName())); //$NON-NLS-1$ //$NON-NLS-2$
    }

    // ================= Function - ENV ========================
    public static Object env(CommandContext context, String propertyName) {
        // All context property keys must be lowercase - we lowercase the incoming key here to match regardless of case
        String propertyNameNocase = propertyName.toLowerCase();
        Properties envProps = context.getEnvironmentProperties();
        if(envProps != null && envProps.containsKey(propertyNameNocase)) {
            return envProps.getProperty(propertyNameNocase);
        }
        String value = System.getProperty(propertyName);
        if (value == null) {
            value = System.getProperty(propertyNameNocase);
        }
        return value;            
    }
    
    public static Object session_id(CommandContext context) {
        return context.getConnectionID();
    }
    
    // ================= Function - MODIFYTIMEZONE ========================
    
    public static Object modifyTimeZone(Timestamp value, String originalTimezoneString, String targetTimezoneString) {
        TimeZone originalTimeZone = TimeZone.getTimeZone(originalTimezoneString);
        TimeZone dbmsTimeZone = TimeZone.getTimeZone(targetTimezoneString);

        // Check that the dbms time zone is really different than the local time zone
        if (originalTimeZone.equals(dbmsTimeZone)) {
            return value;
        }

        Calendar cal = Calendar.getInstance(dbmsTimeZone);
        
        return TimestampWithTimezone.createTimestamp(value, originalTimeZone, cal);
    }

    public static Object modifyTimeZone(CommandContext context, Timestamp value, String targetTimezoneString) {
        TimeZone dbmsTimeZone = TimeZone.getTimeZone(targetTimezoneString);

        Calendar cal = Calendar.getInstance(dbmsTimeZone);
        
        return TimestampWithTimezone.createTimestamp(value, context.getServerTimeZone(), cal);
    } 
    
    public static Clob toChars(BlobType value, String encoding) {
    	Charset cs = getCharset(encoding);
		BlobInputStreamFactory bisf = new BlobInputStreamFactory(value.getReference());
    	ClobImpl clob = new ClobImpl(bisf, -1);
    	clob.setCharset(cs);
    	return new ClobType(clob);
    }
    
    public static Blob toBytes(ClobType value, String encoding) throws IOException {
    	Charset cs = getCharset(encoding);
    	ClobInputStreamFactory cisf = new ClobInputStreamFactory(value.getReference());
    	cisf.setCharset(cs);
    	if (CharsetUtils.BASE64_NAME.equalsIgnoreCase(encoding) || CharsetUtils.HEX_NAME.equalsIgnoreCase(encoding)) {
    		//validate that the binary conversion is possible
    		//TODO: cache the result in a filestore
    		InputStream is = cisf.getInputStream();
    		try {
	    		while (is.read() != -1) {
	    			
	    		}
    		} finally {
    			is.close();
    		}
    	}
    	return new BlobType(new BlobImpl(cisf));
	}
    
    public static Charset getCharset(String encoding) {
    	if (CharsetUtils.BASE64_NAME.equalsIgnoreCase(encoding)) {
    		return CharsetUtils.BASE64;
    	}
    	if (CharsetUtils.HEX_NAME.equalsIgnoreCase(encoding)) {
    		return CharsetUtils.HEX;
    	}
    	return Charset.forName(encoding);
	}

    public static String unescape(String string) {
    	StringBuilder sb = new StringBuilder();
    	boolean escaped = false;
    	for (int i = 0; i < string.length(); i++) {
    		char c = string.charAt(i);
    		if (escaped) {
	    		switch (c) {
	    		case 'b':
	    			sb.append('\b');
	    			break;
	    		case 't':
	    			sb.append('\t');
	    			break;
	    		case 'n':
	    			sb.append('\n');
	    			break;
	    		case 'f':
	    			sb.append('\f');
	    			break;
	    		case 'r':
	    			sb.append('\r');
	    			break;
	    		case 'u':
					i = parseNumericValue(string, sb, i, 0, 4, 4);
					//TODO: this should probably be strict about needing 4 digits
	    			break;
    			default:
    				int value = Character.digit(c, 8);
					if (value == -1) {
						sb.append(c);
					} else {
						int possibleDigits = value < 3 ? 2:1;
						int radixExp = 3;
    					i = parseNumericValue(string, sb, i, value, possibleDigits, radixExp);
    				}
	    		}
	    		escaped = false;
    		} else {
    			if (c == '\\') {
    				escaped = true;
    			} else {
					sb.append(c);
    			}
    		}
    	}
    	//TODO: should this be strict?
    	//if (escaped) {
    		//throw new FunctionExecutionException();
    	//}
    	return sb.toString();
    }

	private static int parseNumericValue(String string, StringBuilder sb,
			int i, int value, int possibleDigits, int radixExp) {
		for (int j = 0; j < possibleDigits; j++) {
			if (i + 1 == string.length()) {
				break;
			}
			char digit = string.charAt(i + 1);
			int val = Character.digit(digit, 1 << radixExp);
			if (val == -1) {
				break;
			}
			i++;
			value = (value << radixExp) + val;
		}
		sb.append((char)value);
		return i;
	}
    
	public static String uuid() {
		return UUID.randomUUID().toString();
	}
	
	public static Object array_get(Object array, int index) throws FunctionExecutionException, SQLException {
		try {
			if (array.getClass().isArray()) {
				return Array.get(array, index - 1);
			}
			if (array instanceof java.sql.Array) {
				return Array.get(((java.sql.Array)array).getArray(index, 1), 0);
			}
		} catch (ArrayIndexOutOfBoundsException e) {
			throw new FunctionExecutionException(QueryPlugin.Util.getString("FunctionMethods.array_index", index)); //$NON-NLS-1$
		}
		throw new FunctionExecutionException(QueryPlugin.Util.getString("FunctionMethods.not_array_value", array.getClass())); //$NON-NLS-1$
	}
	
	public static int array_length(Object array) throws FunctionExecutionException, SQLException {
		if (array.getClass().isArray()) {
			return Array.getLength(array);
		}
		if (array instanceof java.sql.Array) {
			return Array.getLength(((java.sql.Array)array).getArray());
		}
		throw new FunctionExecutionException(QueryPlugin.Util.getString("FunctionMethods.not_array_value", array.getClass())); //$NON-NLS-1$
	}
	
}
