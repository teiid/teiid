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
import java.io.Reader;
import java.io.Serializable;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CodingErrorAction;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.DateFormatSymbols;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.teiid.api.exception.query.ExpressionEvaluationException;
import org.teiid.api.exception.query.FunctionExecutionException;
import org.teiid.client.util.ExceptionUtil;
import org.teiid.common.buffer.BlockedException;
import org.teiid.core.CorePlugin;
import org.teiid.core.TeiidException;
import org.teiid.core.types.BlobImpl;
import org.teiid.core.types.BlobType;
import org.teiid.core.types.ClobImpl;
import org.teiid.core.types.ClobType;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.InputStreamFactory.BlobInputStreamFactory;
import org.teiid.core.types.InputStreamFactory.ClobInputStreamFactory;
import org.teiid.core.types.TransformationException;
import org.teiid.core.util.PropertiesUtils;
import org.teiid.core.util.ReaderInputStream;
import org.teiid.core.util.StringUtil;
import org.teiid.core.util.TimestampWithTimezone;
import org.teiid.language.SQLConstants;
import org.teiid.language.SQLConstants.NonReserved;
import org.teiid.query.QueryPlugin;
import org.teiid.query.function.metadata.FunctionCategoryConstants;
import org.teiid.query.metadata.MaterializationMetadataRepository;
import org.teiid.query.util.CommandContext;

/**
 * Static method hooks for most of the function library.
 */
public final class FunctionMethods {
	
	private static final boolean CALENDAR_TIMESTAMPDIFF = PropertiesUtils.getBooleanProperty(System.getProperties(), "org.teiid.calendarTimestampDiff", true); //$NON-NLS-1$

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
	
	private static final BigDecimal ZERO = new BigDecimal(0);
	
	public static Object divide(BigDecimal x, BigDecimal y) {
		BigDecimal bd = x.divide(y, Math.max(16, x.scale() + y.precision() + 1), RoundingMode.HALF_UP).stripTrailingZeros();
		bd = bd.setScale(Math.max(x.scale(), bd.scale()));
		if (bd.compareTo(ZERO) == 0) {
			return ZERO;
		}
		return bd;
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
    
    public static Object rand(CommandContext context, Integer seed) {        
        if(seed == null) {
            return new Double(context.getNextRand());
        } 
        return new Double(context.getNextRand(seed.longValue()));
    }
        
    public static Object rand(CommandContext context) {
        return new Double(context.getNextRand());
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

	static String[] dayNames;
	static String[] monthNames;
	
	public static Object dayName(Date x) {
		return getDayNames()[getField(x, Calendar.DAY_OF_WEEK) - 1];
	}
	
	private static Locale getSymbolLocale() {
		return PropertiesUtils.getBooleanProperty(System.getProperties(), "org.teiid.enDateNames", false)?Locale.ENGLISH:Locale.getDefault(); //$NON-NLS-1$
	}

	static String[] getMonthNames() {
		if (monthNames == null) {
			DateFormatSymbols dateFormatSymbols = DateFormatSymbols.getInstance(getSymbolLocale());
			String[] months = dateFormatSymbols.getMonths();
			monthNames = Arrays.copyOf(months, 12);
		}
		return monthNames;
	}

	static String[] getDayNames() {
		if (dayNames == null) {
			DateFormatSymbols dateFormatSymbols = DateFormatSymbols.getInstance(getSymbolLocale());
			dayNames = Arrays.copyOfRange(dateFormatSymbols.getWeekdays(), 1, 8);
		}
		return dayNames;
	}

	// ================== Function = dayofmonth =====================

	public static  Object dayOfMonth(Date x) {
		return Integer.valueOf(getField(x, Calendar.DATE));
	}

	// ================== Function = dayofweek =====================

	public static int dayOfWeek(Date x) {
		int result = getField(x, Calendar.DAY_OF_WEEK);
		if (TimestampWithTimezone.ISO8601_WEEK) {
			//technically this is just pg indexing
			//iso would use sunday = 7 rather than 0
			return (result + 6) % 7;
		}
		return result;
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

	public static Object monthName(Date x) {
		return getMonthNames()[getField(x, Calendar.MONTH)];
	}

	// ================== Function = second =====================

	public static Object second(Date x) {
		return Integer.valueOf(getField(x, Calendar.SECOND));
	}

	// ================== Function = week =====================

	public static int week(Date x) {
		return getField(x, Calendar.WEEK_OF_YEAR);
	}

	// ================== Function = year =====================

	public static Object year(Date x) {
		return Integer.valueOf(getField(x, Calendar.YEAR));
	}

	// ================== Function = quarter =====================

	public static Object quarter(Date date) {
		int month = getField(date, Calendar.MONTH);
		
		if (month > 11) {
			 throw new AssertionError("Invalid month for " + date); //$NON-NLS-1$
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

    public static Long timestampDiff(String intervalType, Timestamp ts1Obj, Timestamp ts2Obj) throws FunctionExecutionException  {
    	return timestampDiff(intervalType, ts1Obj, ts2Obj, CALENDAR_TIMESTAMPDIFF);
    }
	
    public static Long timestampDiff(String intervalType, Timestamp ts1Obj, Timestamp ts2Obj, boolean calendarBased) throws FunctionExecutionException  {
        long ts1 = ts1Obj.getTime() / 1000;
        long ts2 = ts2Obj.getTime() / 1000;
        
        long tsDiff = ts2 - ts1;

        long count = 0;
        if(intervalType.equalsIgnoreCase(NonReserved.SQL_TSI_FRAC_SECOND)) {
        	if (Math.abs(tsDiff) > Integer.MAX_VALUE) {
        		throw new FunctionExecutionException(QueryPlugin.Event.TEIID31144, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31144));
        	}
            count = tsDiff * 1000000000 + ts2Obj.getNanos() - ts1Obj.getNanos();
        } else { 
            if(intervalType.equalsIgnoreCase(NonReserved.SQL_TSI_SECOND)) {
                count = tsDiff;
            } else if (calendarBased) {
            	//alternative logic is needed to compute calendar differences 
            	//which looks at elapsed date parts, not total time between
            	if(intervalType.equalsIgnoreCase(NonReserved.SQL_TSI_MINUTE)) {
            		count = ts2 / 60 - ts1 / 60;
            	} else if(intervalType.equalsIgnoreCase(NonReserved.SQL_TSI_HOUR)) {
            		TimeZone tz = TimestampWithTimezone.getCalendar().getTimeZone();
            		if (tz.getDSTSavings() > 0) {
            			ts1 += tz.getOffset(ts1Obj.getTime())/1000;
            			ts2 += tz.getOffset(ts2Obj.getTime())/1000;
            		}
            		count = ts2 / (60*60) - ts1 / (60*60);	
            	} else if(intervalType.equalsIgnoreCase(NonReserved.SQL_TSI_DAY) || intervalType.equalsIgnoreCase(NonReserved.SQL_TSI_WEEK)) {
            		TimeZone tz = TimestampWithTimezone.getCalendar().getTimeZone();
            		if (tz.getDSTSavings() > 0) {
            			ts1 += tz.getOffset(ts1Obj.getTime())/1000;
            			ts2 += tz.getOffset(ts2Obj.getTime())/1000;
            		}
            		//since we are no effectively using GMT we can simply divide since the unix epoch starts at midnight.
            		count = ts2 / (60*60*24) - ts1 / (60*60*24);
                	if (intervalType.equalsIgnoreCase(NonReserved.SQL_TSI_WEEK)) {
                    	//TODO:  this behavior matches SQL Server - but not Derby which expects only whole week
                		
                    	long days = count;
                    	//whole weeks between the two dates
                		count = count/7;
                    	//check for calendar difference assuming sunday as the first week day
                		if (days%7!=0) {
	                    	int day1 = dayOfWeek(ts1Obj);
	                    	int day2 = dayOfWeek(ts2Obj);
	                    	int diff = Integer.signum(day2 - day1);
	                    	if (diff > 0) {
	                    		if (tsDiff < 0) {
	                    			count--;
	                    		}
	                    	} else if (diff < 0) {
	                    		if (tsDiff > 0) {
	                    			count++; 
	                    		}
	                    	}
                		}
                	}
                } else if(intervalType.equalsIgnoreCase(NonReserved.SQL_TSI_MONTH)) {
                	Calendar cal = TimestampWithTimezone.getCalendar();
                	cal.setTimeInMillis(ts1Obj.getTime());
                	int months1 = cal.get(Calendar.YEAR) * 12 + cal.get(Calendar.MONTH);
                	cal.setTimeInMillis(ts2Obj.getTime());
                	int months2 = cal.get(Calendar.YEAR) * 12 + cal.get(Calendar.MONTH);
                    count = months2 - months1;
                } else if(intervalType.equalsIgnoreCase(NonReserved.SQL_TSI_QUARTER)) {
                	Calendar cal = TimestampWithTimezone.getCalendar();
                	cal.setTimeInMillis(ts1Obj.getTime());
                	int quarters1 = cal.get(Calendar.YEAR) * 4 + cal.get(Calendar.MONTH)/3;
                	cal.setTimeInMillis(ts2Obj.getTime());
                	int quarters2 = cal.get(Calendar.YEAR) * 4 + cal.get(Calendar.MONTH)/3;
                    count = quarters2 - quarters1;
                } else if(intervalType.equalsIgnoreCase(NonReserved.SQL_TSI_YEAR)) {
                	Calendar cal = TimestampWithTimezone.getCalendar();
                	cal.setTimeInMillis(ts1Obj.getTime());
                	int years1 = cal.get(Calendar.YEAR);
                	cal.setTimeInMillis(ts2Obj.getTime());
                	int years2 = cal.get(Calendar.YEAR);
                    count = years2 - years1;
                }
            } else if(intervalType.equalsIgnoreCase(NonReserved.SQL_TSI_MINUTE)) {
                count = tsDiff / 60;
            } else if(intervalType.equalsIgnoreCase(NonReserved.SQL_TSI_HOUR)) {
                count = tsDiff / (60*60);	
            } else if(intervalType.equalsIgnoreCase(NonReserved.SQL_TSI_DAY)) {
                count = tsDiff / (60*60*24);
            } else if(intervalType.equalsIgnoreCase(NonReserved.SQL_TSI_WEEK)) {
                count = tsDiff / (60*60*24*7);
            } else if(intervalType.equalsIgnoreCase(NonReserved.SQL_TSI_MONTH)) {
                count = tsDiff / (60*60*24*30);
            } else if(intervalType.equalsIgnoreCase(NonReserved.SQL_TSI_QUARTER)) {
                count = tsDiff / (60*60*24*91);
            } else if(intervalType.equalsIgnoreCase(NonReserved.SQL_TSI_YEAR)) {
                count = tsDiff / (60*60*24*365);
            }    
        }
        //TODO: long results are not consistent with other sources
    	/*if (calendarBased && ((count > 0 && count > Integer.MAX_VALUE) || (count < 0 && count < Integer.MIN_VALUE))) {
    		throw new FunctionExecutionException(QueryPlugin.Event.TEIID31136, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31136));
    	}*/
        return Long.valueOf(count);
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
		return str.length();
	}

	// ================== Function = concat =====================

	public static String concat(String str1, String str2) {
		return str1 + str2;
	}
	
	public static String concat2(String str1, String str2) {
		if (str1 == null) {
			if (str2 == null) {
				return null;
			}
			return str2;
		}
		if (str2 == null) {
			return str1;
		}
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
             throw new FunctionExecutionException(QueryPlugin.Event.TEIID30396, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30396, countValue));
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
             throw new FunctionExecutionException(QueryPlugin.Event.TEIID30396, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30396, countValue));
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
	
	// ================== Function = endsWith =====================

	public static Object endsWith(String sub, String str) {
		return str.endsWith(sub);
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

	public static String trim(String trimSpec, String trimChar, String string) throws FunctionExecutionException {
		if (trimChar.length() != 1) {
			 throw new FunctionExecutionException(QueryPlugin.Event.TEIID30398, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30398, "TRIM CHAR", trimChar));//$NON-NLS-1$
		}
		if (!trimSpec.equalsIgnoreCase(SQLConstants.Reserved.LEADING)) {
			string = rightTrim(string, trimChar.charAt(0));
		}
		if (!trimSpec.equalsIgnoreCase(SQLConstants.Reserved.TRAILING)) {
			string = leftTrim(string, trimChar.charAt(0));
		}
		return string;
	}
	
	// ================== Function = lefttrim =====================

	private static final char SPACE = ' ';

	public static String leftTrim(String string, char trimChar) {
		for(int i=0; i<string.length(); i++) {
			if(string.charAt(i) != trimChar) {
				// end of trim, return what's left
				if (i==0) {
					return string;
				}
				return new String(string.substring(i));
			}
		}

		// All spaces, so trim it all
		return ""; //$NON-NLS-1$
	}
	
	
	public static String leftTrim(String string) {
		return leftTrim(string, SPACE);
	}

	// ================== Function = righttrim =====================

	public static String rightTrim(String string, char trimChar) {
		return rightTrim(string, trimChar, true);
	}
	
	public static String rightTrim(String string, char trimChar, boolean newString) {
		for(int i=string.length()-1; i>=0; i--) {
			if(string.charAt(i) != trimChar) {
				// end of trim, return what's left
				if (i==string.length()-1) {
					return string;
				}
				String result = string.substring(0, i+1);
				if (newString) {
					return new String(result);
				}
				return result;
			}
		}

		// All spaces, so trim it all
		return ""; //$NON-NLS-1$
	}
	
	public static Object rightTrim(String string) {
		return rightTrim(string, SPACE);
	}

	// ================== Function = replace =====================

	public static Object replace(String string, String subString, String replaceString) {
		return StringUtil.replaceAll(string, subString, replaceString);
	}

	// ================== Function = insert =====================

	public static Object insert(String string1, Integer start, Integer length, String str2)
		throws FunctionExecutionException {
		int startValue = start.intValue();
		int len = length.intValue();

		// Check some invalid cases
		if(startValue < 1 || (startValue-1) > string1.length()) {
			 throw new FunctionExecutionException(QueryPlugin.Event.TEIID30399, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30399, start, string1));
		} else if (len < 0) {
			 throw new FunctionExecutionException(QueryPlugin.Event.TEIID30400, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30400, len));
		} else if (string1.length() == 0 && (startValue > 1 || len >0) ) {
			 throw new FunctionExecutionException(QueryPlugin.Event.TEIID30401, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30401));
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
	         throw new FunctionExecutionException(QueryPlugin.Event.TEIID30402, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30402));
	    }
	    if(length < str.length()) {
	        return new String(str.substring(0, length));
	    }
	    if(length > DataTypeManager.MAX_STRING_LENGTH) {
	    	length = DataTypeManager.MAX_STRING_LENGTH;
	    }
	    // Get pad character
	    if(padStr.length() == 0) {
	         throw new FunctionExecutionException(QueryPlugin.Event.TEIID30403, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30403));
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
             throw new FunctionExecutionException(QueryPlugin.Event.TEIID30404, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30404));
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

	public static Object convert(Object src, String type)
		throws FunctionExecutionException {
		try {
			return DataTypeManager.transformValue(src, DataTypeManager.getDataTypeClass(type));
		} catch(TransformationException e) {
			 throw new FunctionExecutionException(QueryPlugin.Event.TEIID30405, e, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30405, new Object[]{src, DataTypeManager.getDataTypeName(src.getClass()), type}));
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

         throw new FunctionExecutionException(QueryPlugin.Event.TEIID30406, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30406));
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
    
         throw new FunctionExecutionException(QueryPlugin.Event.TEIID30407, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30407));
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
    
         throw new FunctionExecutionException(QueryPlugin.Event.TEIID30407, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30407));
    }      
    
    // ================== Function = lookup =====================

    /**
     * This function should never actually be called - it is here solely so the
     * lookup function can be resolved properly.  The actual function is
     * implemented in the ExpresionEvaluator
     * @param codeTable 
     * @param returnElement 
     * @param keyElement 
     * @param keyValue 
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
	public static String format(CommandContext context, Date date, String format)
		throws FunctionExecutionException {
		try {
            SimpleDateFormat sdf = CommandContext.getDateFormat(context, format);
            return sdf.format(date);
		} catch (IllegalArgumentException iae) {
			 throw new FunctionExecutionException(QueryPlugin.Event.TEIID30409, iae, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30409,iae.getMessage()));
		}
	}

	//	================== Parse String TO date/time/timestamp  ==================
	private static Date parseDateHelper(CommandContext context, String date, String format)
			throws FunctionExecutionException {
		DateFormat df = CommandContext.getDateFormat(context, format);
		try {
			return df.parse(date);
		} catch (ParseException e) {
			 throw new FunctionExecutionException(QueryPlugin.Event.TEIID30410, e, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30410, date, format));
		}
	}
	
	public static Timestamp parseTimestamp(CommandContext context, String timestamp, String format)
		throws FunctionExecutionException {
        return new Timestamp(parseDateHelper(context, timestamp, format).getTime());
	}

	//	================== Format number TO String ==================
	public static String format(CommandContext context, Number number, String format)
	throws FunctionExecutionException {
		try {
	        DecimalFormat df = CommandContext.getDecimalFormat(context, format);
	        return df.format(number);
		} catch (IllegalArgumentException iae) {
			 throw new FunctionExecutionException(QueryPlugin.Event.TEIID30411, iae, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30411, iae.getMessage()));
		}
	}

	//	================== Parse String TO numbers ==================
	public static Object parseInteger(CommandContext context, String number, String format)
		throws FunctionExecutionException {
		Number intNum = parseBigDecimal(context, number, format);
		return new Integer(intNum.intValue());
	}

	public static Object parseLong(CommandContext context, String number, String format)
		throws FunctionExecutionException {
		Number longNum = parseBigDecimal(context, number, format);
		return new Long(longNum.longValue());
	}

	public static Object parseDouble(CommandContext context, String number, String format)
		throws FunctionExecutionException {
		Number doubleNum = parseBigDecimal(context, number, format);
		return new Double(doubleNum.doubleValue());
	}

	public static Object parseFloat(CommandContext context, String number, String format)
		throws FunctionExecutionException {
		Number longNum = parseBigDecimal(context, number, format);
		return new Float(longNum.floatValue());
	}

	public static Object parseBigInteger(CommandContext context, String number, String format)
		throws FunctionExecutionException {
		Number bigIntegerNum = parseBigDecimal(context, number, format);
		return new BigInteger(bigIntegerNum.toString());
	}

	public static BigDecimal parseBigDecimal(CommandContext context, String number, String format)
		throws FunctionExecutionException {
		DecimalFormat df= CommandContext.getDecimalFormat(context, format);
		try {
			return (BigDecimal) df.parse(number);
		} catch (ParseException e) {
			 throw new FunctionExecutionException(QueryPlugin.Event.TEIID30412, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30412,number,format));
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
         throw new ExpressionEvaluationException(QueryPlugin.Event.TEIID30413, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30413, "commandPayload", payload.getClass().getName())); //$NON-NLS-1$
    }

    // ================= Function - ENV ========================
    public static Object env(String propertyName) {
        // All context property keys must be lowercase - we lowercase the incoming key here to match regardless of case
        String propertyNameNocase = propertyName.toLowerCase();
        String value = System.getProperty(propertyName);
        if (value == null) {
            value = System.getProperty(propertyNameNocase);
        }
        return value;            
    }
    
    public static Object session_id(CommandContext context) {
        return context.getConnectionId();
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
    
    @TeiidFunction(category=FunctionCategoryConstants.CONVERSION, name="to_chars", nullOnNull=true)
    public static ClobType toChars(BlobType value, String encoding) throws SQLException, IOException {
    	//TODO: defaulting to true as that was the pre 8.4.1 behavior
    	return toChars(value, encoding, true);
    }
    
    @TeiidFunction(category=FunctionCategoryConstants.CONVERSION, name="to_chars")
    public static ClobType toChars(BlobType value, String encoding, boolean wellFormed) throws SQLException, IOException {
    	Charset cs = getCharset(encoding);
		BlobInputStreamFactory bisf = new BlobInputStreamFactory(value.getReference());
    	ClobImpl clob = new ClobImpl(bisf, -1);
    	clob.setCharset(cs);
    	if (!wellFormed && !CharsetUtils.BASE64_NAME.equalsIgnoreCase(encoding) && !CharsetUtils.HEX_NAME.equalsIgnoreCase(encoding)) {
    		//validate that the charcter conversion is possible
    		//TODO: cache the result in a filestore
    		Reader r = clob.getCharacterStream();
    		try {
	    		while (r.read() != -1) {
	    			
	    		}
    		} catch (IOException e) {
    			CharacterCodingException cce = ExceptionUtil.getExceptionOfType(e, CharacterCodingException.class);
    			if (cce != null) {
    				throw new IOException(CorePlugin.Util.gs(CorePlugin.Event.TEIID10082, cs.displayName()), cce);
    			}
    			throw e;
    		} finally {
    			r.close();
    		}
    	}
    	return new ClobType(clob);
    }
    
    @TeiidFunction(category=FunctionCategoryConstants.CONVERSION, name="to_bytes", nullOnNull=true)
    public static BlobType toBytes(ClobType value, String encoding) throws IOException, SQLException {
    	return toBytes(value, encoding, true);
    }
    
    @TeiidFunction(category=FunctionCategoryConstants.CONVERSION, name="to_bytes")
    public static BlobType toBytes(ClobType value, String encoding, boolean wellFormed) throws IOException, SQLException {
    	Charset cs = getCharset(encoding);
    	ClobInputStreamFactory cisf = new ClobInputStreamFactory(value.getReference());
    	cisf.setCharset(cs);
    	if (!wellFormed || CharsetUtils.BASE64_NAME.equalsIgnoreCase(encoding) || CharsetUtils.HEX_NAME.equalsIgnoreCase(encoding)) {
    		//validate that the binary conversion is possible
    		//TODO: cache the result in a filestore
    		InputStream is = new ReaderInputStream(value.getCharacterStream(), 
    				cs.newEncoder().onMalformedInput(CodingErrorAction.REPORT)
    				.onUnmappableCharacter(CodingErrorAction.REPORT));
    		try {
	    		while (is.read() != -1) {
	    			
	    		}
    		} catch (IOException e) {
    			CharacterCodingException cce = ExceptionUtil.getExceptionOfType(e, CharacterCodingException.class);
    			if (cce != null) {
    				throw new IOException(CorePlugin.Util.gs(CorePlugin.Event.TEIID10083, cs.displayName()), cce);
    			}
    			throw e;
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
    	return StringUtil.unescape(string, -1, true, new StringBuilder());
    }

	public static String uuid() {
		return UUID.randomUUID().toString();
	}
	
	public static Object array_get(Object array, int index) throws FunctionExecutionException, SQLException {
		try {
			if (array instanceof java.sql.Array) {
				return Array.get(((java.sql.Array)array).getArray(index, 1), 0);
			}
			if (array.getClass().isArray()) {
				return Array.get(array, index - 1);
			}
		} catch (ArrayIndexOutOfBoundsException e) {
			return null;
		}
		 throw new FunctionExecutionException(QueryPlugin.Event.TEIID30416, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30416, array.getClass()));
	}
	
	public static int array_length(Object array) throws FunctionExecutionException, SQLException {
		if (array instanceof java.sql.Array) {
			return Array.getLength(((java.sql.Array)array).getArray());
		}
		if (array.getClass().isArray()) {
			return Array.getLength(array);
		}
		 throw new FunctionExecutionException(QueryPlugin.Event.TEIID30416, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30416, array.getClass()));
	}
	
	public static int mvstatus(CommandContext context, String schemaName, String viewName, boolean validity, String status, String action) throws SQLException, BlockedException {
		if (!validity || !status.equalsIgnoreCase(MaterializationMetadataRepository.LoadStates.LOADED.name())) {
			if (action.equalsIgnoreCase(MaterializationMetadataRepository.ErrorAction.THROW_EXCEPTION.name())) {
				throw new SQLException(QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31147));
			}
			if (action.equalsIgnoreCase(MaterializationMetadataRepository.ErrorAction.WAIT.name())){
				if (isMatViewLoaded(context, schemaName, viewName) <= 0) {
					context.getWorkItem().scheduleWork(30000);
					throw BlockedException.INSTANCE;
				}
				return 1;
			}
			if (validity && action.equalsIgnoreCase(MaterializationMetadataRepository.ErrorAction.IGNORE.name())) {
				return 1;
			}			
			return 0;
		}
		return 1;
	}	
	
	private static int isMatViewLoaded(CommandContext context, String schemaName, String viewName) throws SQLException {
		try {
			String query = "SELECT X.Valid, X.LoadState FROM (EXECUTE SYSADMIN.matViewStatus('" + schemaName + "', '" + viewName + "')) AS X"; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
			List<? extends List<?>> results = context.getWorkItem().execute(query);

			Boolean newValid = (Boolean)results.get(0).get(0);
			String newStatus = (String)results.get(0).get(1);
			
			if (newStatus.equalsIgnoreCase(MaterializationMetadataRepository.LoadStates.LOADING.name()) || 
					newStatus.equalsIgnoreCase(MaterializationMetadataRepository.LoadStates.NEEDS_LOADING.name())) {
				return -2;
			}
			if (!newValid || newStatus.equalsIgnoreCase(MaterializationMetadataRepository.LoadStates.FAILED_LOAD.name())) {
				// should this result in exception?
				return -1;
			}
			if (newValid && newStatus.equalsIgnoreCase(MaterializationMetadataRepository.LoadStates.LOADED.name())) {
				return 1;
			}
			return 0;
		} catch (TeiidException e) {
			throw new SQLException(e);
		} catch (TimeoutException e) {
			throw new SQLException(e);
		} catch (ExecutionException e) {
			throw new SQLException(e);
		} catch (InterruptedException e) {
			throw new SQLException(e);
		}
	}
}
