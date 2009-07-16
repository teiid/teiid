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

package com.metamatrix.query.function;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Properties;
import java.util.TimeZone;

import com.metamatrix.api.exception.query.ExpressionEvaluationException;
import com.metamatrix.api.exception.query.FunctionExecutionException;
import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.common.types.TransformationException;
import com.metamatrix.common.util.TimestampWithTimezone;
import com.metamatrix.query.QueryPlugin;
import com.metamatrix.query.sql.ReservedWords;
import com.metamatrix.query.util.CommandContext;
import com.metamatrix.query.util.ErrorMessageKeys;

public final class FunctionMethods {

	// ================== Function = plus =====================

	public static Object plus(Object x, Object y) throws FunctionExecutionException {
		if(x instanceof Integer) {
			if(y instanceof Integer) {
				return new Integer(((Integer)x).intValue() + ((Integer)y).intValue());
			}
		} else if(x instanceof Long) {
			if(y instanceof Long) {
				return new Long(((Long)x).longValue() + ((Long)y).longValue());
			}
		} else if(x instanceof Float) {
			if(y instanceof Float) {
				return new Float(((Float)x).floatValue() + ((Float)y).floatValue());
			}
		} else if(x instanceof Double) {
			if(y instanceof Double) {
				return new Double(((Double)x).doubleValue() + ((Double)y).doubleValue());
			}
		} else if(x instanceof BigInteger) {
			if(y instanceof BigInteger) {
				return ((BigInteger)x).add((BigInteger) y);
			}
		} else if(x instanceof BigDecimal) {
			if(y instanceof BigDecimal) {
				return ((BigDecimal)x).add((BigDecimal) y);
			}
		}

		throw new FunctionExecutionException(ErrorMessageKeys.FUNCTION_0007, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0007, new Object[]{"plus", x.getClass().getName(), y.getClass().getName()})); //$NON-NLS-1$
	}

	// ================== Function = minus =====================

	public static Object minus(Object x, Object y) throws FunctionExecutionException {
		if(x instanceof Integer) {
			if(y instanceof Integer) {
				return new Integer(((Integer)x).intValue() - ((Integer)y).intValue());
			}
		} else if(x instanceof Long) {
			if(y instanceof Long) {
				return new Long(((Long)x).longValue() - ((Long)y).longValue());
			}
		} else if(x instanceof Float) {
			if(y instanceof Float) {
				return new Float(((Float)x).floatValue() - ((Float)y).floatValue());
			}
		} else if(x instanceof Double) {
			if(y instanceof Double) {
				return new Double(((Double)x).doubleValue() - ((Double)y).doubleValue());
			}
		} else if(x instanceof BigInteger) {
			if(y instanceof BigInteger) {
				return ((BigInteger)x).subtract((BigInteger) y);
			}
		} else if(x instanceof BigDecimal) {
			if(y instanceof BigDecimal) {
				return ((BigDecimal)x).subtract((BigDecimal) y);
			}
		}

		throw new FunctionExecutionException(ErrorMessageKeys.FUNCTION_0007, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0007, new Object[]{"minus", x.getClass().getName(), y.getClass().getName()})); //$NON-NLS-1$
	}

	// ================== Function = multiply =====================

	public static Object multiply(Object x, Object y) throws FunctionExecutionException {
		if(x instanceof Integer) {
			if(y instanceof Integer) {
				return new Integer(((Integer)x).intValue() * ((Integer)y).intValue());
			}
		} else if(x instanceof Long) {
			if(y instanceof Long) {
				return new Long(((Long)x).longValue() * ((Long)y).longValue());
			}
		} else if(x instanceof Float) {
			if(y instanceof Float) {
				return new Float(((Float)x).floatValue() * ((Float)y).floatValue());
			}
		} else if(x instanceof Double) {
			if(y instanceof Double) {
				return new Double(((Double)x).doubleValue() * ((Double)y).doubleValue());
			}
		} else if(x instanceof BigInteger) {
			if(y instanceof BigInteger) {
				return ((BigInteger)x).multiply((BigInteger) y);
			}
		} else if(x instanceof BigDecimal) {
			if(y instanceof BigDecimal) {
				return ((BigDecimal)x).multiply((BigDecimal) y);
			}
		}

		throw new FunctionExecutionException(ErrorMessageKeys.FUNCTION_0007, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0007, new Object[]{"multiply", x.getClass().getName(), y.getClass().getName()})); //$NON-NLS-1$
	}

	// ================== Function = divide =====================

	public static Object divide(Object x, Object y) throws FunctionExecutionException {
		if(x instanceof Integer) {
			if(y instanceof Integer) {
				return new Integer(((Integer)x).intValue() / ((Integer)y).intValue());
			}
		} else if(x instanceof Long) {
			if(y instanceof Long) {
				return new Long(((Long)x).longValue() / ((Long)y).longValue());
			}
		} else if(x instanceof Float) {
			if(y instanceof Float) {
				return new Float(((Float)x).floatValue() / ((Float)y).floatValue());
			}
		} else if(x instanceof Double) {
			if(y instanceof Double) {
				return new Double(((Double)x).doubleValue() / ((Double)y).doubleValue());
			}
		} else if(x instanceof BigInteger) {
			if(y instanceof BigInteger) {
				return ((BigInteger)x).divide((BigInteger) y);
			}
		} else if(x instanceof BigDecimal) {
			if(y instanceof BigDecimal) {
				return ((BigDecimal)x).divide((BigDecimal) y, BigDecimal.ROUND_HALF_UP);
			}
		}

		throw new FunctionExecutionException(ErrorMessageKeys.FUNCTION_0007, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0007, new Object[]{"divide", x.getClass().getName(), y.getClass().getName()})); //$NON-NLS-1$
	}

	// ================== Function = abs =====================

	public static Object abs(Object x) throws FunctionExecutionException {
		if(x instanceof Integer) {
			return new Integer(Math.abs(((Integer)x).intValue()));
		} else if(x instanceof Long) {
			return new Long(Math.abs(((Long)x).longValue()));
		} else if(x instanceof Float) {
			return new Float(Math.abs(((Float)x).floatValue()));
		} else if(x instanceof Double) {
			return new Double(Math.abs(((Double)x).doubleValue()));
		} else if(x instanceof BigInteger) {
			return ((BigInteger)x).abs();
		} else if(x instanceof BigDecimal) {
			return ((BigDecimal)x).abs();
		}

		throw new FunctionExecutionException(ErrorMessageKeys.FUNCTION_0015, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0015, "abs", x.getClass().getName())); //$NON-NLS-1$
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
        throw new FunctionExecutionException(ErrorMessageKeys.FUNCTION_0069, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0069, "rand", seed)); //$NON-NLS-1$        
    }
        
    public static Object rand(CommandContext context) throws FunctionExecutionException {
        if(context != null) {
            return new Double(context.getNextRand());
        }
        throw new FunctionExecutionException(ErrorMessageKeys.FUNCTION_0069, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0069, "rand")); //$NON-NLS-1$
    }
    
	// ================== Function = mod =====================

	public static  Object mod(Object x, Object y) throws FunctionExecutionException {
		if(x == null || y == null) {
			return null;
		} else if(x instanceof Integer) {
			if(y instanceof Integer) {
				return new Integer(((Integer)x).intValue() % ((Integer)y).intValue());
			}
		} else if(x instanceof Long) {
			if(y instanceof Long) {
				return new Long(((Long)x).longValue() % ((Long)y).longValue());
			}
		} else if(x instanceof Float) {
			if(y instanceof Float) {
				return new Float(((Float)x).floatValue() % ((Float)y).floatValue());
			}
		} else if(x instanceof Double) {
			if(y instanceof Double) {
				return new Double(((Double)x).doubleValue() % ((Double)y).doubleValue());
			}
		} else if(x instanceof BigInteger) {
			if(y instanceof BigInteger) {
				return ((BigInteger)x).mod((BigInteger) y);
			}
		} else if(x instanceof BigDecimal) {
			if(y instanceof BigDecimal) {
				return ((BigDecimal)x).remainder((BigDecimal) y);
			}
		}

		throw new FunctionExecutionException(ErrorMessageKeys.FUNCTION_0007, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0007, new Object[]{"mod", x.getClass().getName(), y.getClass().getName()})); //$NON-NLS-1$
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

	public static  Object sign(Object x) throws FunctionExecutionException {
		if(x == null) {
			return null;
		} else if(x instanceof Integer) {
			int xVal = ((Integer)x).intValue();
			if(xVal > 0) {
				return new Integer(1);
			} else if(xVal == 0) {
				return new Integer(0);
			} 
			return new Integer(-1);
		} else if(x instanceof Long) {
			long xVal = ((Long)x).longValue();
			if(xVal > 0) {
				return new Integer(1);
			} else if(xVal == 0) {
				return new Integer(0);
			}
			return new Integer(-1);
		} else if(x instanceof Float) {
			float xVal = ((Float)x).floatValue();
			if(xVal > 0) {
				return new Integer(1);
			} else if(xVal == 0) {
				return new Integer(0);
			} 
			return new Integer(-1);
		} else if(x instanceof Double) {
			double xVal = ((Double)x).doubleValue();
			if(xVal > 0) {
				return new Integer(1);
			} else if(xVal == 0) {
				return new Integer(0);
			}
			return new Integer(-1);
		} else if(x instanceof BigInteger) {
			return new Integer(((BigInteger)x).signum());
		} else if(x instanceof BigDecimal) {
			return new Integer(((BigDecimal)x).signum());
		}

		throw new FunctionExecutionException(ErrorMessageKeys.FUNCTION_0015, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0015, "sign", x.getClass().getName())); //$NON-NLS-1$
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
			throw new FunctionExecutionException(ErrorMessageKeys.FUNCTION_0066, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0066,
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
		if (intervalType.equalsIgnoreCase(ReservedWords.SQL_TSI_FRAC_SECOND)) {
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
	
	public static Object timestampAdd(String intervalType, Integer count, java.sql.Date timestamp) {
		Calendar cal = TimestampWithTimezone.getCalendar();
		// Note: if dates are different, for example, days are different, the times
		// are still different even they may have the same hours, minutes and seconds.
		cal.setTime(timestamp);
		addField(intervalType, count, cal);
		return TimestampWithTimezone.createDate(cal.getTime());
	}
	
	public static Object timestampAdd(String intervalType, Integer count, Time timestamp) {
		Calendar cal = TimestampWithTimezone.getCalendar();
		cal.setTime(timestamp);
		addField(intervalType, count, cal);
	    return TimestampWithTimezone.createTime(cal.getTime());
	}


	/** Helper method for timestampAdd method
	 * @param interval Integer
	 * @param count Integer
	 * @param cal Calendar instance
	 */
	private static void addField(String interval, Integer count, Calendar cal) {
		int countValue = count.intValue();

        if(interval.equalsIgnoreCase(ReservedWords.SQL_TSI_FRAC_SECOND)) {
            //nano seconds - should never get into this branch
        } else if(interval.equalsIgnoreCase(ReservedWords.SQL_TSI_SECOND)) {
            cal.add(Calendar.SECOND, countValue);
        } else if(interval.equalsIgnoreCase(ReservedWords.SQL_TSI_MINUTE)) {
            cal.add(Calendar.MINUTE, countValue);
        } else if(interval.equalsIgnoreCase(ReservedWords.SQL_TSI_HOUR)) {
            cal.add(Calendar.HOUR_OF_DAY, countValue);
        } else if(interval.equalsIgnoreCase(ReservedWords.SQL_TSI_DAY)) {
            cal.add(Calendar.DAY_OF_YEAR, countValue);
        } else if(interval.equalsIgnoreCase(ReservedWords.SQL_TSI_WEEK)) {
            cal.add(Calendar.WEEK_OF_YEAR, countValue);
        } else if(interval.equalsIgnoreCase(ReservedWords.SQL_TSI_MONTH)) {
            cal.add(Calendar.MONTH, countValue);
        } else if(interval.equalsIgnoreCase(ReservedWords.SQL_TSI_QUARTER)) {
            cal.add(Calendar.MONTH, countValue*3);
        } else if(interval.equalsIgnoreCase(ReservedWords.SQL_TSI_YEAR)) {
            cal.add(Calendar.YEAR, countValue);
        }
	}

	//	================== Function = timestampdiff =====================

    public static Object timestampDiff(String intervalType, Time timestamp1, Time timestamp2) {
    	return timestampDiff(intervalType, new Timestamp(timestamp1.getTime()), new Timestamp(timestamp2.getTime()));
    }
	
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
        if(intervalType.equalsIgnoreCase(ReservedWords.SQL_TSI_FRAC_SECOND)) {
            count = tsDiff;
        } else { 
        	tsDiff = tsDiff / 1000000; //convert to milliseconds
            if(intervalType.equalsIgnoreCase(ReservedWords.SQL_TSI_SECOND)) {
                count = tsDiff / 1000;
            } else if(intervalType.equalsIgnoreCase(ReservedWords.SQL_TSI_MINUTE)) {
                count = (tsDiff / 1000) / 60;
            } else if(intervalType.equalsIgnoreCase(ReservedWords.SQL_TSI_HOUR)) {
                count = (tsDiff / 1000) / (60*60);
            } else if(intervalType.equalsIgnoreCase(ReservedWords.SQL_TSI_DAY)) {
                count = (tsDiff / 1000) / (60*60*24);
            } else if(intervalType.equalsIgnoreCase(ReservedWords.SQL_TSI_WEEK)) {
                count = (tsDiff / 1000) / (60*60*24*7);
            } else if(intervalType.equalsIgnoreCase(ReservedWords.SQL_TSI_MONTH)) {
                count = (tsDiff / 1000) / (60*60*24*30);
            } else if(intervalType.equalsIgnoreCase(ReservedWords.SQL_TSI_QUARTER)) {
                count = (tsDiff / 1000) / (60*60*24*91);
            } else if(intervalType.equalsIgnoreCase(ReservedWords.SQL_TSI_YEAR)) {
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

		return string.substring(startVal, endVal);
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
            throw new FunctionExecutionException(ErrorMessageKeys.FUNCTION_0017, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0017, countValue));
        } 
        if(string.length() < countValue) {
            return string;
        }
        return string.substring(0, countValue);
	}

	// ================== Function = right =====================

	public static Object right(String string, Integer count) 
		throws FunctionExecutionException {
		int countValue = count.intValue();
        if(countValue < 0) {
            throw new FunctionExecutionException(ErrorMessageKeys.FUNCTION_0017, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0017, countValue));
        } else if(string.length() < countValue) {
            return string;
		} else {
			return string.substring(string.length() - countValue);
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
				return string.substring(i);
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
				return string.substring(0, i+1);
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
				result.append( string.substring(index) );

				// Break out of loop
				break;

			}
			// Matched the substring at newIndex

			// First append section from old index to new
			result.append( string.substring( index, newIndex) );

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
			throw new FunctionExecutionException(ErrorMessageKeys.FUNCTION_0061, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0061, start, string1));
		} else if (len < 0) {
			throw new FunctionExecutionException(ErrorMessageKeys.FUNCTION_0062, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0062, len));
		} else if (string1.length() == 0 && (startValue > 1 || len >0) ) {
			throw new FunctionExecutionException(ErrorMessageKeys.FUNCTION_0063, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0063));
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

    public static Object ascii(Object ch)
        throws FunctionExecutionException {

        char c = 0;
        if(ch instanceof Character) {
            c = ((Character) ch).charValue();
        } else if(ch instanceof String) {
            String s = (String) ch;
            if(s.length() >= 1) {
                c = s.charAt(0);
            } else if(s.length() == 0) {
                throw new FunctionExecutionException(ErrorMessageKeys.FUNCTION_0021, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0021));
            }
        } else {
            throw new FunctionExecutionException(ErrorMessageKeys.FUNCTION_0015, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0015, "ascii", ch.getClass().getName())); //$NON-NLS-1$
        }

        return new Integer(c);
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
	        throw new FunctionExecutionException(ErrorMessageKeys.FUNCTION_0025, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0025));
	    }
	    if(length < str.length()) {
	        return str.substring(0, length);
	    }
	    if(length > DataTypeManager.MAX_STRING_LENGTH) {
	    	length = DataTypeManager.MAX_STRING_LENGTH;
	    }
	    // Get pad character
	    if(padStr.length() == 0) {
	        throw new FunctionExecutionException(ErrorMessageKeys.FUNCTION_0027, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0027));
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
	    	return outStr.substring(outStr.length() - length);
	    }
	    return outStr.substring(0, length);
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
            throw new FunctionExecutionException(ErrorMessageKeys.FUNCTION_0031, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0031));
        }

        if(in.length() == 0 || str.length() == 0) {
            return str;
        }

        StringBuffer translated = new StringBuffer(str.length());
        for(int i=0; i<str.length(); i++) {
            char c = str.charAt(i);
            boolean matched = false;
            for(int j=0; j<in.length(); j++) {
                char inChar = in.charAt(j);
                if(c == inChar) {
                    translated.append(out.charAt(j));
                    matched = true;
                    break;
                }
            }
            if(! matched) {
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
			throw new FunctionExecutionException(e, ErrorMessageKeys.FUNCTION_0033, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0033, new Object[]{src, DataTypeManager.getDataTypeName(src.getClass()), type}));
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

        throw new FunctionExecutionException(ErrorMessageKeys.FUNCTION_0035, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0035));
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
    
        throw new FunctionExecutionException(ErrorMessageKeys.FUNCTION_0035a, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0035a));
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
    
        throw new FunctionExecutionException(ErrorMessageKeys.FUNCTION_0035a, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0035a));
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
	public static Object formatDate(Object date, Object format)
		throws FunctionExecutionException {
		try {
            SimpleDateFormat sdf = new SimpleDateFormat((String)format);
            return sdf.format((Date)date);
		} catch (IllegalArgumentException iae) {
			throw new FunctionExecutionException(ErrorMessageKeys.FUNCTION_0042, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0042 ,
				iae.getMessage()));
		}
	}

	public static Object formatTime(Object time, Object format)
		throws FunctionExecutionException {
		try {
            SimpleDateFormat sdf = new SimpleDateFormat((String)format);
            return sdf.format((Time)time);
		} catch (IllegalArgumentException iae) {
			throw new FunctionExecutionException(ErrorMessageKeys.FUNCTION_0042, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0042 ,
				iae.getMessage()));
		}
	}

	public static Object formatTimestamp(Object timestamp, Object format)
		throws FunctionExecutionException {
		try {
            SimpleDateFormat sdf = new SimpleDateFormat((String)format);
            return sdf.format((Timestamp) timestamp);
		} catch (IllegalArgumentException iae) {
			throw new FunctionExecutionException(ErrorMessageKeys.FUNCTION_0042, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0042 ,
				iae.getMessage()));
		}
	}

	//	================== Parse String TO date/time/timestamp  ==================
	public static Object parseDate(String date, String format)
		throws FunctionExecutionException {
		try {
			DateFormat df= new SimpleDateFormat(format);
			Date parsedDate = df.parse(date);
            return TimestampWithTimezone.createDate(parsedDate);
		} catch (java.text.ParseException pe) {
			throw new FunctionExecutionException(ErrorMessageKeys.FUNCTION_0043, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0043 ,
				date, format));
		}
	}

	public static Object parseTime(String time, String format)
		throws FunctionExecutionException {

		try {
			DateFormat df= new SimpleDateFormat(format);
			Date date = df.parse(time);
            return TimestampWithTimezone.createTime(date);
		} catch (java.text.ParseException pe) {
			throw new FunctionExecutionException(ErrorMessageKeys.FUNCTION_0043, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0043 ,
				time,format));
		}
	}

	public static Object parseTimestamp(String timestamp, String format)
		throws FunctionExecutionException {

		try {
			DateFormat df= new SimpleDateFormat(format);
			Date date = df.parse(timestamp);
            return new Timestamp(date.getTime());
		} catch (java.text.ParseException pe) {
			throw new FunctionExecutionException(ErrorMessageKeys.FUNCTION_0043, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0043 ,
				timestamp,format));
		}
	}

	//	================== Format number TO String ==================
	public static Object formatInteger(Object integerNum, Object format)
		throws FunctionExecutionException {
		return formatNumberHelper(integerNum, format);
	}

	public static Object formatLong(Object longNum, Object format)
		throws FunctionExecutionException {
		return formatNumberHelper(longNum, format);
	}

	public static Object formatDouble(Object doubleNum, Object format)
		throws FunctionExecutionException {
		return formatNumberHelper(doubleNum, format);
	}

	public static Object formatFloat(Object floatNum, Object format)
		throws FunctionExecutionException {
		return formatNumberHelper(floatNum, format);
	}
	public static Object formatBigInteger(Object bigIntegerNum, Object format)
		throws FunctionExecutionException {
		return formatNumberHelper(bigIntegerNum, format);
	}

	public static Object formatBigDecimal(Object bigDecimalNum, Object format)
		throws FunctionExecutionException {
		return formatNumberHelper(bigDecimalNum, format);
	}

	//	================== Parse String TO numbers ==================
	public static Object parseInteger(Object number, Object format)
		throws FunctionExecutionException {
		Number intNum = parseNumberHelper(number, format);
		return new Integer(intNum.intValue());
	}

	public static Object parseLong(Object number, Object format)
		throws FunctionExecutionException {
		Number longNum = parseNumberHelper(number, format);
		return new Long(longNum.longValue());
	}

	public static Object parseDouble(Object number, Object format)
		throws FunctionExecutionException {
		Number doubleNum = parseNumberHelper(number, format);
		return new Double(doubleNum.doubleValue());
	}

	public static Object parseFloat(Object number, Object format)
		throws FunctionExecutionException {
		Number longNum = parseNumberHelper(number, format);
		return new Float(longNum.floatValue());
	}

	public static Object parseBigInteger(Object number, Object format)
		throws FunctionExecutionException {
		Number bigIntegerNum = parseNumberHelper(number, format);
		return new BigInteger(bigIntegerNum.toString());
	}

	public static Object parseBigDecimal(Object number, Object format)
		throws FunctionExecutionException {
		Number bigDecimalNum = parseNumberHelper(number, format);
		return new BigDecimal(bigDecimalNum.toString());
	}

	// ============== Helper Function for format/parse numbers ==================
	public static String formatNumberHelper(Object number, Object format)
		throws FunctionExecutionException {
		if (number == null || format == null) {
			return null;
		}

		try {
            DecimalFormat df = new DecimalFormat((String)format);
            return df.format(number);
		} catch (IllegalArgumentException iae) {
			throw new FunctionExecutionException(ErrorMessageKeys.FUNCTION_0042, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0042 ,
			iae.getMessage()));
		}
	}

	private static Number parseNumberHelper(Object number, Object format)
		throws FunctionExecutionException {

		Number num = null;
		if (number == null || format == null) {
			return null;
		}

		if (number instanceof String && format instanceof String) {
			try {
				DecimalFormat df= new DecimalFormat((String) format);
				num = df.parse((String) number);
			} catch (java.text.ParseException pe) {
				throw new FunctionExecutionException(ErrorMessageKeys.FUNCTION_0043, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0043 ,
					number,format));
			}
		}

		return num;
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
    
}
