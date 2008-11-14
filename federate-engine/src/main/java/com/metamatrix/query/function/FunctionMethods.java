/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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
import java.util.StringTokenizer;
import java.util.TimeZone;

import com.metamatrix.api.exception.query.ExpressionEvaluationException;
import com.metamatrix.api.exception.query.FunctionExecutionException;
import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.common.types.Transform;
import com.metamatrix.common.types.TransformationException;
import com.metamatrix.common.util.TimestampWithTimezone;
import com.metamatrix.query.QueryPlugin;
import com.metamatrix.query.sql.ReservedWords;
import com.metamatrix.query.util.CommandContext;
import com.metamatrix.query.util.ErrorMessageKeys;

public final class FunctionMethods {

    private static final String DEFAULT_DECODE_STRING_DELIMITER = ","; //$NON-NLS-1$

	// ================== Function = plus =====================

	public static Object plus(Object x, Object y) throws FunctionExecutionException {
		if(x == null || y == null) {
			return null;
		} else if(x instanceof Integer) {
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
		if(x == null || y == null) {
			return null;
		} else if(x instanceof Integer) {
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
		if(x == null || y == null) {
			return null;
		} else if(x instanceof Integer) {
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
		if(x == null || y == null) {
			return null;
		} else if(x instanceof Integer) {
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
		if(x == null) {
			return null;
		} else if(x instanceof Integer) {
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

	public static Object ceiling(Object x) throws FunctionExecutionException {
		if(x == null) {
			return null;
		} else if(x instanceof Double) {
			return new Double(Math.ceil(((Double)x).doubleValue()));
		}

		throw new FunctionExecutionException(ErrorMessageKeys.FUNCTION_0015, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0015, "ceiling", x.getClass().getName())); //$NON-NLS-1$
	}

	// ================== Function = exp =====================

	public static Object exp(Object x) throws FunctionExecutionException {
		if(x == null) {
			return null;
		} else if(x instanceof Double) {
			return new Double(Math.exp(((Double)x).doubleValue()));
		}

		throw new FunctionExecutionException(ErrorMessageKeys.FUNCTION_0015, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0015, "exp", x.getClass().getName())); //$NON-NLS-1$
	}

	// ================== Function = floor =====================

	public static  Object floor(Object x) throws FunctionExecutionException {
		if(x == null) {
			return null;
		} else if(x instanceof Double) {
			return new Double(Math.floor(((Double)x).doubleValue()));
		}

		throw new FunctionExecutionException(ErrorMessageKeys.FUNCTION_0015, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0015, "floor", x.getClass().getName())); //$NON-NLS-1$
	}

	// ================== Function = log =====================

	public static  Object log(Object x) throws FunctionExecutionException {
		if(x == null) {
			return null;
		} else if(x instanceof Double) {
			return new Double(Math.log(((Double)x).doubleValue()));
		}

		throw new FunctionExecutionException(ErrorMessageKeys.FUNCTION_0015, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0015, "log", x.getClass().getName())); //$NON-NLS-1$
	}

	// ================== Function = log10 =====================

	private static final double log10baseE = Math.log(10);

	public static Object log10(Object x) throws FunctionExecutionException {
		if(x == null) {
			return null;
		} else if(x instanceof Double) {
			return new Double( Math.log(((Double)x).doubleValue()) / log10baseE);
		}

		throw new FunctionExecutionException(ErrorMessageKeys.FUNCTION_0015, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0015, "log10", x.getClass().getName())); //$NON-NLS-1$
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
		}

		throw new FunctionExecutionException(ErrorMessageKeys.FUNCTION_0007, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0007, new Object[]{"mod", x.getClass().getName(), y.getClass().getName()})); //$NON-NLS-1$
	}

	// ================== Function = power =====================

	public static  Object power(Object x, Object y) throws FunctionExecutionException {
		if(x == null || y == null) {
			return null;
		} else if(x instanceof Double) {
			if(y instanceof Double) {
				return new Double(Math.pow(((Double)x).doubleValue(), ((Double)y).doubleValue()));
			}
		} else if(x instanceof BigInteger) {
			if(y instanceof Integer) {
				return ((BigInteger)x).pow(((Integer)y).intValue());
			}
		}

		throw new FunctionExecutionException(ErrorMessageKeys.FUNCTION_0007, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0007, new Object[]{"power", x.getClass().getName(), y.getClass().getName()})); //$NON-NLS-1$
	}

    // ================== Function = power =====================

    public static  Object round(Object number, Object places) throws FunctionExecutionException {
        // Null inputs generate null output
        if(number == null || places == null) {
            return null;
        }

        // Places will always be an Integer
        int intValuePlaces = ((Integer)places).intValue();
        double placeMultiplier = Math.pow(10,intValuePlaces);

        if(number instanceof Integer) {
            Integer integerNumber = (Integer)number;
            if(intValuePlaces <= 0){
                return new Integer((int)((Math.round((integerNumber.intValue()*placeMultiplier)))/placeMultiplier));
            }
            return number;
        } else if(number instanceof Float) {
            Float floatNumber = (Float)number;
            return new Float((float)((Math.round((floatNumber.floatValue()*placeMultiplier)))/placeMultiplier));
        } else if(number instanceof Double) {
            Double doubleNumber = (Double)number;
            return new Double((Math.round((doubleNumber.doubleValue()*placeMultiplier)))/placeMultiplier);
        } else if(number instanceof BigDecimal) {
            BigDecimal bigDecimalNumber = (BigDecimal)number;
            int scale = bigDecimalNumber.scale();
            bigDecimalNumber = bigDecimalNumber.multiply(new BigDecimal("" + placeMultiplier)); //$NON-NLS-1$
            bigDecimalNumber = bigDecimalNumber.setScale(0,BigDecimal.ROUND_HALF_UP);

            BigDecimal bigDecimalMultiplier = new BigDecimal("" + placeMultiplier); //$NON-NLS-1$
            if(intValuePlaces > 0){
                bigDecimalNumber = bigDecimalNumber.setScale(scale,BigDecimal.ROUND_HALF_UP);
                bigDecimalNumber = bigDecimalNumber.divide(bigDecimalMultiplier,BigDecimal.ROUND_HALF_UP);
            }else{
                bigDecimalNumber = bigDecimalNumber.divide(bigDecimalMultiplier,BigDecimal.ROUND_HALF_UP);
                bigDecimalNumber = bigDecimalNumber.setScale(scale,BigDecimal.ROUND_HALF_UP);
            }
            return bigDecimalNumber;
        } else {
            Object[] params = new Object[] { "round", number.getClass().getName(), places.getClass().getName() }; //$NON-NLS-1$
            throw new FunctionExecutionException(ErrorMessageKeys.FUNCTION_0065, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0065, params));
        }
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


	public static  Object sqrt(Object x) throws FunctionExecutionException {
		if(x == null) {
			return null;
		} else if(x instanceof Integer) {
			return new Double( Math.sqrt(((Integer)x).intValue()));
		} else if(x instanceof Long) {
			return new Double( Math.sqrt(((Long)x).longValue()));
		} else if(x instanceof Float) {
			return new Double( Math.sqrt(((Float)x).floatValue()));
		} else if(x instanceof Double) {
			return new Double( Math.sqrt(((Double)x).doubleValue()));
		}

		throw new FunctionExecutionException(ErrorMessageKeys.FUNCTION_0015, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0015, "sqrt", x.getClass().getName())); //$NON-NLS-1$
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
		Calendar cal = Calendar.getInstance();
		cal.setTime(date);

		return cal.get(field);
	}

	// ================== Function = dayname =====================

	static final String[] dayNames = new String[] {
		"Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday" }; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$ //$NON-NLS-7$

	public static Object dayName(Object x)
		throws FunctionExecutionException {

		if(x == null) {
			return null;
		} else if(x instanceof Date) {
            // Day of week is 1-based - convert to 0-based for lookup
			return dayNames[getField((Date)x, Calendar.DAY_OF_WEEK) - 1];
		} else if(x instanceof Timestamp) {
            // Day of week is 1-based - convert to 0-based for lookup
			return dayNames[getField((Timestamp)x, Calendar.DAY_OF_WEEK) - 1];
		}

		throw new FunctionExecutionException(ErrorMessageKeys.FUNCTION_0015, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0015, "dayName", x.getClass().getName())); //$NON-NLS-1$
	}

	// ================== Function = dayofmonth =====================

	public static  Object dayOfMonth(Object x)
		throws FunctionExecutionException {

		if(x == null) {
			return null;
		} else if(x instanceof Date) {
			return new Integer(getField((Date)x, Calendar.DATE));
		} else if(x instanceof Timestamp) {
			return new Integer(getField((Timestamp)x, Calendar.DATE));
		}

		throw new FunctionExecutionException(ErrorMessageKeys.FUNCTION_0015, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0015, "dayOfMonth", x.getClass().getName())); //$NON-NLS-1$
	}

	// ================== Function = dayofweek =====================

	public static Object dayOfWeek(Object x)
		throws FunctionExecutionException {

		if(x == null) {
			return null;
		} else if(x instanceof Date) {
			return new Integer(getField((Date)x, Calendar.DAY_OF_WEEK));
		} else if(x instanceof Timestamp) {
			return new Integer(getField((Timestamp)x, Calendar.DAY_OF_WEEK));
		}

		throw new FunctionExecutionException(ErrorMessageKeys.FUNCTION_0015, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0015, "dayOfWeek", x.getClass().getName())); //$NON-NLS-1$
	}

	// ================== Function = dayofyear =====================

	public static Object dayOfYear(Object x)
		throws FunctionExecutionException {

		if(x == null) {
			return null;
		} else if(x instanceof Date) {
			return new Integer(getField((Date)x, Calendar.DAY_OF_YEAR));
		} else if(x instanceof Timestamp) {
			return new Integer(getField((Timestamp)x, Calendar.DAY_OF_YEAR));
		}

		throw new FunctionExecutionException(ErrorMessageKeys.FUNCTION_0015, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0015, "dayOfYear", x.getClass().getName())); //$NON-NLS-1$
	}

	// ================== Function = hour =====================

	public static Object hour(Object x)
		throws FunctionExecutionException {

		if(x == null) {
			return null;
		} else if(x instanceof Time) {
			return new Integer(getField((Time)x, Calendar.HOUR_OF_DAY));
		} else if(x instanceof Timestamp) {
			return new Integer(getField((Timestamp)x, Calendar.HOUR_OF_DAY));
		}

		throw new FunctionExecutionException(ErrorMessageKeys.FUNCTION_0015, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0015, "hour", x.getClass().getName())); //$NON-NLS-1$
	}

	// ================== Function = minute =====================

	public static Object minute(Object x)
		throws FunctionExecutionException {

		if(x == null) {
			return null;
		} else if(x instanceof Time) {
			return new Integer(getField((Time)x, Calendar.MINUTE));
		} else if(x instanceof Timestamp) {
			return new Integer(getField((Timestamp)x, Calendar.MINUTE));
		}

		throw new FunctionExecutionException(ErrorMessageKeys.FUNCTION_0015, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0015, "minute", x.getClass().getName())); //$NON-NLS-1$
	}

	// ================== Function = month =====================

	public static Object month(Object x)
		throws FunctionExecutionException {

		if(x == null) {
			return null;
		} else if(x instanceof Date) {
			return new Integer(getField((Date)x, Calendar.MONTH)+1);
		} else if(x instanceof Timestamp) {
			return new Integer(getField((Timestamp)x, Calendar.MONTH)+1);
		}

		throw new FunctionExecutionException(ErrorMessageKeys.FUNCTION_0015, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0015, "month", x.getClass().getName())); //$NON-NLS-1$
	}

	// ================== Function = monthname =====================

	static final String[] monthNames = new String[] {
		"January", "February", "March", "April", "May", "June", //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$
		"July", "August", "September", "October", "November", "December" }; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$

	public static Object monthName(Object x)
		throws FunctionExecutionException {

		if(x == null) {
			return null;
		} else if(x instanceof Date) {
			return monthNames[getField((Date)x, Calendar.MONTH)];
		} else if(x instanceof Timestamp) {
			return monthNames[getField((Timestamp)x, Calendar.MONTH)];
		}

		throw new FunctionExecutionException(ErrorMessageKeys.FUNCTION_0015, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0015, "monthName", x.getClass().getName())); //$NON-NLS-1$
	}

	// ================== Function = second =====================

	public static Object second(Object x)
		throws FunctionExecutionException {

		if(x == null) {
			return null;
		} else if(x instanceof Time) {
			return new Integer(getField((Time)x, Calendar.SECOND));
		} else if(x instanceof Timestamp) {
			return new Integer(getField((Timestamp)x, Calendar.SECOND));
		}

		throw new FunctionExecutionException(ErrorMessageKeys.FUNCTION_0015, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0015, "second", x.getClass().getName())); //$NON-NLS-1$
	}

	// ================== Function = week =====================

	public static Object week(Object x)
		throws FunctionExecutionException {

		if(x == null) {
			return null;
		} else if(x instanceof Date) {
			return new Integer(getField((Date)x, Calendar.WEEK_OF_YEAR));
		} else if(x instanceof Timestamp) {
			return new Integer(getField((Timestamp)x, Calendar.WEEK_OF_YEAR));
		}

		throw new FunctionExecutionException(ErrorMessageKeys.FUNCTION_0015, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0015, "week", x.getClass().getName())); //$NON-NLS-1$
	}

	// ================== Function = year =====================

	public static Object year(Object x)
		throws FunctionExecutionException {

		if(x == null) {
			return null;
		} else if(x instanceof Date) {
			return new Integer(getField((Date)x, Calendar.YEAR));
		} else if(x instanceof Timestamp) {
			return new Integer(getField((Timestamp)x, Calendar.YEAR));
		}

		throw new FunctionExecutionException(ErrorMessageKeys.FUNCTION_0015, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0015, "year", x.getClass().getName())); //$NON-NLS-1$
	}

	// ================== Function = quarter =====================

	public static Object quarter(Object date)
		throws FunctionExecutionException {
		String month = null;

		if(date == null) {
			return null;
		} else if(date instanceof Date) {
			month = monthNames[getField((Date)date, Calendar.MONTH)];
		} else if(date instanceof Timestamp) {
			month = monthNames[getField((Timestamp)date, Calendar.MONTH)];
		}

		if (month.equals("January") || month.equals("February") || month.equals("March")) { //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
			return new Integer(1);
		} else if (month.equals("April") || month.equals("May") || month.equals("June")) { //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
			return new Integer(2);
		} else if (month.equals("July") || month.equals("August") || month.equals("September")) { //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
			return new Integer(3);
		} else if (month.equals("October") || month.equals("November") || month.equals("December")) { //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
			return new Integer(4);
		}

		throw new FunctionExecutionException(ErrorMessageKeys.FUNCTION_0066, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0066,
			new Object[] {"quarter", date.getClass().getName()})); //$NON-NLS-1$
	}

	//	================== Function = timestampadd =====================

	public static Object timestampAdd(Object interval, Object count, Object timestamp)
		throws FunctionExecutionException {
		Calendar cal = Calendar.getInstance();

		if (interval == null || count == null || timestamp == null) {
			return null;
		} else if (interval instanceof String && count instanceof Integer) {
            String intervalType = (String) interval;
			if (timestamp instanceof Timestamp) {
				int nanos = ((Timestamp) timestamp).getNanos();
				cal.setTime((Timestamp) timestamp);

				// case of interval = 1, fractional seconds (nanos), don't go to branches of addField()
				if (intervalType.equalsIgnoreCase(ReservedWords.SQL_TSI_FRAC_SECOND)) {
					int countValue = ((Integer) count).intValue();
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
				addField(intervalType, (Integer)count, cal);
				Timestamp ts = new Timestamp(cal.getTime().getTime());

				//rectify returned timestamp with original nanos
				ts.setNanos(nanos);
				return ts;
			} else if (timestamp instanceof Time) {
				// Note: if dates are different, for example, days are different, the times
				// are still different even they may have the same hours, minutes and seconds.
				cal.setTime((Time) timestamp);
				addField(intervalType, (Integer)count, cal);
				return TimestampWithTimezone.createTime(cal.getTime());
			} else if (timestamp instanceof Date) {
				cal.setTime((Date) timestamp);
				addField(intervalType, (Integer)count, cal);
                return TimestampWithTimezone.createDate(cal.getTime());
			}
		}

		throw new FunctionExecutionException(ErrorMessageKeys.FUNCTION_0067, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0067,
			new Object[] {"timestampAdd", interval.getClass().getName(), count.getClass().getName(), timestamp.getClass().getName() })); //$NON-NLS-1$
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

	/**
     * This method truncates (ignores) figures
     * @param interval
     * @param timestamp1
     * @param timestamp2
     * @return
     * @throws FunctionExecutionException
     */
    public static Object timestampDiff(Object interval, Object timestamp1, Object timestamp2) 
        throws FunctionExecutionException {

		if (interval == null || timestamp1 == null || timestamp2 == null) {
			return null;
        } else if (interval instanceof String) {
            String intervalType = (String) interval;
            // Incoming can be time or timestamp  - convert to timestamp
            if(timestamp1 instanceof Time) {
            	Time t1 = (Time)timestamp1;
            	timestamp1 = new Timestamp(t1.getTime());
            }
            if(timestamp2 instanceof Time) {
            	Time t2 = (Time)timestamp2;
            	timestamp2 = new Timestamp(t2.getTime());
            }
            // In nanos
            Timestamp ts1Obj = (Timestamp)timestamp1;
            Timestamp ts2Obj = (Timestamp)timestamp2;
            
            long ts1 = ts1Obj.getTime() / 1000 * 1000000000 + ts1Obj.getNanos();
            long ts2 = ts2Obj.getTime() / 1000 * 1000000000 + ts2Obj.getNanos();
            
            long tsDiff = ts2 - ts1;
    
            long count = 0;
            if(interval.equals(ReservedWords.SQL_TSI_FRAC_SECOND)) {
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
        
        throw new FunctionExecutionException(ErrorMessageKeys.FUNCTION_0067, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0067,
                  new Object[] {"timestampDiff", interval.getClass().getName(),timestamp1.getClass().getName(),timestamp2.getClass().getName() })); //$NON-NLS-1$        
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
    public static Object timestampCreate(Object date, Object time) {

        if (date == null || time == null) {
            return null;
        }

        // Get calendar for time
        Calendar timeCal = Calendar.getInstance();
        timeCal.setTime((java.sql.Time)time);

        // Build calendar for output timestamp based on the date
        Calendar tsCal = Calendar.getInstance();
        tsCal.setTime((java.sql.Date)date);
        
        tsCal.set(Calendar.HOUR_OF_DAY, timeCal.get(Calendar.HOUR_OF_DAY));
        tsCal.set(Calendar.MINUTE, timeCal.get(Calendar.MINUTE));
        tsCal.set(Calendar.SECOND, timeCal.get(Calendar.SECOND));

        return new Timestamp(tsCal.getTime().getTime());
    }

	// ================== Function = length =====================

	public static Object length(Object str)
		throws FunctionExecutionException {

		if(str == null) {
			return null;
		} else if(str instanceof String) {
			return new Integer(((String)str).length());
		}

		throw new FunctionExecutionException(ErrorMessageKeys.FUNCTION_0015, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0015, "length", str.getClass().getName())); //$NON-NLS-1$
	}

	// ================== Function = concat =====================

	public static Object concat(Object str1, Object str2)
		throws FunctionExecutionException {

		if(str1 == null || str2 == null) {
			return null;
		} else if(str1 instanceof String && str2 instanceof String) {
			return (String) str1 + (String) str2;
		}

		throw new FunctionExecutionException(ErrorMessageKeys.FUNCTION_0007, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0007, new Object[]{"concat", str1.getClass().getName(), str2.getClass().getName()})); //$NON-NLS-1$
	}

    public static Object concat2(Object str1, Object str2)
        throws FunctionExecutionException {

        if(str1 == null && str2 == null) {
            return null;
        } else if (str1 != null && str2 == null) {
            return str1;
        } else if (str1 == null && str2 != null) {
            return str2;
        }
        else if(str1 instanceof String && str2 instanceof String) {
            return (String) str1 + (String) str2;
        }
    
        throw new FunctionExecutionException(ErrorMessageKeys.FUNCTION_0007, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0007, new Object[]{"concat", str1.getClass().getName(), str2.getClass().getName()})); //$NON-NLS-1$
    }
    
	// ================== Function = substring =====================

	public static Object substring(Object str, Object start, Object length)
		throws FunctionExecutionException {

		if(str == null || start == null || length == null) {
			return null;
		} 
		if(str instanceof String && start instanceof Integer && length instanceof Integer) {
			String string = (String) str;
			int startVal = ((Integer)start).intValue();
			int lengthVal = ((Integer)length).intValue();
            return substring(string, startVal, lengthVal);
		}

		throw new FunctionExecutionException(ErrorMessageKeys.FUNCTION_0013, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0013, new Object[]{"substring", str.getClass().getName(), start.getClass().getName(), length.getClass().getName()} )); //$NON-NLS-1$
	}

	private static Object substring(String string, int startVal, int lengthVal) {
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

    public static Object substring(Object str, Object start)
        throws FunctionExecutionException {

        if(str == null || start == null) {
            return null;
        } else if(str instanceof String && start instanceof Integer) {
            String string = (String) str;
            int startVal = ((Integer)start).intValue();
            return substring(string, startVal, string.length());
        }

        throw new FunctionExecutionException(ErrorMessageKeys.FUNCTION_0007, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0007, new Object[]{"substring",  str.getClass().getName(), start.getClass().getName()})); //$NON-NLS-1$
    }

	// ================== Function = left =====================

	public static Object left(Object str, Object count)
		throws FunctionExecutionException {

		if(str == null || count == null) {
			return null;
		} else if(str instanceof String && count instanceof Integer) {

			String string = (String) str;
			int countValue = ((Integer)count).intValue();
            if(countValue < 0) {
                throw new FunctionExecutionException(ErrorMessageKeys.FUNCTION_0017, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0017, countValue));
            } else if(string.length() < countValue) {
                return string;
			} else {
    			return string.substring(0, countValue);
            }
		}

		throw new FunctionExecutionException(ErrorMessageKeys.FUNCTION_0007, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0007, new Object[]{"left", str.getClass().getName(), count.getClass().getName()})); //$NON-NLS-1$
	}

	// ================== Function = right =====================

	public static Object right(Object str, Object count)
		throws FunctionExecutionException {

		if(str == null || count == null) {
			return null;
		} else if(str instanceof String && count instanceof Integer) {

			String string = (String) str;
			int countValue = ((Integer)count).intValue();
            if(countValue < 0) {
                throw new FunctionExecutionException(ErrorMessageKeys.FUNCTION_0017, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0017, countValue));
            } else if(string.length() < countValue) {
                return string;
			} else {
    			return string.substring(string.length() - countValue);
            }
		}

		throw new FunctionExecutionException(ErrorMessageKeys.FUNCTION_0007, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0007, new Object[]{"right", str.getClass().getName(), count.getClass().getName()})); //$NON-NLS-1$
	}

	// ================== Function = lowercase =====================

	public static Object lowerCase(Object str)
		throws FunctionExecutionException {

		if(str == null) {
			return null;
		} else if(str instanceof String) {
			return ((String)str).toLowerCase();
		}

		throw new FunctionExecutionException(ErrorMessageKeys.FUNCTION_0015, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0015, "lowerCase", str.getClass().getName())); //$NON-NLS-1$
	}

	// ================== Function = uppercase =====================

	public static Object upperCase(Object str)
		throws FunctionExecutionException {

		if(str == null) {
			return null;
		} else if(str instanceof String) {
			return ((String)str).toUpperCase();
		}

		throw new FunctionExecutionException(ErrorMessageKeys.FUNCTION_0015, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0015, "upperCase", str.getClass().getName())); //$NON-NLS-1$
	}

	// ================== Function = locate =====================

	public static Object locate(Object sub, Object str)
		throws FunctionExecutionException {

		if(str == null || sub == null) {
			return null;
		} else if(sub instanceof String && str instanceof String) {
    		return new Integer(((String)str).indexOf((String)sub) + 1);
		}

		throw new FunctionExecutionException(ErrorMessageKeys.FUNCTION_0007, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0007, new Object[]{"locate", sub.getClass().getName(), str.getClass().getName()})); //$NON-NLS-1$
	}

	public static Object locate(Object sub, Object str, Object start)
		throws FunctionExecutionException {

		if(str == null || sub == null) {
			return null;
		} else if(sub instanceof String && str instanceof String) {
			if(start == null) {
				return new Integer(((String)str).indexOf((String)sub) + 1);
			} else if(start instanceof Integer) {
				return new Integer(((String)str).indexOf((String)sub, ((Integer)start).intValue() - 1) + 1);
			}
		}

		throw new FunctionExecutionException(ErrorMessageKeys.FUNCTION_0013, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0013, new Object[]{"locate",  sub.getClass().getName() ,str.getClass().getName(), start.getClass().getName()})); //$NON-NLS-1$
	}

	// ================== Function = lefttrim =====================

	private static final char SPACE = ' ';

	public static Object leftTrim(Object str)
		throws FunctionExecutionException {

		if(str == null) {
			return null;
		} else if(str instanceof String) {
			String string = (String) str;
			for(int i=0; i<string.length(); i++) {
				if(string.charAt(i) != SPACE) {
					// end of trim, return what's left
					return string.substring(i);
				}
			}

			// All spaces, so trim it all
			return ""; //$NON-NLS-1$
		}

		throw new FunctionExecutionException(ErrorMessageKeys.FUNCTION_0015, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0015, "leftTrim", str.getClass().getName())); //$NON-NLS-1$
	}

	// ================== Function = righttrim =====================

	public static Object rightTrim(Object str)
		throws FunctionExecutionException {

		if(str == null) {
			return null;
		} else if(str instanceof String) {
			String string = (String) str;

			for(int i=string.length()-1; i>=0; i--) {
				if(string.charAt(i) != SPACE) {
					// end of trim, return what's left
					return string.substring(0, i+1);
				}
			}

			// All spaces, so trim it all
			return ""; //$NON-NLS-1$
		}

		throw new FunctionExecutionException(ErrorMessageKeys.FUNCTION_0015, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0015, "rightTrim", str.getClass().getName())); //$NON-NLS-1$
	}

	// ================== Function = replace =====================

	public static Object replace(Object str, Object sub, Object replace)
		throws FunctionExecutionException {

		if(str == null || sub == null || replace == null) {
			return null;
		} else if(str instanceof String && sub instanceof String && replace instanceof String) {
			String string = (String) str;
			String subString = (String) sub;
			String replaceString = (String) replace;

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

		throw new FunctionExecutionException(ErrorMessageKeys.FUNCTION_0013, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0013, new Object[]{"replace",  str.getClass().getName(), sub.getClass().getName(), replace.getClass().getName()})); //$NON-NLS-1$
	}

	// ================== Function = insert =====================

	public static Object insert(Object str1, Object start, Object length, Object str2)
		throws FunctionExecutionException {

		if(str1 == null || start == null || length == null || str2 == null) {
			return null;
		} else if(str1 instanceof String && str2 instanceof String
			&& start instanceof Integer && length instanceof Integer) {

			String string1 = (String) str1;
			int startValue = ((Integer) start).intValue();
			int len = ((Integer) length).intValue();

			// Check some invalid cases
			if(startValue < 1 || (startValue-1) > ((String)str1).length()) {
				throw new FunctionExecutionException(ErrorMessageKeys.FUNCTION_0061, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0061, start, str1));
			} else if (len < 0) {
				throw new FunctionExecutionException(ErrorMessageKeys.FUNCTION_0062, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0062, len));
			} else if (((String) str1).length() == 0 && (startValue > 1 || len >0) ) {
				throw new FunctionExecutionException(ErrorMessageKeys.FUNCTION_0063, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0063));
			}

			StringBuffer result = new StringBuffer();
			result.append(string1.substring(0, startValue-1));
			int endValue = startValue + len - 1;

			// str2.length() = 0 is a valid case
			if (endValue > ((String)str1).length()) {
				result.append((String) str2);
			} else {
				result.append((String) str2);
				result.append(string1.substring( endValue ));
			}

			return result.toString();
		}

		throw new FunctionExecutionException(ErrorMessageKeys.FUNCTION_0064, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0064,
			new Object[] {"insert", str1.getClass().getName(), start.getClass().getName(), length.getClass().getName() , str2.getClass().getName()})); //$NON-NLS-1$
	}

	// ================== Function = repeat =====================
	public static Object repeat(Object str, Object count)
		throws FunctionExecutionException {
		if (str == null || count == null) {
			return null;
		} else if (str instanceof String && count instanceof Integer) {
			int repeatCount = ((Integer) count).intValue();
			StringBuffer result = new StringBuffer();

			for (int i = 0; i < repeatCount; i++) {
				result.append((String)str);
			}

			return result.toString();
		}

		throw new FunctionExecutionException(ErrorMessageKeys.FUNCTION_0065, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0065,
			new Object[] {"repeat", str.getClass().getName(), count.getClass().getName()})); //$NON-NLS-1$
	}

	// ================== Function = space =====================
	public static Object space(Object count)
		throws FunctionExecutionException {
		if (count == null) {
			return null;
		} else if (count instanceof Integer) {
			int repeatCount = ((Integer) count).intValue();
			StringBuffer result = new StringBuffer();

			for (int i = 0; i < repeatCount; i++) {
				result.append(" "); //$NON-NLS-1$
			}

			return result.toString();
		}

		throw new FunctionExecutionException(ErrorMessageKeys.FUNCTION_0066, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0066,
			new Object[] {"space", count.getClass().getName()})); //$NON-NLS-1$
	}

    // ================== Function = ascii =====================

    public static Object ascii(Object ch)
        throws FunctionExecutionException {

        if(ch == null) {
            return null;
        }

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

    public static Object chr(Object intValue) {

        if(intValue == null) {
            return null;
        }

        Integer theInt = (Integer) intValue;

        return new Character((char) theInt.intValue());
    }

    // ================== Function = initCap =====================

    public static Object initCap(Object str)
        throws FunctionExecutionException {

        if(str == null) {
            return null;
        } else if(str instanceof String) {
            String s = (String) str;
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

        throw new FunctionExecutionException(ErrorMessageKeys.FUNCTION_0015, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0015, "initCap", str.getClass().getName())); //$NON-NLS-1$
    }

    // ================== Function = lpad =====================

    public static Object lpad(Object inputString, Object padLength, Object padChar)
        throws FunctionExecutionException {

        if(inputString == null || padLength == null || padChar == null) {
            return null;
        }

        String str = (String) inputString;
        int length = ((Integer)padLength).intValue();
        if(length < 1) {
            throw new FunctionExecutionException(ErrorMessageKeys.FUNCTION_0025, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0025));
        }

        int numPadChar = length - str.length();
        if(numPadChar <= 0) {
            return str;
        }
        // Get pad character
        char ch = 0;
        if(padChar instanceof String) {
            String charStr = (String) padChar;
            if(charStr.length() != 1) {
                throw new FunctionExecutionException(ErrorMessageKeys.FUNCTION_0027, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0027));
            }
            ch = charStr.charAt(0);
        } else {
            ch = ((Character)padChar).charValue();
        }

        // Pad string
        StringBuffer outStr = new StringBuffer();
        for(int i=0; i<numPadChar; i++) {
            outStr.append(ch);
        }
        outStr.append(str);
        return outStr.toString();
    }

    public static final Character SPACE_CHAR = new Character(' ');

    public static Object lpad(Object inputString, Object padLength)
        throws FunctionExecutionException {

        return lpad(inputString, padLength, SPACE_CHAR);
    }

    // ================== Function = rpad =====================

    public static Object rpad(Object inputString, Object padLength, Object padChar)
        throws FunctionExecutionException {

        if(inputString == null || padLength == null || padChar == null) {
            return null;
        }

        String str = (String) inputString;
        int length = ((Integer)padLength).intValue();
        if(length < 1) {
            throw new FunctionExecutionException(ErrorMessageKeys.FUNCTION_0025, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0025));
        }

        int numPadChar = length - str.length();
        if(numPadChar <= 0) {
            return str;
        }
        // Get pad character
        char ch = 0;
        if(padChar instanceof String) {
            String charStr = (String) padChar;
            if(charStr.length() != 1) {
                throw new FunctionExecutionException(ErrorMessageKeys.FUNCTION_0029, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0029));
            }
            ch = charStr.charAt(0);
        } else {
            ch = ((Character)padChar).charValue();
        }

        // Pad string
        StringBuffer outStr = new StringBuffer();
        outStr.append(str);
        for(int i=0; i<numPadChar; i++) {
            outStr.append(ch);
        }
        return outStr.toString();
    }

    public static Object rpad(Object inputString, Object padLength)
        throws FunctionExecutionException {

        return rpad(inputString, padLength, SPACE_CHAR);
    }

    // ================== Function = translate =====================

    public static Object translate(Object inputString, Object srcChars, Object destChars)
        throws FunctionExecutionException {

        if(inputString == null || srcChars == null || destChars == null) {
            return null;
        }

        String str = (String) inputString;
        String in = (String) srcChars;
        String out = (String) destChars;

        if(in.length() != out.length()) {
            throw new FunctionExecutionException(ErrorMessageKeys.FUNCTION_0031, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0031));
        }

        if(in.length() == 0 || str.length() == 0) {
            return inputString;
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

	public static Object convert(Object src, Object type)
		throws FunctionExecutionException {

		if(src == null) {
			return null;
		} else if(type instanceof String) {
			String typeStr = (String) type;

			try {
				return DataTypeManager.transformValue(src, DataTypeManager.getDataTypeClass(typeStr));
			} catch(TransformationException e) {
				throw new FunctionExecutionException(e, ErrorMessageKeys.FUNCTION_0033, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0033, new Object[]{src, DataTypeManager.getDataTypeName(src.getClass()), typeStr}));
			}
		}

		throw new FunctionExecutionException(ErrorMessageKeys.FUNCTION_0034, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0034, type));
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

    // ================== Function = decodeInteger =====================

    public static Object decodeInteger(
        Object columnValue,
        Object decodeObject)
        throws FunctionExecutionException {
        return decode(
            columnValue,
            decodeObject,
            DEFAULT_DECODE_STRING_DELIMITER,
            Integer.class);
    }

    public static Object decodeString(
        Object columnValue,
        Object decodeObject)
        throws FunctionExecutionException {
        return decode(
            columnValue,
            decodeObject,
            DEFAULT_DECODE_STRING_DELIMITER,
            String.class);
    }

    public static Object decodeInteger(
        Object columnValue,
        Object decodeObject,
        Object decodeDelimiter)
        throws FunctionExecutionException {
        return decode(
            columnValue,
            decodeObject,
            decodeDelimiter,
            Integer.class);
    }

    public static Object decodeString(
        Object columnValue,
        Object decodeObject,
        Object decodeDelimiter)
        throws FunctionExecutionException {

        return decode(columnValue, decodeObject, decodeDelimiter, String.class);

    }

    private static Object decode(
        Object columnValue,
        Object decodeObject,
        Object decodeDelimiter,
        Class returnType)
        throws FunctionExecutionException {

        Object returnObject = null;

        if (decodeObject == null) {
            throw new FunctionExecutionException(ErrorMessageKeys.FUNCTION_0036, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0036));
        }

        if(decodeDelimiter==null){
            throw new FunctionExecutionException(ErrorMessageKeys.FUNCTION_0037, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0037));
        }

        if (decodeObject instanceof String
            && decodeDelimiter instanceof String) {

            String decodeDelimiterString = (String) decodeDelimiter;
            String decodeString = (String) decodeObject;
            StringTokenizer tokenizer =
                new StringTokenizer(decodeString, decodeDelimiterString);

            while (tokenizer.hasMoreTokens()) {
                String resultString;
                String compareString =
                    convertString(tokenizer.nextToken().trim());
                if (tokenizer.hasMoreTokens()) {
                    resultString = convertString(tokenizer.nextToken().trim());
                } else {
                    /*
                     *  if there are no more tokens in the decode string, the last token in
                     *  the String is considered to be the 'default' value for the
                     * return from this function.  If we reach this point in this loop, then
                     * we just return this string as the return value from this function as
                     * there are no more decode compare strings to be processed.
                     */
                    try {
                        returnObject =
                            convertType(returnType, compareString);
                    } catch (TransformationException e) {
                        throw new FunctionExecutionException(e, ErrorMessageKeys.FUNCTION_0038, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0038, compareString, returnType!=null?returnType.getName():"null")); //$NON-NLS-1$
                    }

                    // we break out of the while if we find an endpoint.
                    break;

                }

                boolean match;

                match = areSemanticallyEqual(columnValue, compareString);

                if (match) {
                    try {
                        returnObject = convertType(returnType, resultString);
                    } catch (TransformationException e) {
                        throw new FunctionExecutionException(e, ErrorMessageKeys.FUNCTION_0038, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0038, resultString, returnType!=null?returnType.getName():"null")); //$NON-NLS-1$
                    }
                    break;
                }else if (!tokenizer.hasMoreTokens()) {
                    /*
                     * if we get to this point and there are no more tokens in the
                     * decode string, this means that we have run out of strings to
                     * compare against the column value.  In this case we simply return
                     * the column value itself.  We are safe doing that here because
                     * this loop will be exited and the specified default value will have
                     * already been returned if there is one.
                     */
                    try {
                        returnObject = convertType(returnType, columnValue);
                    } catch (TransformationException e) {
                        throw new FunctionExecutionException(e, ErrorMessageKeys.FUNCTION_0039, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0039,  columnValue ));
                    }
                }

            }

            return returnObject;

        }
        throw new FunctionExecutionException(ErrorMessageKeys.FUNCTION_0040, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0040));
    }

    private static boolean areSemanticallyEqual(Object object1, String string){
        if(object1==null) {
            if(string==null) {
                return true;
            }
            return false;
        }
        if(string == null) {
            return false;
        } else if(object1.equals(string)) {
            return true;
        }

        Class targetClass = object1.getClass();

        if(DataTypeManager.isTransformable(String.class, targetClass)){
            Transform transform =
                DataTypeManager.getPreferredTransform(String.class, targetClass);
            Object result = null;
            try {
                if (transform.getSourceType().equals(String.class)) {
                    result = transform.transform(string);
                } else {
                    result = transform.transform(object1);
                }
            } catch (TransformationException e) {
                /* if the attempt to convert the types to be compatible so that
                 * they can be propertly compared fails, then we consider these
                 * two objects to be not semantically equal.
                 */
                return false;
            }
            return result.equals(object1);
        }
        return false;
    }

    private static Object convertType(Class targetType, Object objectToConvert)
        throws TransformationException, FunctionExecutionException {

        if(objectToConvert==null){
            return null;
        }

        if(targetType==objectToConvert.getClass()){
            return objectToConvert;
        }

        Transform transform = null;
        if(DataTypeManager.isTransformable(targetType, objectToConvert.getClass())){
            transform =
                DataTypeManager.getTransform(
                    objectToConvert.getClass(),
                    targetType);
        }else{
            throw new FunctionExecutionException(ErrorMessageKeys.FUNCTION_0041, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0041, new Object[]{objectToConvert, objectToConvert.getClass().getName(), targetType.getName()}));
        }
        return transform.transform(objectToConvert);
    }

    public static String convertString(String string) {
        /*
         * if there are no characters in the compare string we designate that as
         * an indication of null.  ie if the decode string looks like this:
         *
         * "'this', 1,,'null'"
         *
         * Then if the value in the first argument is null then the String 'null' is
         * returned from the function.
         */
        if (string.equals("")) { //$NON-NLS-1$
            return null;
        }

        /*
         * we also allow the use of the keyword null in the decode string.  if it
         * wished to match on the string 'null' then the string must be qualified by
         * ' designators.
         */
         if(string.equalsIgnoreCase("null")){ //$NON-NLS-1$
            return null;
         }

        /*
         * Here we check to see if the String in the decode String submitted
         * was surrounded by String literal characters. In this case we strip
         * these literal characters from the String.
         */
        if ((string.startsWith("\"") && string.endsWith("\"")) //$NON-NLS-1$ //$NON-NLS-2$
            || (string.startsWith("'") && string.endsWith("'"))) { //$NON-NLS-1$ //$NON-NLS-2$
            if (string.length() == 2) {
                /*
                 * This is an indication that the desired string to be compared is
                 * the "" empty string, so we return it as such.
                 */
                string = ""; //$NON-NLS-1$
            } else if (!string.equalsIgnoreCase("'") && !string.equalsIgnoreCase("\"")){ //$NON-NLS-1$ //$NON-NLS-2$
                string = string.substring(1);
                string = string.substring(0, string.length()-1);
            }
        }

        return string;
    }

    // ================== Function = nvl =====================

    public static Object nvl(Object value, Object valueIfNull) {

        if(value == null) {
            return valueIfNull;
        }
        return value;
    }

	// ================== Format date/time/timestamp TO String ==================
	public static Object formatDate(Object date, Object format)
		throws FunctionExecutionException {
		if (date == null || format == null) {
			return null;
		}

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
		if (time == null || format == null) {
			return null;
		}

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
		if (timestamp == null || format == null) {
			return null;
		}

		try {
            SimpleDateFormat sdf = new SimpleDateFormat((String)format);
            return sdf.format((Timestamp) timestamp);
		} catch (IllegalArgumentException iae) {
			throw new FunctionExecutionException(ErrorMessageKeys.FUNCTION_0042, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0042 ,
				iae.getMessage()));
		}
	}

	//	================== Parse String TO date/time/timestamp  ==================
	public static Object parseDate(Object date, Object format)
		throws FunctionExecutionException {
		java.util.Date parsedDate = null;

		if (date == null || format == null) {
			return null;
		}

		try {
			DateFormat df= new SimpleDateFormat((String) format);
			parsedDate = df.parse((String) date);
            return TimestampWithTimezone.createDate(parsedDate);
		} catch (java.text.ParseException pe) {
			throw new FunctionExecutionException(ErrorMessageKeys.FUNCTION_0043, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0043 ,
				date, format));
		}
	}

	public static Object parseTime(Object time, Object format)
		throws FunctionExecutionException {
		java.util.Date date = null;

		if (time == null || format == null) {
			return null;
		}

		try {
			DateFormat df= new SimpleDateFormat((String) format);
			date = df.parse((String) time);
            return TimestampWithTimezone.createTime(date);
		} catch (java.text.ParseException pe) {
			throw new FunctionExecutionException(ErrorMessageKeys.FUNCTION_0043, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0043 ,
				time,format));
		}
	}

	public static Object parseTimestamp(Object timestamp, Object format)
		throws FunctionExecutionException {
		java.util.Date date = null;

		if (timestamp == null || format == null) {
			return null;
		}

		try {
			DateFormat df= new SimpleDateFormat((String) format);
			date = df.parse((String) timestamp);
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
	public static Object acos(Object number) throws FunctionExecutionException {
		if(number == null){
			return null;
		}
		if(number instanceof Double){
			return new Double(Math.acos(((Double)number).doubleValue()));
		}
		throw new FunctionExecutionException(ErrorMessageKeys.FUNCTION_0015, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0015, "acos", number.getClass().getName())); //$NON-NLS-1$
	}

	// ================== Function - ASIN =====================
	public static Object asin(Object number) throws FunctionExecutionException {
		if(number == null){
			return null;
		}
		if(number instanceof Double){
			return new Double(Math.asin(((Double)number).doubleValue()));
		}
		throw new FunctionExecutionException(ErrorMessageKeys.FUNCTION_0015, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0015, "asin", number.getClass().getName())); //$NON-NLS-1$
	}


	// ================== Function - ATAN =====================
	public static Object atan(Object number) throws FunctionExecutionException {
		if(number == null){
			return null;
		}
		if(number instanceof Double){
			return new Double(Math.atan(((Double)number).doubleValue()));
		}
		throw new FunctionExecutionException(ErrorMessageKeys.FUNCTION_0015, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0015, "atan", number.getClass().getName())); //$NON-NLS-1$
	}

	// ================== Function - ATAN2 =====================
	public static Object atan2(Object number1, Object number2) throws FunctionExecutionException {
		if(number1 == null || number2 == null){
			return null;
		}
		if(number1 instanceof Double && number2 instanceof Double ){
			return new Double(Math.atan2(((Double)number1).doubleValue(), ((Double)number2).doubleValue()));
		}
		throw new FunctionExecutionException(ErrorMessageKeys.FUNCTION_0007, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0007, "atan2", number1.getClass().getName(), number2.getClass().getName())); //$NON-NLS-1$
	}

	// ================== Function - COS =====================
	public static Object cos(Object number) throws FunctionExecutionException {
		if(number == null){
			return null;
		}
		if(number instanceof Double){
			return new Double(Math.cos(((Double)number).doubleValue()));
		}
		throw new FunctionExecutionException(ErrorMessageKeys.FUNCTION_0015, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0015, "cos", number.getClass().getName())); //$NON-NLS-1$
	}

	// ================== Function - COT =====================
	public static Object cot(Object number) throws FunctionExecutionException {
		if(number == null){
			return null;
		}
		if(number instanceof Double){
			return new Double(1/Math.tan(((Double)number).doubleValue()));
		}
		throw new FunctionExecutionException(ErrorMessageKeys.FUNCTION_0015, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0015, "cot", number.getClass().getName())); //$NON-NLS-1$
	}


	// ================== Function - DEGREES =====================
	public static Object degrees(Object number) throws FunctionExecutionException {
		if(number == null){
			return null;
		}
		if(number instanceof Double){
			return new Double(Math.toDegrees(((Double)number).doubleValue()));
		}
		throw new FunctionExecutionException(ErrorMessageKeys.FUNCTION_0015, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0015, "degrees", number.getClass().getName())); //$NON-NLS-1$
	}

	// ================== Function - PI =====================
	public static Object pi() {
		return new Double(Math.PI);
	}

	// ================== Function - RADIANS =====================
	public static Object radians(Object number) throws FunctionExecutionException {
		if(number == null){
			return null;
		}
		if(number instanceof Double){
			return new Double(Math.toRadians(((Double)number).doubleValue()));
		}
		throw new FunctionExecutionException(ErrorMessageKeys.FUNCTION_0015, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0015, "redians", number.getClass().getName())); //$NON-NLS-1$
	}

	// ================== Function - SIN =====================
	public static Object sin(Object number) throws FunctionExecutionException {
		if(number == null){
			return null;
		}
		if(number instanceof Double){
			return new Double(Math.sin(((Double)number).doubleValue()));
		}
		throw new FunctionExecutionException(ErrorMessageKeys.FUNCTION_0015, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0015, "sin", number.getClass().getName())); //$NON-NLS-1$
	}

	// ================== Function - TAN =====================
	public static Object tan(Object number) throws FunctionExecutionException {
		if(number == null){
			return null;
		}
		if(number instanceof Double){
			return new Double(Math.tan(((Double)number).doubleValue()));
		}
		throw new FunctionExecutionException(ErrorMessageKeys.FUNCTION_0015, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0015, "tan", number.getClass().getName())); //$NON-NLS-1$
	}

    // ================== Function - BITAND =====================
	public static Object bitand(Object x, Object y) throws FunctionExecutionException {
        if (x == null || y == null) {
            return null;
        }
        if (!(x instanceof Integer)) {
            throw new FunctionExecutionException(ErrorMessageKeys.FUNCTION_0015, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0015, "bitand", x.getClass().getName())); //$NON-NLS-1$
        }
        if (!(y instanceof Integer)) {
            throw new FunctionExecutionException(ErrorMessageKeys.FUNCTION_0015, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0015, "bitand", y.getClass().getName())); //$NON-NLS-1$
        }
        return new Integer(((Integer)x).intValue() & ((Integer)y).intValue());
	}

    // ================== Function - BITOR =====================
    public static Object bitor(Object x, Object y) throws FunctionExecutionException {
        if (x == null || y == null) {
            return null;
        }
        if (!(x instanceof Integer)) {
            throw new FunctionExecutionException(ErrorMessageKeys.FUNCTION_0015, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0015, "bitor", x.getClass().getName())); //$NON-NLS-1$
        }
        if (!(y instanceof Integer)) {
            throw new FunctionExecutionException(ErrorMessageKeys.FUNCTION_0015, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0015, "bitor", y.getClass().getName())); //$NON-NLS-1$
        }
        return new Integer(((Integer)x).intValue() | ((Integer)y).intValue());
    }

    // ================== Function - BITXOR =====================
    public static Object bitxor(Object x, Object y) throws FunctionExecutionException {
        if (x == null || y == null) {
            return null;
        }
        if (!(x instanceof Integer)) {
            throw new FunctionExecutionException(ErrorMessageKeys.FUNCTION_0015, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0015, "bitxor", x.getClass().getName())); //$NON-NLS-1$
        }
        if (!(y instanceof Integer)) {
            throw new FunctionExecutionException(ErrorMessageKeys.FUNCTION_0015, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0015, "bitxor", y.getClass().getName())); //$NON-NLS-1$
        }
        return new Integer(((Integer)x).intValue() ^ ((Integer)y).intValue());
    }

    // ================== Function - BITNOT =====================
    public static Object bitnot(Object x) throws FunctionExecutionException {
        if (x == null) {
            return null;
        }
        if (!(x instanceof Integer)) {
            throw new FunctionExecutionException(ErrorMessageKeys.FUNCTION_0015, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0015, "bitxor", x.getClass().getName())); //$NON-NLS-1$
        }
        return new Integer(((Integer)x).intValue() ^ 0xFFFFFFFF);
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

    public static Object commandPayload(CommandContext context, Object param) 
        throws ExpressionEvaluationException, FunctionExecutionException{
        Serializable payload = context.getCommandPayload();
        if(payload == null || param == null) {
            return null;
        }
        
        if (param instanceof String) {
            // 1-arg form - assume payload is a Properties object
            if(payload instanceof Properties) {
                String property = (String)param;                
                return ((Properties)payload).getProperty(property);
            }            
            // Payload was bad
            throw new ExpressionEvaluationException(QueryPlugin.Util.getString("ExpressionEvaluator.Expected_props_for_payload_function", "commandPayload", payload.getClass().getName())); //$NON-NLS-1$ //$NON-NLS-2$
        }
        throw new FunctionExecutionException(ErrorMessageKeys.FUNCTION_0071, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0070, "commandPayload", param.getClass().getName())); //$NON-NLS-1$
    }

    // ================= Function - ENV ========================
    public static Object env(CommandContext context, Object param) throws FunctionExecutionException {
        if (param == null) {
            return null;
        }      
        if (param instanceof String) {
            // All context property keys must be lowercase - we lowercase the incoming key here to match regardless of case
            String propertyName = ((String)param);
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
        throw new FunctionExecutionException(ErrorMessageKeys.FUNCTION_0070, QueryPlugin.Util.getString(ErrorMessageKeys.FUNCTION_0070, "env", param.getClass().getName())); //$NON-NLS-1$
    }
    
    // ================= Function - MODIFYTIMEZONE ========================
    
    public static Object modifyTimeZone(Object value, Object originalTimezoneString, Object targetTimezoneString) {
        if (value == null || originalTimezoneString == null || targetTimezoneString == null) {
            return null;
        }

        TimeZone originalTimeZone = TimeZone.getTimeZone((String)originalTimezoneString);
        TimeZone dbmsTimeZone = TimeZone.getTimeZone((String)targetTimezoneString);

        // Check that the dbms time zone is really different than the local time zone
        if (originalTimeZone.equals(dbmsTimeZone)) {
            return value;
        }

        Calendar cal = Calendar.getInstance(dbmsTimeZone);
        
        Timestamp in = (Timestamp)value;
        
        return TimestampWithTimezone.createTimestamp(in, originalTimeZone, cal);
    }

    public static Object modifyTimeZone(CommandContext context, Object value, Object targetTimezoneString) {
        if (value == null || targetTimezoneString == null) {
            return null;
        }

        TimeZone dbmsTimeZone = TimeZone.getTimeZone((String)targetTimezoneString);

        Calendar cal = Calendar.getInstance(dbmsTimeZone);
        
        Timestamp in = (Timestamp)value;
        
        return TimestampWithTimezone.createTimestamp(in, context.getServerTimeZone(), cal);
    } 
    
}

