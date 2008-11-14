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

/*
 */
package com.metamatrix.connector.object.util;

import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.List;
import java.util.TimeZone;

import com.metamatrix.common.util.TimestampWithTimezone;
import com.metamatrix.connector.jdbc.extension.ValueTranslator;
import com.metamatrix.connector.jdbc.extension.impl.BasicValueTranslator;
import com.metamatrix.connector.object.ObjectPlugin;
import com.metamatrix.connector.object.extension.IObjectCommand;
import com.metamatrix.connector.object.extension.ISourceTranslator;
import com.metamatrix.core.util.StringUtil;
import com.metamatrix.data.api.Batch;
import com.metamatrix.data.api.ExecutionContext;
import com.metamatrix.data.api.TypeFacility;
import com.metamatrix.data.basic.BasicBatch;
import com.metamatrix.data.exception.ConnectorException;
import com.metamatrix.data.visitor.util.SQLReservedWords;

/**
 */
public class ObjectExecutionHelper implements SQLReservedWords {
    private static final String ESCAPED_QUOTE = "''"; //$NON-NLS-1$

    private static final TimeZone LOCAL_TIME_ZONE = TimeZone.getDefault();
  
    /**
    * Creates a SQL-safe string. Simply replaces all occurrences of ' with ''
    * @param str the input string
    * @return a SQL-safe string
    */
   protected String escapeString(String str) {
       return StringUtil.replaceAll(str, QUOTE, ESCAPED_QUOTE);
   }    
    
    /**
     * Subclasses should override this method to provide a different sql translation
     * of the literal boolean value.  By default, a boolean literal is represented as:
     * <code>'0'</code> or <code>'1'</code>.
     * @param booleanValue Boolean value, never null
     * @return Translated string
     */
    protected String translateLiteralBoolean(Boolean booleanValue, TimeZone databaseTimeZone) {
        if(booleanValue.booleanValue()) {
            return "1"; //$NON-NLS-1$
        }
        return "0"; //$NON-NLS-1$
    }

    /**
     * Subclasses should override this method to provide a different sql translation
     * of the literal date value.  By default, a date literal is represented as:
     * <code>{d'2002-12-31'}</code>
     * @param dateValue Date value, never null
     * @return Translated string
     */
    protected String translateLiteralDate(java.sql.Date dateValue, TimeZone databaseTimeZone) {
        return "{d'" + formatDateValue(dateValue, databaseTimeZone) + "'}"; //$NON-NLS-1$ //$NON-NLS-2$
    }

    /**
     * Subclasses should override this method to provide a different sql translation
     * of the literal time value.  By default, a time literal is represented as:
     * <code>{t'23:59:59'}</code>
     * @param timeValue Time value, never null
     * @return Translated string
     */
    protected String translateLiteralTime(Time timeValue, TimeZone databaseTimeZone) {
        return "{t'" + formatDateValue(timeValue,databaseTimeZone) + "'}"; //$NON-NLS-1$ //$NON-NLS-2$
    }

    /**
     * Subclasses should override this method to provide a different sql translation
     * of the literal timestamp value.  By default, a timestamp literal is
     * represented as: <code>{ts'2002-12-31 23:59:59'}</code>.
     * @param timestampValue Timestamp value, never null
     * @return Translated string
     */
    protected String translateLiteralTimestamp(Timestamp timestampValue, TimeZone databaseTimeZone) {
        return "{ts'" + formatDateValue(timestampValue, databaseTimeZone) + "'}"; //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    /**
     * Format the dateObject (of type date, time, or timestamp) into a string
     * using the DatabaseTimeZone format.
     * @param dateObject
     * @return Formatted string
     */
    protected String formatDateValue(Object dateObject, TimeZone databaseTimeZone) {
        if(databaseTimeZone == null) {
            return dateObject.toString();
        }
        
//System.out.println("!!! translating timestamp value " + dateObject + " (" + ((java.util.Date)dateObject).getTime() + " in " + this.databaseTimeZone);        
        
        if(dateObject instanceof Timestamp) {
            SimpleDateFormat timestampFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); //$NON-NLS-1$
            timestampFormatter.setTimeZone(databaseTimeZone);

            Timestamp ts = (Timestamp) dateObject;                  
            String nanoStr = "" + (1000000000L + ts.getNanos()); //$NON-NLS-1$
            while(nanoStr.length() > 2 && nanoStr.charAt(nanoStr.length()-1) == '0') {
                nanoStr = nanoStr.substring(0, nanoStr.length()-1);
            }
            String tsStr = timestampFormatter.format(ts) + "." + nanoStr.substring(1); //$NON-NLS-1$
            
//System.out.println("!!!   returning " + tsStr);            
            
            return tsStr;
                    
        } else if(dateObject instanceof java.sql.Date) {
            SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd"); //$NON-NLS-1$
            dateFormatter.setTimeZone(databaseTimeZone);
            return dateFormatter.format((java.sql.Date)dateObject);
            
        } else if(dateObject instanceof Time) {
            SimpleDateFormat timeFormatter = new SimpleDateFormat("HH:mm:ss"); //$NON-NLS-1$
            timeFormatter.setTimeZone(databaseTimeZone);
            return timeFormatter.format((java.sql.Time)dateObject);
            
        } else {
            return dateObject.toString();
        }       
    }    
    
    /**
     * Create batch for a stored procedure.
     * @param results
     * @param columnDataTypes
     * @param statement
     * @param maxBatchSize
     * @return Created batch.
     */
    public static Batch createBatch(List spResults, IObjectCommand command, int maxBatchSize, boolean trimStrings, ISourceTranslator resultsTranslator, ExecutionContext context)  throws ConnectorException {
        //Class[] columnDataTypes
        Batch batch = new BasicBatch();
        transferResults(batch, spResults, command, maxBatchSize, true, trimStrings, resultsTranslator, context);
        return batch;
    }
    
   
    /**
     * Transfer results for Stored Procedure.
     * @param batch The target batch
     * @param spResults The original stored procedure results in format of <List<List>>
     * @param columnTypes An array of column types, including the OUT/RETURN/INOUT parameters
     * @param maxBatchSize Max size for batch
     * @param setLastBatch flag indicating set the last batch or not
     * @throws ConnectorException
     */
    protected static void transferResults(Batch batch, List spResults, IObjectCommand command, int maxBatchSize, boolean setLastBatch, boolean trimStrings, ISourceTranslator resultsTranslator, ExecutionContext context) throws ConnectorException{
        Class[] columnTypes = command.getResultColumnTypes();

        ValueTranslator transform = null;
        
        // Move the result data to the query results
        int rowCnt = 0;
        int rsize = spResults.size();
        TimeZone dbmsTimeZone = resultsTranslator.getDatabaseTimezone();  

        Calendar cal = null;
        if (dbmsTimeZone != null) {
            cal = Calendar.getInstance(dbmsTimeZone);
        }        
        try {
            while (rowCnt < maxBatchSize) {
                if (rowCnt < rsize) {
                                        
                    Object value =  spResults.get(rowCnt);
                    
                        if(value != null) {
                            Class valueType = value.getClass();

                            int javaType = TypeFacility.getSQLTypeFromRuntimeType(valueType.getClass());
                            if (javaType == Types.JAVA_OBJECT) {
                                
                                if (value instanceof Collection || value instanceof List) {
                                    // bypass transformation
                                } else {
                                    transform = determineTransformation(valueType, valueType, resultsTranslator);
                                
                                    value = transform.translate(value, context);
                                }
                                
                                if (value != null) {
                                    if (value instanceof List ) {
                                        List vt = (List) value;
                                        
                                        List lresults = new ArrayList(1);
                                        lresults.add(vt);
                                        
                                        transferObjectResults(batch, lresults, command, trimStrings, resultsTranslator,  context) ;
                                        
                                    } else if (value instanceof Collection) { 
                                        List vt = new ArrayList((Collection) value);
                                        List lresults = new ArrayList(1);
                                        lresults.add(vt);
                                        
                                        transferObjectResults(batch, lresults, command, trimStrings, resultsTranslator,  context) ;
                                        
                                       
                                    } else {
                                        List vt = new ArrayList(1);
                                        vt.add(value);
                                        
                                        List lresults = new ArrayList(1);
                                        lresults.add(vt);
                                        
                                        transferObjectResults(batch, lresults, command, trimStrings, resultsTranslator,  context) ;
                                    }
                                    
                                } else {
                                    List l = new ArrayList(1);
                                    l.add(value);
                                    batch.addRow(l);
                                }
                                

                            } 
                            else {
                                
	                            // if the result is a primitive, non-java
	                            // object, then there should only be
								// one column type
								transform = determineTransformation(valueType,columnTypes[0], resultsTranslator);
	
								// Transform value if necessary
								if (transform != null) {
									value = transform.translate(value, context);
								} else {
									// Convert time zone if necessary
									value = modifyTimeZone(value, dbmsTimeZone, cal);
								}
	
								// Trim string column if necessary
								if (trimStrings && (value instanceof String)) {
									// if(trimColumn[i]) {
									value = trimString((String) value);
								}
								
								List l = new ArrayList(1);
								l.add(value);
								batch.addRow(l);
                            }
                            rowCnt++;
                        } 
                } else {
                    break;
                }
            }
            
            if( (rowCnt == maxBatchSize || rowCnt == rsize) && setLastBatch){
                // no more row then set last batch
                batch.setLast();
            }

        } catch (ConnectorException e) {
            throw e;
        } catch (Throwable e) {
            throw new ConnectorException(
                e,
                ObjectPlugin.Util.getString("ObjectExecutionHelper.Unknown_error_translating_results___9", e.getMessage())); //$NON-NLS-1$
        }
    }  
    
    protected static void transferObjectResults(Batch batch, List spResults, IObjectCommand command, boolean trimStrings, ISourceTranslator resultsTranslator, ExecutionContext context) throws ConnectorException{
        Class[] columnTypes = command.getResultColumnTypes();

        // Build up list of flags on whether to trim strings
        boolean[] trimColumn = new boolean[columnTypes.length];     
        if(trimStrings) {
            for(int i=0; i<columnTypes.length; i++) {
                if(columnTypes[i].equals(String.class)) {
                    trimColumn[i] = true;
                }
            }
        }

        // Reusable list of transformations for the types, lazily loaded
        boolean[] transformKnown = new boolean[columnTypes.length];
        ValueTranslator[] transforms = new ValueTranslator[columnTypes.length];

        // Move the result data to the query results
        List vals = null;
        int numCols = columnTypes.length;
        int rowCnt = 0;
        TimeZone dbmsTimeZone = resultsTranslator.getDatabaseTimezone();  
        Calendar cal = null;
        if (dbmsTimeZone != null) {
            cal = Calendar.getInstance(dbmsTimeZone);
        }        
        try {
                if (rowCnt < spResults.size()) {
                    vals = new ArrayList(numCols);
                    List valueList = (List) spResults.get(rowCnt);
                    
                    // transform each column value in the current row of the list if necessary
                    for (int i = 0; i < numCols; i++) {
                        Object value = valueList.get(i);
                        if(value != null) {
                            // Determine transformation if unknown
                            if(! transformKnown[i]) {
                                Class valueType = value.getClass();
                                if(valueType != columnTypes[i]) {                                    
                                    transforms[i] = determineTransformation(valueType, columnTypes[i], resultsTranslator);
                                }
                                transformKnown[i] = true;
                            }

// System.out.println("\nRead value = " + value + " of type " + (value != null ? value.getClass().getName() : ""));

                            // Transform value if necessary
                            if(transforms[i] != null) {
                                value = transforms[i].translate(value, context);
                            }

                            // Convert time zone if necessary
                            value = modifyTimeZone(value, dbmsTimeZone, cal);                      
// System.out.println("After modify time zone: value = " + value + " of type " + (value != null ? value.getClass().getName() : ""));

                            // Trim string column if necessary
                            if(trimColumn[i]) {
                                value = trimString((String) value);
                            }  
                        } 
                        vals.add(value); 
                    }

                    // Add a row to the result set and  set the local variable to determine if more rows should be read
                    batch.addRow(vals);
                    rowCnt++;
                } 


        } catch (ConnectorException e) {
            throw e;
        } catch (Throwable e) {
            throw new ConnectorException(e,ObjectPlugin.Util.getString("ObjectExecutionHelper.Unknown_error_translating_results___9", e.getMessage())); //$NON-NLS-1$
        }
    }  
    
    
    /**
     * Modify the time zone of the object if a databaseTimeZone is defined
     * and this value is a Date, Time, or Timestamp.
     * @param value The value, never null
     * @return New value
     */
    private static Object modifyTimeZone(Object value, TimeZone dbmsTimeZone, Calendar target) {       
        if(value instanceof TimestampWithTimezone) {
            return value;
            
        } else if(dbmsTimeZone != null && value instanceof java.util.Date) {
            if(value instanceof Timestamp) {
                return TimestampWithTimezone.createTimestamp((Timestamp)value, LOCAL_TIME_ZONE, target);               
                
            } else if(value instanceof java.sql.Date) {
                return TimestampWithTimezone.createDate((java.sql.Date)value, LOCAL_TIME_ZONE, target);                      

    
            } else if(value instanceof Time) {
                return TimestampWithTimezone.createTime((Time)value, LOCAL_TIME_ZONE, target);                      
            }
            return TimestampWithTimezone.createDate((java.util.Date)value, LOCAL_TIME_ZONE, target);
            
        }                                        

        return value;
    }
    
    /**
     * @param actualType
     * @param expectedType
     * @return Transformation between actual and expected type
     */
    protected static ValueTranslator determineTransformation(Class actualType, Class expectedType, ISourceTranslator sourceTranslator) throws ConnectorException {
        ValueTranslator valueTranslator = null;

        List<ValueTranslator> valueTranslators = sourceTranslator.getValueTranslators();
        
        // Now check to see if there is an override to the translator
        //check valueTranslators first
        if(valueTranslators != null && !valueTranslators.isEmpty()){
        	for(ValueTranslator translator:valueTranslators) {
                //Evaluate expressions in this order for performance.
                if(expectedType.equals(translator.getTargetType()) || translator.getSourceType().isAssignableFrom(actualType)){
                    valueTranslator = translator;
                    break;
                }
        	}        	
        }
        
        if(valueTranslator == null){
        	TypeFacility typeFacility = sourceTranslator.getTypeFacility();
        	if (typeFacility.hasTransformation(actualType, expectedType)) {
        		valueTranslator = new BasicValueTranslator(actualType, expectedType, typeFacility);
            } else {
                throw new ConnectorException(ObjectPlugin.Util.getString("ObjectExecutionHelper.Unable_to_translate_data_value__1", actualType.getName(), expectedType.getName())); //$NON-NLS-1$
            }
        }
        return valueTranslator;
    }

        
    /**
     * Expects string to never be null 
     * @param value Incoming value
     * @return Right trimmed value  
     * @since 4.2
     */
    static String trimString(String value) {
        for(int i=value.length()-1; i>=0; i--) {
            if(value.charAt(i) != ' ') {
                // end of trim, return what's left
                return value.substring(0, i+1);
            }
        }

        // All spaces, so trim it all
        return ""; //$NON-NLS-1$        
    }
    
   
}
