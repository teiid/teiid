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

/*
 */
package com.metamatrix.connector.object.util;

import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.List;
import java.util.TimeZone;

import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.ExecutionContext;
import org.teiid.connector.api.TypeFacility;
import org.teiid.connector.visitor.util.SQLReservedWords;

import com.metamatrix.connector.object.ObjectPlugin;
import com.metamatrix.connector.object.extension.IObjectCommand;
import com.metamatrix.connector.object.extension.ISourceTranslator;
import com.metamatrix.core.util.StringUtil;

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
     * Transfer results for Stored Procedure.
     * @param batch The target batch
     * @param spResults The original stored procedure results in format of <List<List>>
     * @param columnTypes An array of column types, including the OUT/RETURN/INOUT parameters
     * @param maxBatchSize Max size for batch
     * @param setLastBatch flag indicating set the last batch or not
     * @throws ConnectorException
     */
    public static List transferResults(List spResults, IObjectCommand command, boolean trimStrings, ISourceTranslator resultsTranslator, ExecutionContext context) throws ConnectorException{
        Class[] columnTypes = command.getResultColumnTypes();

        List batch = new ArrayList(spResults.size());
        
        // Move the result data to the query results
        TimeZone dbmsTimeZone = resultsTranslator.getDatabaseTimezone();  

        Calendar cal = null;
        if (dbmsTimeZone != null) {
            cal = Calendar.getInstance(dbmsTimeZone);
        }        
        try {
        	for (Object value : spResults) {
        		if (value == null) {
        			batch.add(Arrays.asList(null));
        			continue;
        		}
                Class valueType = value.getClass();

                int javaType = TypeFacility.getSQLTypeFromRuntimeType(valueType.getClass());
                if (javaType == Types.JAVA_OBJECT) {
                    
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
                        batch.add(Arrays.asList(value));
                    }
                } 
                else {
					// Trim string column if necessary
					if (trimStrings && (value instanceof String)) {
						// if(trimColumn[i]) {
						value = trimString((String) value);
					}
                    batch.add(Arrays.asList(value));
                }
            } 
        } catch (ConnectorException e) {
            throw e;
        } catch (Throwable e) {
            throw new ConnectorException(
                e,
                ObjectPlugin.Util.getString("ObjectExecutionHelper.Unknown_error_translating_results___9", e.getMessage())); //$NON-NLS-1$
        }
        return batch;
    }  
    
    protected static void transferObjectResults(List batch, List spResults, IObjectCommand command, boolean trimStrings, ISourceTranslator resultsTranslator, ExecutionContext context) throws ConnectorException{
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
                            // Trim string column if necessary
                            if(trimColumn[i]) {
                                value = trimString((String) value);
                            }  
                        } 
                        vals.add(value); 
                    }

                    // Add a row to the result set and  set the local variable to determine if more rows should be read
                    batch.add(vals);
                    rowCnt++;
                } 
        } catch (Throwable e) {
            throw new ConnectorException(e,ObjectPlugin.Util.getString("ObjectExecutionHelper.Unknown_error_translating_results___9", e.getMessage())); //$NON-NLS-1$
        }
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
