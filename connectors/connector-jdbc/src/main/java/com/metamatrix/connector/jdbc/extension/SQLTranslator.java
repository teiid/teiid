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

package com.metamatrix.connector.jdbc.extension;

import java.sql.Time;
import java.sql.Timestamp;
import java.text.MessageFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

import com.metamatrix.common.util.PropertiesUtils;
import com.metamatrix.connector.api.ConnectorEnvironment;
import com.metamatrix.connector.api.ConnectorException;
import com.metamatrix.connector.api.ExecutionContext;
import com.metamatrix.connector.jdbc.JDBCPropertyNames;
import com.metamatrix.connector.language.ICommand;
import com.metamatrix.connector.language.ILanguageFactory;
import com.metamatrix.connector.language.ILimit;
import com.metamatrix.connector.language.ISetQuery;

/**
 * Base class for creating source SQL queries and retrieving results.
 * Specific databases should override as necessary.
 */
public class SQLTranslator {

    private static final MessageFormat COMMENT = new MessageFormat("/*teiid sessionid:{0}, requestid:{1}.{2}*/ "); //$NON-NLS-1$

    public final static TimeZone DEFAULT_TIME_ZONE = TimeZone.getDefault();

	private Map<String, FunctionModifier> functionModifiers = new HashMap<String, FunctionModifier>();
    private TimeZone databaseTimeZone;
    private ConnectorEnvironment environment;
    
    private boolean useComments;
    private boolean usePreparedStatements;
    
    /**
     * Initialize the SQLTranslator.
     * @param env
     * @param metadata
     * @throws ConnectorException
     */
    public void initialize(ConnectorEnvironment env) throws ConnectorException {
        this.environment = env;

        String timeZone = env.getProperties().getProperty(JDBCPropertyNames.DATABASE_TIME_ZONE);
        if(timeZone != null && timeZone.trim().length() > 0) {
        	TimeZone tz = TimeZone.getTimeZone(timeZone);
            // Check that the dbms time zone is really different than the local time zone
            if(!DEFAULT_TIME_ZONE.hasSameRules(tz)) {
                this.databaseTimeZone = tz;                
            }               
        }   
        
        this.useComments = PropertiesUtils.getBooleanProperty(env.getProperties(), JDBCPropertyNames.USE_COMMENTS_SOURCE_QUERY, false);
        this.usePreparedStatements = PropertiesUtils.getBooleanProperty(env.getProperties(), JDBCPropertyNames.USE_BIND_VARIABLES, false);
    }
    
    public TimeZone getDatabaseTimeZone() {
		return databaseTimeZone;
	}
    
    public ConnectorEnvironment getEnvironment() {
		return environment;
	}
    
    public ILanguageFactory getLanguageFactory() {
    	return environment.getLanguageFactory();
    }
    
    /**
     * Modify the command.
     * @param command
     * @param context
     * @return
     */
    public ICommand modifyCommand(ICommand command, ExecutionContext context) throws ConnectorException {
    	return command;
    }
    
    /**
     * Return a map of function name in lower case to FunctionModifier.
     * @return Map of function name to FunctionModifier.
     */
    public Map<String, FunctionModifier> getFunctionModifiers() {
    	return functionModifiers;
    }
    
    public void registerFunctionModifier(String name, FunctionModifier modifier) {
    	this.functionModifiers.put(name, modifier);
    }
    
    /**
     * Subclasses should override this method to provide a different sql translation
     * of the literal boolean value.  By default, a boolean literal is represented as:
     * <code>'0'</code> or <code>'1'</code>.
     * @param booleanValue Boolean value, never null
     * @return Translated string
     */
    public String translateLiteralBoolean(Boolean booleanValue) {
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
    public String translateLiteralDate(java.sql.Date dateValue, Calendar cal) {
        return "{d'" + formatDateValue(dateValue, cal) + "'}"; //$NON-NLS-1$ //$NON-NLS-2$
    }

    /**
     * Subclasses should override this method to provide a different sql translation
     * of the literal time value.  By default, a time literal is represented as:
     * <code>{t'23:59:59'}</code>
     * @param timeValue Time value, never null
     * @return Translated string
     */
    public String translateLiteralTime(Time timeValue, Calendar cal) {
    	if (!hasTimeType()) {
    		return "{ts'1970-01-01 " + formatDateValue(timeValue, cal) + "'}"; //$NON-NLS-1$ //$NON-NLS-2$
    	}
        return "{t'" + formatDateValue(timeValue, cal) + "'}"; //$NON-NLS-1$ //$NON-NLS-2$
    }

    /**
     * Subclasses should override this method to provide a different sql translation
     * of the literal timestamp value.  By default, a timestamp literal is
     * represented as: <code>{ts'2002-12-31 23:59:59'}</code>.
     * @param timestampValue Timestamp value, never null
     * @return Translated string
     */
    public String translateLiteralTimestamp(Timestamp timestampValue, Calendar cal) {
        return "{ts'" + formatDateValue(timestampValue, cal) + "'}"; //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    /**
     * Format the dateObject (of type date, time, or timestamp) into a string
     * using the DatabaseTimeZone format.
     * @param dateObject
     * @param cal
     * @return Formatted string
     */
    public String formatDateValue(java.util.Date dateObject, Calendar cal) {
        if (dateObject instanceof Timestamp && getTimestampNanoSecondPrecision() < 9) {
        	Timestamp ts = (Timestamp)dateObject;
        	Timestamp newTs = new Timestamp(ts.getTime());
        	if (getTimestampNanoSecondPrecision() > 0) {
	        	int mask = 10^(9-getTimestampNanoSecondPrecision());
	        	newTs.setNanos(ts.getNanos()/mask*mask);
        	}
        	dateObject = newTs;
        }
    	
    	if(cal == null) {
            return dateObject.toString();
        }
        
        return getEnvironment().getTypeFacility().convertDate(dateObject,
				DEFAULT_TIME_ZONE, cal, dateObject.getClass()).toString();        
    }    
    
    public boolean addSourceComment() {
        return useComments;
    }   
    
    public String addLimitString(String queryCommand, ILimit limit) {
    	return queryCommand + " " + limit.toString(); //$NON-NLS-1$
    }
    
    /**
     * Indicates whether group alias should be of the form
     * "...FROM groupA AS X" or "...FROM groupA X".  Certain
     * data sources (such as Oracle) may not support the first
     * form. 
     * @return boolean
     */
    public boolean useAsInGroupAlias(){
        return true;
    }
    
    public boolean usePreparedStatements() {
    	return this.usePreparedStatements;
    }
    
    public boolean useParensForSetQueries() {
    	return false;
    }
    
    public boolean hasTimeType() {
    	return true;
    }
    
    public String getSetOperationString(ISetQuery.Operation operation) {
    	return operation.toString();
    }
    
    public String getSourceComment(ExecutionContext context, ICommand command) {
	    if (addSourceComment() && context != null) {
	    	synchronized (COMMENT) {
	            return COMMENT.format(new Object[] {context.getConnectionIdentifier(), context.getRequestIdentifier(), context.getPartIdentifier()});
			}
	    }
	    return ""; //$NON-NLS-1$ 
    }
    
    public String replaceElementName(String group, String element) {
    	return null;
    }
    
    public int getTimestampNanoSecondPrecision() {
    	return 9;
    }
    
    public String getConnectionTestQuery() {
    	return "select 1"; //$NON-NLS-1$
    }
    
}
