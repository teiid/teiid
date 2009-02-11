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
package com.metamatrix.connector.jdbc.extension.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

import com.metamatrix.connector.api.ConnectorEnvironment;
import com.metamatrix.connector.api.ExecutionContext;
import com.metamatrix.connector.exception.ConnectorException;
import com.metamatrix.connector.jdbc.JDBCPropertyNames;
import com.metamatrix.connector.jdbc.extension.SQLConversionVisitor;
import com.metamatrix.connector.jdbc.extension.SQLTranslator;
import com.metamatrix.connector.language.ICommand;
import com.metamatrix.connector.metadata.runtime.RuntimeMetadata;

/**
 */
public class BasicSQLTranslator implements SQLTranslator {
    public final static TimeZone DEFAULT_TIME_ZONE = TimeZone.getDefault();

    private RuntimeMetadata metadata;
    private ConnectorEnvironment environment;
    private Map functionModifiers = new HashMap();
    private TimeZone databaseTimeZone;
    
    
    /**
     * @see com.metamatrix.connector.jdbc.extension.SQLTranslator#initialize(com.metamatrix.data.ConnectorEnvironment)
     */
    public void initialize(ConnectorEnvironment env, RuntimeMetadata metadata) throws ConnectorException {
        this.metadata = metadata;
        this.environment = env;

        String timeZone = env.getProperties().getProperty(JDBCPropertyNames.DATABASE_TIME_ZONE);
        if(timeZone != null && timeZone.trim().length() > 0) {
        	TimeZone tz = TimeZone.getTimeZone(timeZone);
            // Check that the dbms time zone is really different than the local time zone
            if(!DEFAULT_TIME_ZONE.hasSameRules(tz)) {
                this.databaseTimeZone = tz;                
            }               
        }               
                
    }

    /**
     * Subclass should override this methods to modify the command if necessary.
     * @see com.metamatrix.connector.jdbc.extension.SQLTranslator#modifyCommand(com.metamatrix.connector.language.ICommand, com.metamatrix.data.ExecutionContext)
     */
    public ICommand modifyCommand(ICommand command, ExecutionContext context) throws ConnectorException {
        return command;
    }

    /**
     * @see com.metamatrix.connector.jdbc.extension.SQLTranslator#getTranslationVisitor()
     */
    public SQLConversionVisitor getTranslationVisitor() {
        SQLConversionVisitor visitor = new SQLConversionVisitor();
        visitor.setRuntimeMetadata(metadata);
        visitor.setFunctionModifiers(getFunctionModifiers());
        visitor.setProperties(environment.getProperties());
        visitor.setLanguageFactory(environment.getLanguageFactory());        
        visitor.setDatabaseTimeZone(databaseTimeZone);
        
        return visitor;
    }

    /**
     * @see com.metamatrix.connector.jdbc.extension.SQLTranslator#getFunctionModifiers()
     */
    public Map getFunctionModifiers() {
        return functionModifiers;
    }

    protected RuntimeMetadata getRuntimeMetadata(){
        return this.metadata;
    }
    
    protected ConnectorEnvironment getConnectorEnvironment(){
        return this.environment;
    }

    protected TimeZone getDatabaseTimeZone() {
        return this.databaseTimeZone;
    }
}
