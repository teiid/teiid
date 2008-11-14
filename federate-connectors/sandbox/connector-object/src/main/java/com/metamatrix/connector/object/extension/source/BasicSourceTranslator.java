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
package com.metamatrix.connector.object.extension.source;

import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;

import com.metamatrix.connector.jdbc.extension.ValueTranslator;
import com.metamatrix.connector.object.ObjectPropertyNames;
import com.metamatrix.connector.object.extension.IObjectCommand;
import com.metamatrix.connector.object.extension.ISourceTranslator;
import com.metamatrix.connector.object.extension.IValueRetriever;
import com.metamatrix.connector.object.extension.command.ProcedureCommand;
import com.metamatrix.connector.object.extension.value.BasicValueRetriever;
import com.metamatrix.connector.object.extension.value.JavaUtilDateValueTranslator;
import com.metamatrix.data.api.Batch;
import com.metamatrix.data.api.ConnectorEnvironment;
import com.metamatrix.data.api.ExecutionContext;
import com.metamatrix.data.api.TypeFacility;
import com.metamatrix.data.exception.ConnectorException;
import com.metamatrix.data.language.ICommand;
import com.metamatrix.data.language.IProcedure;
import com.metamatrix.data.metadata.runtime.RuntimeMetadata;

/**
 */
public class BasicSourceTranslator implements ISourceTranslator {

    private static final TimeZone LOCAL_TIME_ZONE = TimeZone.getDefault();

    private List valueTranslators = new ArrayList();
    private IValueRetriever valueRetriever = new BasicValueRetriever();
    private TimeZone dbmsTimeZone = null;
    private TypeFacility typeFacility;

    /**
     * @see com.metamatrix.connector.jdbc.extension.ResultsTranslator#initialize(com.metamatrix.data.ConnectorEnvironment)
     */
    public void initialize(ConnectorEnvironment env) throws ConnectorException {
        ValueTranslator valueTranslator;

        this.typeFacility = env.getTypeFacility();
        
        
        valueTranslator = new JavaUtilDateValueTranslator();
        addValueTranslator(valueTranslator);
                       
        String timeZone = env.getProperties().getProperty(ObjectPropertyNames.DATABASE_TIME_ZONE);
        if(timeZone != null && timeZone.trim().length() > 0) {
            this.dbmsTimeZone = TimeZone.getTimeZone(timeZone);
                
            // Check that the dbms time zone is really different than the local time zone
            if(LOCAL_TIME_ZONE.equals(timeZone)) {
                this.dbmsTimeZone = null;
            }               
        }               
    }
    
 
    /**
     * @see com.metamatrix.connector.jdbc.extension.ResultsTranslator#getValueTranslators()
     */
    public List getValueTranslators() {
        return valueTranslators;
    }
    
    public IValueRetriever getValueRetriever() {
        return valueRetriever;
    }
    
    protected void addValueTranslator(ValueTranslator valueTranslator) {
        valueTranslators.add(valueTranslator);
    }
    
    public TimeZone getDatabaseTimezone() {
        return this.dbmsTimeZone;
    }       
    
    /** 
     * @see com.metamatrix.connector.jdbc.extension.ResultsTranslator#modifyBatch(com.metamatrix.data.api.Batch, com.metamatrix.data.api.ExecutionContext, com.metamatrix.data.language.ICommand)
     * @since 4.2
     */
    public Batch modifyBatch(Batch batch,
                             ExecutionContext context,
                             ICommand command) {
        return batch;
    }
    
    /** 
     * @see com.metamatrix.connector.object.extension.ISourceTranslator#createObjectCommand(com.metamatrix.data.metadata.runtime.RuntimeMetadata, com.metamatrix.data.language.IProcedure)
     * @since 4.3
     */
    public IObjectCommand createObjectCommand(RuntimeMetadata metadata,
                                              ICommand command) throws ConnectorException {
        if (command instanceof IProcedure) {
            return new ProcedureCommand(metadata, (IProcedure) command);
        }
        return null;
    } 
    
 
    public TypeFacility getTypeFacility() {
    	return this.typeFacility;
    }
    
}
