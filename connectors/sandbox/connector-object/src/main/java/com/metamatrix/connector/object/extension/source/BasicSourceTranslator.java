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
package com.metamatrix.connector.object.extension.source;

import java.util.TimeZone;

import org.teiid.connector.api.ConnectorEnvironment;
import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.TypeFacility;
import org.teiid.connector.language.ICommand;
import org.teiid.connector.language.IProcedure;
import org.teiid.connector.metadata.runtime.RuntimeMetadata;

import com.metamatrix.connector.object.ObjectPropertyNames;
import com.metamatrix.connector.object.extension.IObjectCommand;
import com.metamatrix.connector.object.extension.ISourceTranslator;
import com.metamatrix.connector.object.extension.IValueRetriever;
import com.metamatrix.connector.object.extension.command.ProcedureCommand;
import com.metamatrix.connector.object.extension.value.BasicValueRetriever;

/**
 */
public class BasicSourceTranslator implements ISourceTranslator {

    private static final TimeZone LOCAL_TIME_ZONE = TimeZone.getDefault();

    private IValueRetriever valueRetriever = new BasicValueRetriever();
    private TimeZone dbmsTimeZone = null;
    private TypeFacility typeFacility;

    /**
     * @see com.metamatrix.connector.jdbc.extension.ResultsTranslator#initialize(com.metamatrix.data.ConnectorEnvironment)
     */
    public void initialize(ConnectorEnvironment env) throws ConnectorException {

        this.typeFacility = env.getTypeFacility();
        
        
        String timeZone = env.getProperties().getProperty(ObjectPropertyNames.DATABASE_TIME_ZONE);
        if(timeZone != null && timeZone.trim().length() > 0) {
            this.dbmsTimeZone = TimeZone.getTimeZone(timeZone);
                
            // Check that the dbms time zone is really different than the local time zone
            if(LOCAL_TIME_ZONE.equals(timeZone)) {
                this.dbmsTimeZone = null;
            }               
        }               
    }
    
    public IValueRetriever getValueRetriever() {
        return valueRetriever;
    }
    
    public TimeZone getDatabaseTimezone() {
        return this.dbmsTimeZone;
    }       
    
    /** 
     * @see com.metamatrix.connector.object.extension.ISourceTranslator#createObjectCommand(org.teiid.connector.metadata.runtime.RuntimeMetadata, org.teiid.connector.language.IProcedure)
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
