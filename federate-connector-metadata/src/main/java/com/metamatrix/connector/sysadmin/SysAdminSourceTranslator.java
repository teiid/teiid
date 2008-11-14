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
package com.metamatrix.connector.sysadmin;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.TimeZone;

import com.metamatrix.connector.sysadmin.extension.ICommandTranslator;
import com.metamatrix.connector.sysadmin.extension.IObjectCommand;
import com.metamatrix.connector.sysadmin.extension.ISourceTranslator;
import com.metamatrix.connector.sysadmin.extension.IValueRetriever;
import com.metamatrix.connector.sysadmin.extension.IValueTranslator;
import com.metamatrix.connector.sysadmin.extension.value.JavaUtilDateToSqlDateValueTranslator;
import com.metamatrix.connector.sysadmin.extension.value.JavaUtilDateToStringValueTranslator;
import com.metamatrix.connector.sysadmin.extension.value.BasicValueRetriever;
import com.metamatrix.connector.sysadmin.extension.value.DefaultProcedureCommandTranslator;
import com.metamatrix.connector.sysadmin.extension.value.JavaUtilDateToTimeStampValueTranslator;
import com.metamatrix.connector.sysadmin.extension.value.SourceInterfaceValueTranslator;
import com.metamatrix.connector.sysadmin.util.SysAdminUtil;
import com.metamatrix.data.api.Batch;
import com.metamatrix.data.api.ConnectorEnvironment;
import com.metamatrix.data.api.ExecutionContext;
import com.metamatrix.data.exception.ConnectorException;
import com.metamatrix.data.language.ICommand;
import com.metamatrix.data.language.IProcedure;
import com.metamatrix.data.metadata.runtime.RuntimeMetadata;

/**
 */
public class SysAdminSourceTranslator implements ISourceTranslator {

    private static final TimeZone LOCAL_TIME_ZONE = TimeZone.getDefault();
    
    private Class apiClazz = null;

    private List valueTranslators = new ArrayList(5);
    private IValueRetriever valueRetriever = new BasicValueRetriever();
    private List commandTranslators = new ArrayList(5);
    private static final ICommandTranslator defaultCommandTranslator=new DefaultProcedureCommandTranslator();
    private TimeZone dbmsTimeZone = null;
    
    SysAdminSourceTranslator (Class clazz) {
        apiClazz = clazz;
    }

    /**
     * @see com.metamatrix.connector.jdbc.extension.ResultsTranslator#initialize(com.metamatrix.data.ConnectorEnvironment)
     */
    public void initialize(ConnectorEnvironment env) throws ConnectorException {
        IValueTranslator valueTranslator;
        
        
        valueTranslator = new SourceInterfaceValueTranslator(apiClazz);
        valueTranslator.initialize(env);
        addValueTranslator(valueTranslator);
        
        
        valueTranslator = new JavaUtilDateToTimeStampValueTranslator();
        valueTranslator.initialize(env);
        addValueTranslator(valueTranslator);
        
        valueTranslator = new JavaUtilDateToSqlDateValueTranslator();
        valueTranslator.initialize(env);
        addValueTranslator(valueTranslator);   
        
        valueTranslator = new JavaUtilDateToStringValueTranslator();
        valueTranslator.initialize(env);
        addValueTranslator(valueTranslator);         
        
// VAH        
// TODO:  add support for blob and clobs

        //add blob translator
//        valueTranslator = new BlobValueTranslator();
//        valueTranslator.initialize(env);
//        addValueTranslator(valueTranslator);
        
        //add clob translator
//        valueTranslator = new ClobValueTranslator();
//        valueTranslator.initialize(env);
//        addValueTranslator(valueTranslator);
        
        
        // setup the default translator
                        
        String timeZone = env.getProperties().getProperty(SysAdminPropertyNames.DATABASE_TIME_ZONE);
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
    
    
    /** 
     * @see com.metamatrix.connector.sysadmin.extension.ISourceTranslator#getCommandTranslators()
     * @since 4.3
     */
    public ICommandTranslator getCommandTranslator(String commandName) {
        for (Iterator it=commandTranslators.iterator(); it.hasNext(); ) {
            ICommandTranslator oc = (ICommandTranslator) it.next();
            if (oc.canTranslateCommand(commandName)) {
                return oc;
            }
        }
        return defaultCommandTranslator;
    }

    
    protected void addCommandTranslator( ICommandTranslator objectCommand) {
        this.commandTranslators.add(objectCommand);
    }
    
    protected void addValueTranslator(IValueTranslator valueTranslator) {
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
            String commandName = SysAdminUtil.getExecutionName(metadata, (IProcedure) command);
            ICommandTranslator ct = getCommandTranslator(commandName);            
            return ct.getCommand(metadata, command); 
        }
        // this should never happend because the command was already filtered when the {@link SysAdminProcedureExecution} calls this.
        // However, if other commands become supported then this method will need to be changed.
        throw new ConnectorException(SysAdminPlugin.Util.getString("SysAdminSourceTranslator.Command_type_not_supported", command.getClass().getName())); //$NON-NLS-1$           
    } 
    
    
}
