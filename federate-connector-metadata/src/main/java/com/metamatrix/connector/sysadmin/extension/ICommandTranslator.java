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

package com.metamatrix.connector.sysadmin.extension;

import com.metamatrix.data.exception.ConnectorException;
import com.metamatrix.data.language.ICommand;
import com.metamatrix.data.metadata.runtime.RuntimeMetadata;



/**
 * The ICommandTranslator provides the mechinism for intercepting a command
 * and translating before execution.  
 * 
 * An example of this could be the following
 * -  The api requires a custom object for which only the connector has in it's classpath and
 *      is not needed to be exposed to the user.  So the ICommandTranslator would be used
 *      to create that custom object based on parameters passed in the sql statement.  That 
 *      custom object would then replace those in parameters map that passed for actual 
 *      execution on the api.
 */
public interface ICommandTranslator {
           
    
    /**
     * Called and passed a commandName to determine if this translator supports translating that command 
     * @param commandName
     * @return boolean true if this translator will translate the command.
     * @since 4.3
     */
    boolean canTranslateCommand(String commandName);
    
    
    /**
     * Called to obtain the command that will perform the command translation. 
     * @param metadata that is used to build the command
     * @param command is the actual command sent from the query engine
     * @return IObjectCommand to perform the translation
     * @since 4.3
     */
    IObjectCommand getCommand(RuntimeMetadata metadata,
                              ICommand command) throws ConnectorException;
    
     
    

}
