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

package com.metamatrix.connector.sysadmin.extension.command;

import java.util.Iterator;
import java.util.List;

import com.metamatrix.data.exception.ConnectorException;
import com.metamatrix.data.language.IParameter;
import com.metamatrix.data.language.IProcedure;
import com.metamatrix.data.metadata.runtime.RuntimeMetadata;


/** 
 * Note: This is different from the @link ObjectProcedure because this doesn't have specific code 
 *      looking for nameInSources with a specific naming conventions for getters and setters for 
 *      querying the index files
 *      
 * @since 4.3
 */
public class ProcedureCommand extends BaseProcedureCommand {
    
    
    public ProcedureCommand(final RuntimeMetadata metadata, final IProcedure command) throws ConnectorException {
        super(metadata, command);
    }
        
    
    /** 
     * @see com.metamatrix.connector.sysadmin.extension.command.BaseProcedureCommand#setCriteria()
     * @since 4.3
     */
    void setCriteria() throws ConnectorException {
        List parms = getParameters();
        if (parms != null && parms.size() > 0) {
            for (Iterator it=parms.iterator(); it.hasNext();) {
                IParameter iparm = (IParameter) it.next();
                String nameInSource = determineName(iparm); 
                addCriteria(iparm.getType(), iparm.getValue(), nameInSource);
            }
        }
    }  
    

    
}
