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

package com.metamatrix.connector.metadata.internal;

import java.util.ArrayList;
import java.util.List;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.buffer.BlockedException;
import com.metamatrix.common.buffer.TupleSource;
import com.metamatrix.query.processor.FakeTupleSource;
import com.metamatrix.query.processor.ProcessorDataManager;
import com.metamatrix.query.sql.lang.Command;
import com.metamatrix.query.util.CommandContext;

/**
 */
public class FakeProcessorDataManager implements ProcessorDataManager {
    private Command command;
    
    public Command getCommand(){
        return command;
    }
    /**
     * 
     */
    public FakeProcessorDataManager() {
        super();
    }

    /* 
     * @see com.metamatrix.query.processor.ProcessorDataManager#registerRequest(java.lang.Object, com.metamatrix.query.sql.lang.Command, java.lang.String, int)
     */
    public TupleSource registerRequest(Object processorID, Command command, String modelName, String connectorBindingId, int nodeID)
        throws MetaMatrixComponentException {
        this.command = command;
        return new FakeTupleSource(new ArrayList(), new List[0]);
    }

    /* 
     * @see com.metamatrix.query.processor.ProcessorDataManager#lookupCodeValue(com.metamatrix.query.util.CommandContext, java.lang.String, java.lang.String, java.lang.String, java.lang.Object)
     */
    public Object lookupCodeValue(
        CommandContext context,
        String codeTableName,
        String returnElementName,
        String keyElementName,
        Object keyValue)
        throws BlockedException, MetaMatrixComponentException {
        return null;
    }
    
    @Override
    public void clearCodeTables() {
    	
    }

}
