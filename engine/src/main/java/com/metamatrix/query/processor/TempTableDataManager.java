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

package com.metamatrix.query.processor;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.common.buffer.BlockedException;
import com.metamatrix.common.buffer.TupleSource;
import com.metamatrix.common.buffer.TupleSourceID;
import com.metamatrix.query.sql.lang.Command;
import com.metamatrix.query.tempdata.TempTableStore;
import com.metamatrix.query.util.CommandContext;

/**
 * This proxy ProcessorDataManager is used during XML query processing to handle temporary groups
 * in the document model.  Temp groups are materialized during processing, and their tuple sources
 * are cached, so this proxy shortcuts the need to go to the DataTierManager and immediately
 * returns the tuple source (synchronously) if a temp group's source is what's being requested.
 */
public class TempTableDataManager implements ProcessorDataManager {

    private ProcessorDataManager processorDataManager;
    private TempTableStore tempTableStore;

    /**
     * Constructor takes the "real" ProcessorDataManager that this object will be a proxy to,
     * and will pass most calls through to transparently.  Only when a request is registered for
     * a temp group will this proxy do it's thing.  A ProcessorEnvironment is needed to to 
     * access cached information about temp groups
     * @param processorDataManager the real ProcessorDataManager that this object is a proxy to
     * @param env a ProcessorEnvironment implementation
     */
    public TempTableDataManager(ProcessorDataManager processorDataManager, TempTableStore tempTableStore){
        this.processorDataManager = processorDataManager;
        this.tempTableStore = tempTableStore;
    }

	/**
     * This is the magic method.  If the command is selecting from a temporary group, that 
     * temporary groups tuple source (which is cached in the ProcessorEnvironment) will
     * be retrieved and immediately (synchronously) delivered to the QueryProcessor.
     * If a temp group is <i>not</i> being selected from, then this request will be
     * passed through to the underlying ProcessorDataManager.
	 * @throws MetaMatrixProcessingException 
	 * @see com.metamatrix.query.processor.ProcessorDataManager#registerRequest(Object, Command, String, String, TupleSourceID)
	 */
	public TupleSource registerRequest(
		Object processorID,
		Command command,
		String modelName,
		String connectorBindingId, int nodeID)
		throws MetaMatrixComponentException, MetaMatrixProcessingException {          

        if(tempTableStore != null) {
            TupleSource result = tempTableStore.registerRequest(command);
            if (result != null) {
            	return result;
            }
        }
        return this.processorDataManager.registerRequest(processorID, command, modelName, connectorBindingId, nodeID);
	}

    public Object lookupCodeValue(
        CommandContext context,
        String codeTableName,
        String returnElementName,
        String keyElementName,
        Object keyValue)
        throws BlockedException, MetaMatrixComponentException, MetaMatrixProcessingException {
            
        return this.processorDataManager.lookupCodeValue(context, codeTableName, returnElementName, keyElementName, keyValue);
    }
    
    @Override
    public void clearCodeTables() {
    	this.processorDataManager.clearCodeTables();
    }
    
}
