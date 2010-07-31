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

package org.teiid.query.processor;

import org.teiid.common.buffer.BlockedException;
import org.teiid.common.buffer.TupleSource;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.tempdata.TempTableStore;
import org.teiid.query.util.CommandContext;


/**
 * This proxy ProcessorDataManager is used to handle temporary tables.
 */
public class TempTableDataManager implements ProcessorDataManager {

    private ProcessorDataManager processorDataManager;
    private TempTableStore tempTableStore;

    /**
     * Constructor takes the "real" ProcessorDataManager that this object will be a proxy to,
     * and will pass most calls through to transparently.  Only when a request is registered for
     * a temp group will this proxy do it's thing.
     * @param processorDataManager the real ProcessorDataManager that this object is a proxy to
     */
    public TempTableDataManager(ProcessorDataManager processorDataManager, TempTableStore tempTableStore){
        this.processorDataManager = processorDataManager;
        this.tempTableStore = tempTableStore;
    }

	public TupleSource registerRequest(
		CommandContext context,
		Command command,
		String modelName,
		String connectorBindingId, int nodeID)
		throws TeiidComponentException, TeiidProcessingException {          

        if(tempTableStore != null) {
            TupleSource result = tempTableStore.registerRequest(context, command);
            if (result != null) {
            	return result;
            }
        }
        return this.processorDataManager.registerRequest(context, command, modelName, connectorBindingId, nodeID);
	}

    public Object lookupCodeValue(
        CommandContext context,
        String codeTableName,
        String returnElementName,
        String keyElementName,
        Object keyValue)
        throws BlockedException, TeiidComponentException, TeiidProcessingException {
            
        return this.processorDataManager.lookupCodeValue(context, codeTableName, returnElementName, keyElementName, keyValue);
    }
    
    @Override
    public void clearCodeTables() {
    	this.processorDataManager.clearCodeTables();
    }
    
}
