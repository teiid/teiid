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

import java.util.*;

import org.teiid.common.buffer.BlockedException;
import org.teiid.common.buffer.TupleSource;
import org.teiid.core.TeiidComponentException;
import org.teiid.dqp.internal.datamgr.LanguageBridgeFactory;
import org.teiid.events.EventDistributor;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.util.CommandContext;



/** 
 * @since 4.2
 */
public class HardcodedDataManager implements
                                 ProcessorDataManager {

    // sql string to data
    private Map<String, List<?>[]> data = new HashMap<String, List<?>[]>();
    
    // valid models - if null, any is assumed valid
    private Set<String> validModels;
    
    private boolean mustRegisterCommands = true;
    
    private boolean blockOnce;
    
    // Collect all commands run against this class
    private List<Command> commandHistory = new ArrayList<Command>(); // Commands
    private List<org.teiid.language.Command> pushdownCommands = new ArrayList<org.teiid.language.Command>(); // Commands
    
    private LanguageBridgeFactory lbf;
    
    public HardcodedDataManager() {
    	this(true);
    }
    
    public HardcodedDataManager(QueryMetadataInterface metadata) {
    	this(true);
    	this.lbf = new LanguageBridgeFactory(metadata);
    }
    
    public HardcodedDataManager(boolean mustRegisterCommands) {
    	this.mustRegisterCommands = mustRegisterCommands;
    }
    
    public void addData(String sql, List<?>[] rows) {
        data.put(sql, rows);
    }
    
    public void clearData() {
    	this.data.clear();
    	this.commandHistory.clear();
    }
    
    public void setBlockOnce(boolean blockOnce) {
		this.blockOnce = blockOnce;
	}
    
    /**
     * Set of model names that are valid.  Invalid ones will throw an error. 
     * @param models
     * @since 4.2
     */
    public void setValidModels(Set<String> models) {
        this.validModels = models;
    }
    
    /**
     * Return collection of Command that has occurred on this data manager 
     * @return
     * @since 4.2
     */
    public List<Command> getCommandHistory() {
        return this.commandHistory;
    }
    
    public List<org.teiid.language.Command> getPushdownCommands() {
		return pushdownCommands;
	}
    
    /** 
     * @see org.teiid.query.processor.ProcessorDataManager#lookupCodeValue(org.teiid.query.util.CommandContext, java.lang.String, java.lang.String, java.lang.String, java.lang.Object)
     * @since 4.2
     */
    public Object lookupCodeValue(CommandContext context,
                                  String codeTableName,
                                  String returnElementName,
                                  String keyElementName,
                                  Object keyValue) throws BlockedException,
                                                  TeiidComponentException {
        return null;
    }
    
    /** 
     * @see org.teiid.query.processor.ProcessorDataManager#registerRequest(CommandContext, org.teiid.query.sql.lang.Command, java.lang.String, RegisterRequestParameter)
     * @since 4.2
     */
    public TupleSource registerRequest(CommandContext context,
                                Command command,
                                String modelName,
                                RegisterRequestParameter parameterObject) throws TeiidComponentException {
        
        if(modelName != null && validModels != null && ! validModels.contains(modelName)) {
            throw new TeiidComponentException("Detected query against invalid model: " + modelName + ": " + command);  //$NON-NLS-1$//$NON-NLS-2$
        } 
        this.commandHistory.add(command);
        
        List<Expression> projectedSymbols = command.getProjectedSymbols();

        String commandString = null;
        if (lbf == null) {
        	commandString = command.toString();
        } else {
        	org.teiid.language.Command cmd = lbf.translate(command);
        	this.pushdownCommands.add(cmd);
        	commandString = cmd.toString();
        }
        
        List<?>[] rows = data.get(commandString);
        if(rows == null) {
            if (mustRegisterCommands) {
                throw new TeiidComponentException("Unknown command: " + commandString);  //$NON-NLS-1$
            }
            // Create one row of nulls
            rows = new List[1];
            rows[0] = new ArrayList();
            
            for(int i=0; i<projectedSymbols.size(); i++) {
                rows[0].add(null);
            }
        }
        
        FakeTupleSource source = new FakeTupleSource(projectedSymbols, rows);
        if (blockOnce) {
        	source.setBlockOnce();
        }
        return source;
    }

    public void setMustRegisterCommands(boolean mustRegisterCommands) {
        this.mustRegisterCommands = mustRegisterCommands;
    }

	public void clearCodeTables() {
		
	}

	@Override
	public EventDistributor getEventDistributor() {
		return null;
	}
}
