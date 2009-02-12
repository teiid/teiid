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

package com.metamatrix.query.sql.lang;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.QueryMetadataException;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.sql.LanguageVisitor;
import com.metamatrix.query.sql.symbol.GroupSymbol;


/** 
 * Represents a batch of INSERT, UPDATE, DELETE, and SELECT INTO commands
 * @since 4.2
 */
public class BatchedUpdateCommand extends Command implements CommandContainer {
    
    protected List commands;
    
    /**
     * Add sub command
     * @param command Additional sub-command
     */
    public void addSubCommand(Command command) {
        if(this.commands == null) {
            this.commands = new ArrayList();
        }
        this.commands.add(command);
    }

    /**
     * Add sub commands
     * @param commands Additional sub-commands
     */
    public void addSubCommands(Collection commands) {
        if(commands == null || commands.size() == 0) {
            return;
        }
        
        if(this.commands == null) {
            this.commands = new ArrayList();
        } 
        this.commands.addAll(commands);    
    }
    
    /** 
     * @see com.metamatrix.query.sql.lang.Command#getSubCommands()
     */
    public List getSubCommands() {
        if(commands == null || commands.size() == 0) {
            return Collections.EMPTY_LIST;
        }
        return commands;
    }
    
    /**
     *  
     * @param updateCommands
     * @since 4.2
     */
    public BatchedUpdateCommand(List updateCommands) {
        addSubCommands(updateCommands);
    }
    
    /**
     * Gets the List of updates contained in this batch
     * @return
     * @since 4.2
     */
    public List getUpdateCommands() {
        return getSubCommands();
    }

    /** 
     * @see com.metamatrix.query.sql.lang.Command#getType()
     * @since 4.2
     */
    public int getType() {
        return Command.TYPE_BATCHED_UPDATE;
    }

    /** 
     * @see com.metamatrix.query.sql.lang.Command#getProjectedSymbols()
     * @since 4.2
     */
    public List getProjectedSymbols() {
        return Command.getUpdateCommandSymbol();
    }
    
    public int updatingModelCount(QueryMetadataInterface metadata) throws MetaMatrixComponentException{
        //if all the updates are against the same physical source, 
        //the txn is handled in the connector using the transaction
        //from the source, and 0 will be returned
        
        int sunCommandUpdatingModelCount = 0;
        if(commands != null && !commands.isEmpty()) {
            Object sourceModel = null;
            //if all the updates are against the same physical source, 
            //the txn is handled in the connector using the transaction
            //from the source, and 0 will be returned
            Iterator iter = commands.iterator();
            while(iter.hasNext()) {
                Command command = (Command)iter.next();
                GroupSymbol group = null;
                if(command.getType() == Command.TYPE_INSERT) {
                    group = ((Insert)command).getGroup();
                }else if(command.getType() == Command.TYPE_UPDATE) {
                    group = ((Update)command).getGroup();
                }else if(command.getType() == Command.TYPE_DELETE) {
                    group = ((Delete)command).getGroup();
                }else {
                    sunCommandUpdatingModelCount += command.updatingModelCount(metadata);
                }
                
                if(group != null) {
                    try {
                        if(sourceModel == null) {
                            sourceModel = metadata.getModelID(group.getMetadataID());
                            sunCommandUpdatingModelCount += 1;
                            continue;
                        }
                        if(!sourceModel.equals(metadata.getModelID(group.getMetadataID()))) {
                            return 2;
                        }
                    }catch(QueryMetadataException qme) {
                        throw new MetaMatrixComponentException(qme);
                    }
                }
                
                if(sunCommandUpdatingModelCount > 1) {
                    break;
                }
            }
        }
        return sunCommandUpdatingModelCount;
    }

    /** 
     * @since 4.2
     */
    public void acceptVisitor(LanguageVisitor visitor) {
        visitor.visit(this);
    }

    /** 
     * @see java.lang.Object#clone()
     * @since 4.2
     */
    public Object clone() {
        List clonedCommands = new ArrayList(commands.size());
        for (int i = 0; i < commands.size(); i++) {
            clonedCommands.add(((Command)commands.get(i)).clone());
        }
        BatchedUpdateCommand copy = new BatchedUpdateCommand(clonedCommands);
        copyMetadataState(copy);
        return copy;
    }
    
	/**
	 * @see com.metamatrix.query.sql.lang.Command#areResultsCachable()
	 */
	public boolean areResultsCachable() {
		return false;
	}

    public String toString() {
        StringBuffer val = new StringBuffer("BatchedUpdate{"); //$NON-NLS-1$
        if (commands != null && commands.size() > 0) {
            val.append(getCommandToken((Command)commands.get(0)));
            for (int i = 1; i < commands.size(); i++) {
                val.append(',').append(getCommandToken((Command)commands.get(i)));
            }
        }
        val.append('}');
        return val.toString();
    }
    
    private char getCommandToken(Command command) {
        int commandType = command.getType();
        if (commandType == Command.TYPE_INSERT) {
            return 'I';
        } else if (commandType == Command.TYPE_UPDATE) {
            return 'U';
        } else if (commandType == Command.TYPE_DELETE) {
            return 'D';
        } else if (commandType == Command.TYPE_QUERY) {
            // SELECT INTO command
            return 'S';
        }
        return '?';
    }

    /** 
     * @see com.metamatrix.query.sql.lang.CommandContainer#getContainedCommands()
     */
    public List getContainedCommands() {
        return getSubCommands();
    }

}
