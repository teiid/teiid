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

package org.teiid.query.sql.lang;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.core.TeiidComponentException;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.util.VariableContext;



/** 
 * Represents a batch of INSERT, UPDATE, DELETE, and SELECT INTO commands
 * @since 4.2
 */
public class BatchedUpdateCommand extends Command {
    
    protected List commands;
    private List<VariableContext> variableContexts; //processing state
    
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
     * @see org.teiid.query.sql.lang.Command#getSubCommands()
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
     * @see org.teiid.query.sql.lang.Command#getType()
     * @since 4.2
     */
    public int getType() {
        return Command.TYPE_BATCHED_UPDATE;
    }

    /** 
     * @see org.teiid.query.sql.lang.Command#getProjectedSymbols()
     * @since 4.2
     */
    public List getProjectedSymbols() {
        return Command.getUpdateCommandSymbol();
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
	 * @see org.teiid.query.sql.lang.Command#areResultsCachable()
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

	public void setVariableContexts(List<VariableContext> variableContexts) {
		this.variableContexts = variableContexts;
	}

	public List<VariableContext> getVariableContexts() {
		return variableContexts;
	}

}
