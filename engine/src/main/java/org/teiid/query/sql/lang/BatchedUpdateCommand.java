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
import java.util.List;

import org.teiid.core.util.EquivalenceUtil;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.util.VariableContext;



/** 
 * Represents a batch of INSERT, UPDATE, DELETE, and SELECT INTO commands
 * @since 4.2
 */
public class BatchedUpdateCommand extends Command {
    
	protected List<Command> commands;
    private List<VariableContext> variableContexts; //processing state
    private boolean singleResult;
    
    /**
     *  
     * @param updateCommands
     * @since 4.2
     */
    public BatchedUpdateCommand(List<? extends Command> updateCommands) {
        this(updateCommands, false);
    }
    
    public BatchedUpdateCommand(List<? extends Command> updateCommands, boolean singleResult) {
        this.commands = new ArrayList<Command>(updateCommands);
        this.singleResult = singleResult;
    }
    
    /**
     * Gets the List of updates contained in this batch
     * @return
     * @since 4.2
     */
    public List<Command> getUpdateCommands() {
        return commands;
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
        List<Command> clonedCommands = LanguageObject.Util.deepClone(this.commands, Command.class);
        BatchedUpdateCommand copy = new BatchedUpdateCommand(clonedCommands);
        copy.singleResult = this.singleResult;
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
       return getStringForm(false);
    }
    
	public void setVariableContexts(List<VariableContext> variableContexts) {
		this.variableContexts = variableContexts;
	}

	public List<VariableContext> getVariableContexts() {
		return variableContexts;
	}
	
    @Override
	public int hashCode() {
    	return commands.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (!(obj instanceof BatchedUpdateCommand)) {
			return false;
		}
		BatchedUpdateCommand other = (BatchedUpdateCommand)obj;
		return EquivalenceUtil.areEqual(commands, other.commands) && this.singleResult == other.singleResult;
	}
	
	public void setSingleResult(boolean singleResult) {
		this.singleResult = singleResult;
	}
	
	public boolean isSingleResult() {
		return singleResult;
	}

	public String getStringForm(boolean full) {
		StringBuffer val = new StringBuffer(); 
		if (!full) {
			val.append("BatchedUpdate{"); //$NON-NLS-1$
		}
        if (commands != null && commands.size() > 0) {
            for (int i = 0; i < commands.size(); i++) {
            	if (i > 0) { 
            		if (full) {
            			val.append(";\n"); //$NON-NLS-1$
            		} else {
            			val.append(","); //$NON-NLS-1$
            		}
            	}
            	if (full) {
            		val.append(commands.get(i));
            	} else {
            		val.append(getCommandToken(commands.get(i).getType()));	
            	}
            }
        }
        if (!full) {
        	val.append('}');
        }
	    return val.toString();
	}
	
}
