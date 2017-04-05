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

package org.teiid.language;

import java.util.List;

import org.teiid.language.visitor.LanguageObjectVisitor;

/** 
 * Represents a batch of INSERT, UPDATE and DELETE commands to be executed together.
 */
public class BatchedUpdates extends BaseLanguageObject implements Command {

    private static final int MAX_COMMANDS_TOSTRING = 5;
    private List<Command> updateCommands;
    private boolean singleResult;
    
    public BatchedUpdates(List<Command> updateCommands) {
        this.updateCommands = updateCommands;
    }
    
    /**
     * @return a list of IInsert, IUpdate and IDelete commands in this batched update.
     */
    public List<Command> getUpdateCommands() {
        return updateCommands;
    }

    public void acceptVisitor(LanguageObjectVisitor visitor) {
        visitor.visit(this);
    }
    
    /**
     * Whether the batch represents a logical unit of work
     * It is not required that this be treated as atomic, but
     * the translator can use this as hint
     * @return
     */
    public boolean isSingleResult() {
		return singleResult;
	}
    
    public void setSingleResult(boolean atomic) {
		this.singleResult = atomic;
	}
    
    @Override
    public String toString() {
        return toString(false);
    }
        
    public String toString(boolean allCommands) {
    	StringBuffer result = new StringBuffer();
    	int toDisplay = updateCommands.size();
    	boolean overMax = false;
    	if (!allCommands && toDisplay > MAX_COMMANDS_TOSTRING) {
    	    toDisplay = MAX_COMMANDS_TOSTRING;
    	    overMax = true;
    	}
    	for (int i = 0; i < toDisplay; i++) {
    	    if (i > 0) {
    	        result.append("\n"); //$NON-NLS-1$
    	    }
    		result.append(updateCommands.get(i).toString());
    		result.append(";"); //$NON-NLS-1$
    	}
    	if (overMax) {
    	    result.append("\n..."); //$NON-NLS-1$
    	}
    	return result.toString();
    }

}
