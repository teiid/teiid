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
import java.util.List;
import java.util.Map;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.QueryMetadataException;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.sql.proc.CreateUpdateProcedureCommand;
import com.metamatrix.query.sql.symbol.GroupSymbol;

public abstract class ProcedureContainer extends Command implements CommandContainer {

    /**
     * This is a little hack to maintain a list of at most 1
     */
    private List subCommands = new ArrayList(1);
    
    private int updateCount = -1;
    
    public abstract GroupSymbol getGroup();
    
    /** 
     * @return Returns the subCommand.
     */
    public Command getSubCommand() {
        if (subCommands.isEmpty()) {
            return null;
        }
        return (Command)subCommands.get(0);
    }

    /** 
     * @param subCommand The subCommand to set.
     */
    public void setSubCommand(Command subCommand) {
        if (subCommands.isEmpty()) {
            subCommands.add(null);
        }
        if (subCommand == null) {
            subCommands.remove(0);
        } else {
            this.subCommands.set(0, subCommand);
        }
    }
    
    protected void copyMetadataState(Command copy) {
        super.copyMetadataState(copy);
        
        Command subCommand = getSubCommand();
        if (subCommand != null) {
        	subCommand = (Command)subCommand.clone();
	        ((ProcedureContainer)copy).subCommands.add(subCommand);
	        if (subCommand instanceof CreateUpdateProcedureCommand) {
	            ((CreateUpdateProcedureCommand)subCommand).setUserCommand(copy);
	        } 
        }
    }
    
    /** 
     * @see com.metamatrix.query.sql.lang.CommandContainer#getContainedCommands()
     */
    public List getContainedCommands() {
        return subCommands;
    }
    
    public int updatingModelCount(QueryMetadataInterface metadata) throws MetaMatrixComponentException{
        if (updateCount != -1) {
            return updateCount;
        }
        
        if(this.getGroup().isTempGroupSymbol()) {
            //TODO: this is not correct.  once temp tables are transactional, this will need to return a better value
            return 0;
        }
        
        try {
            if (!metadata.isVirtualGroup(this.getGroup().getMetadataID())) {
                return 1; //physical stored procedures are assumed to perform an update
            } 
        } catch (QueryMetadataException e) {
            throw new MetaMatrixComponentException(e);
        }
        
        return this.getSubCommand().updatingModelCount(metadata);
    }

    
    /** 
     * @return Returns the updateCount.
     */
    public int getUpdateCount() {
        return this.updateCount;
    }

    
    /** 
     * @param updateCount The updateCount to set.
     */
    public void setUpdateCount(int updateCount) {
        if (updateCount < 0) {
            return;
        }
        if (updateCount > 2) {
            updateCount = 2;
        }
        this.updateCount = updateCount;
    }
    
    public abstract Map getProcedureParameters();
}
