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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.processor.ProcessorPlan;
import com.metamatrix.query.sql.LanguageObject;
import com.metamatrix.query.sql.symbol.ElementSymbol;
import com.metamatrix.query.sql.symbol.GroupSymbol;
import com.metamatrix.query.sql.symbol.SingleElementSymbol;
import com.metamatrix.query.sql.util.SymbolMap;
import com.metamatrix.query.sql.visitor.CommandCollectorVisitor;
import com.metamatrix.query.sql.visitor.SQLStringVisitor;

/**
 * A Command is an interface for all the language objects that are at the root
 * of a language object tree representing a SQL statement.  For instance, a 
 * Query command represents a SQL select query, an Update command represents a 
 * SQL update statement, etc.
 */
public abstract class Command implements LanguageObject {

	/** 
	 * Represents an unknown type of command 
	 */
	public static final int TYPE_UNKNOWN = 0;
	
	/**
	 * Represents a SQL SELECT statement
	 */
	public static final int TYPE_QUERY = 1;
	
	/**
	 * Represents a SQL INSERT statement
	 */
	public static final int TYPE_INSERT = 2;

	/**
	 * Represents a SQL UPDATE statement
	 */
	public static final int TYPE_UPDATE = 3;

	/**
	 * Represents a SQL DELETE statement
	 */
	public static final int TYPE_DELETE = 4;

	/**
	 * Represents any SQL statement, wrapped as a string
	 */
	public static final int TYPE_SQL = 5;

	/**
	 * Represents a stored procedure command
	 */
    public static final int TYPE_STORED_PROCEDURE = 6;
    
	/**
	 * Represents a update stored procedure command
	 */
    public static final int TYPE_UPDATE_PROCEDURE = 7;

    /**
     * Represents an XQuery
     */
    public static final int TYPE_XQUERY = 8;

    /**
     * Represents a batched sequence of UPDATE statements
     */
    public static final int TYPE_BATCHED_UPDATE = 9;
    
    public static final int TYPE_DYNAMIC = 10;
    
    public static final int TYPE_CREATE = 11;
    
    public static final int TYPE_DROP = 12;

    private static List updateCommandSymbol;
    
    /**
     * All temporary group IDs discovered while resolving this 
     * command.  The key is a TempMetadataID and the value is an 
     * ordered List of TempMetadataID representing the elements.
     */
    protected Map tempGroupIDs;
    
    private transient GroupContext externalGroups;

    private boolean isResolved;
    
    private GroupSymbol virtualGroup;
    
	/** The option clause */
	private Option option;
	
	private ProcessorPlan plan;
	
	private SymbolMap correlatedReferences;
    
	/**
	 * Return type of command to make it easier to build switch statements by command type.
	 * @return Type from TYPE constants
	 */	
	public abstract int getType();
	
	/**
	 * Get the correlated references to the containing scope only
	 * @return
	 */
	public SymbolMap getCorrelatedReferences() {
		return correlatedReferences;
	}
	
	public void setCorrelatedReferences(SymbolMap correlatedReferences) {
		this.correlatedReferences = correlatedReferences;
	}

    /**
     * Gets the subCommands (both embedded and non-embedded) under this command.  In general the returned list
     * is not safe to manipulate (see @link#CommandContainer insead) 
     * @return
     */
    public List getSubCommands() {
        return CommandCollectorVisitor.getCommands(this);
    }
    
    public void setTemporaryMetadata(Map metadata) {
        this.tempGroupIDs = metadata;
    }
    
    public Map getTemporaryMetadata() {
        return this.tempGroupIDs;
    }
    
    public void addExternalGroupToContext(GroupSymbol group) {
        getExternalGroupContexts().addGroup(group);
    }
    
    public void addExternalGroupsToContext(Collection groups) {
        getExternalGroupContexts().getGroups().addAll(groups);
    }

    public void setExternalGroupContexts(GroupContext root) {
        if (root == null) {
            this.externalGroups = null;
        } else {
            this.externalGroups = (GroupContext)root.clone();
        }
    }
    
    public void pushNewResolvingContext(Collection groups) {
        externalGroups = new GroupContext(externalGroups, new LinkedList(groups));
    }

    public GroupContext getExternalGroupContexts() {
        if (externalGroups == null) {
            this.externalGroups = new GroupContext();
        }
        return this.externalGroups;
    }
    
    public List getAllExternalGroups() {
        if (externalGroups == null) {
            return Collections.EMPTY_LIST;
        }
        
        return externalGroups.getAllGroups();
    }

    /**
     * Indicates whether this command has been resolved or not - 
     * attempting to resolve a command that has already been resolved
     * has undefined results.  Also, caution should be taken in modifying
     * a command which has already been resolved, as it could result in
     * adding unresolved components to a supposedly resolved command.
     * @return whether this command is resolved or not.
     */
    public boolean isResolved() {
        return this.isResolved;
    }

    /**
     * This command is intended to only be used by the QueryResolver.
     * @param isResolved whether this command is resolved or not
     */
    public void setIsResolved(boolean isResolved) {
        this.isResolved = isResolved;
    }
        
    public abstract Object clone();
    
    protected void copyMetadataState(Command copy) {
        if(this.getExternalGroupContexts() != null) {
            copy.externalGroups = (GroupContext)this.externalGroups.clone();
        }
        if(this.tempGroupIDs != null) {
            copy.setTemporaryMetadata(new HashMap(this.tempGroupIDs));
        }
        copy.setVirtualGroup(getVirtualGroup());
        
        copy.setIsResolved(this.isResolved());
        copy.plan = this.plan;
        if (this.correlatedReferences != null) {
        	copy.correlatedReferences = this.correlatedReferences.clone();
        }
    }
    
    /**
     * Print the full tree of commands with indentation - useful for debugging
     * @return String String representation of command tree
     */
    public String printCommandTree() {
        StringBuffer str = new StringBuffer();
        printCommandTree(str, 0);
        return str.toString();
    }
    
    /**
     * Helper method to print command tree at given tab level
     * @param str String buffer to add command sub tree to
     * @param tabLevel Number of tabs to print this command at
     */
    protected void printCommandTree(StringBuffer str, int tabLevel) {
        // Add tabs
        for(int i=0; i<tabLevel; i++) {
            str.append("\t"); //$NON-NLS-1$
        }
        
        // Add this command
        str.append(toString());
        str.append("\n"); //$NON-NLS-1$
        
        // Add children recursively
        tabLevel++;
        Iterator iter = getSubCommands().iterator();
        while(iter.hasNext()) {
            Command subCommand = (Command) iter.next();
            subCommand.printCommandTree(str, tabLevel);
        }
    }
    
    // =========================================================================
    //                     O P T I O N      M E T H O D S
    // =========================================================================
    
    /**
     * Get the option clause for the query.
     * @return option clause
     */
    public Option getOption() {
        return option;
    }
    
    /**
     * Set the option clause for the query.
     * @param option New option clause
     */
    public void setOption(Option option) {
        this.option = option;
    }

	/**
	 * Get the ordered list of all elements returned by this query.  These elements
	 * may be ElementSymbols or ExpressionSymbols but in all cases each represents a 
	 * single column.
	 * @return Ordered list of SingleElementSymbol
	 */
	public abstract List<SingleElementSymbol> getProjectedSymbols();

	/**
	 * Whether the results are cachable.
	 * @return True if the results are cachable; false otherwise.
	 */
	public abstract boolean areResultsCachable();
    
    public static List getUpdateCommandSymbol() {
        if (updateCommandSymbol == null ) {
            ElementSymbol symbol = new ElementSymbol("Count"); //$NON-NLS-1$
            symbol.setType(DataTypeManager.DefaultDataClasses.INTEGER);
            updateCommandSymbol = Arrays.asList(new ElementSymbol[] {symbol});
        }
        return updateCommandSymbol;
    }
    
    /**
     * Return the number of updates on physical sources by this command. 
     * @param metadata QueryMetadataInterface
     * @return The number of updates on physical sources by this command. 
     * @since 4.3
     */
    public abstract int updatingModelCount(QueryMetadataInterface metadata)  throws MetaMatrixComponentException;
    
    
    protected int getSubCommandsUpdatingModelCount(QueryMetadataInterface metadata) throws MetaMatrixComponentException{
        return getSubCommandsUpdatingModelCount(this, metadata);
    }

    public static int getSubCommandsUpdatingModelCount(Command object, QueryMetadataInterface metadata) throws MetaMatrixComponentException {
        List<Command> subCommands = CommandCollectorVisitor.getCommands(object);
        int numCommands = subCommands.size();
        for (Command command : subCommands) {
            if (numCommands == 1) {
                return command.updatingModelCount(metadata);
            }
            if (command.updatingModelCount(metadata) > 0) {
                return 2; //require a transaction if there is more than 1 statement
            }
        }
        return 0;
    }

    public GroupSymbol getVirtualGroup() {
        return this.virtualGroup;
    }

    public void setVirtualGroup(GroupSymbol virtualGroup) {
        this.virtualGroup = virtualGroup;
    }
    
    public ProcessorPlan getProcessorPlan() {
    	return this.plan;
    }
    
    public void setProcessorPlan(ProcessorPlan plan) {
    	this.plan = plan;
    }
    
    /**
     * Returns a string representation of an instance of this class.
     * @return String representation of object
     */
    public String toString() {
        return SQLStringVisitor.getSQLString(this);
    }
}
