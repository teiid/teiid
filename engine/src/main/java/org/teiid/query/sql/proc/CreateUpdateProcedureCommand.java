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

package org.teiid.query.sql.proc;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.teiid.core.util.EquivalenceUtil;
import org.teiid.core.util.HashCodeUtil;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.lang.StoredProcedure;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.symbol.SingleElementSymbol;
import org.teiid.query.sql.visitor.SQLStringVisitor;


/**
 * <p> This class represents a update procedure in the storedprocedure language.
 * It extends the <code>Command</code> and represents the command for Insert , Update
 * and Delete procedures.</p>
 */
public class CreateUpdateProcedureCommand extends Command {
	
	// top level block for the procedure
	private Block block;
	
	// map between elements on the virtual groups and the elements in the
	// transformation query that define it.
	private Map symbolMap;

	// the command the user submitted against the virtual group being updated
	private Command userCommand;
	
    //whether it is update procedure or virtual stored procedure, default to update procedure
    private boolean isUpdateProcedure = true;
    
    private List projectedSymbols;
    
    private GroupSymbol virtualGroup;

    //command that returns resultset. For virtual procedure only.
    private Command resultsCommand;
	/**
	 * Constructor for CreateUpdateProcedureCommand.
	 */
	public CreateUpdateProcedureCommand() {
		super();
	}

	/**
	 * Constructor for CreateUpdateProcedureCommand.
	 * @param block The block on this command
	 * @param type The procedure type 
	 */
	public CreateUpdateProcedureCommand(Block block) {
		this.block = block;
	}	

	/**
	 * Return type of command to make it easier to build switch statements by command type.
	 * @return The type of this command
	 */
	public int getType() {
		return Command.TYPE_UPDATE_PROCEDURE;	
	}

	/**
	 * Get the block on this command.
	 * @return The <code>Block</code> on this command
	 */
	public Block getBlock() {
		return block;
	}

	/**
	 * Set the block on this command.
	 * @param block The <code>Block</code> on this command
	 */
	public void setBlock(Block block) {
		this.block = block;
	}

	/**
	 * Set the user's command to which this obj which is the subcommand
	 * @param command The user's command
	 */
	public void setUserCommand(Command command) {
		this.userCommand = command;
	}

	/**
	 * Get the user's command to which this obj which is the subcommand
	 * @return The user's command
	 */	
	public Command getUserCommand() {
		return this.userCommand;	
	}	

	/**
	 * Set the symbol map between elements on the virtual group being updated and the
	 * elements on the transformation query.
	 * @param symbolMap Map of virtual group elements -> elements that define those
	 */
	public void setSymbolMap(Map symbolMap) {
		this.symbolMap = symbolMap;
	}

	/**
	 * Get the symbol map between elements on the virtual group being updated and the
	 * elements on the transformation query.
	 * @return Map of virtual group elements -> elements that define those
	 */
	public Map getSymbolMap() {
		return this.symbolMap;
	}

    // =========================================================================
    //                  P R O C E S S I N G     M E T H O D S
    // =========================================================================

    public void acceptVisitor(LanguageVisitor visitor) {
        visitor.visit(this);
    }

	/**
	 * Deep clone statement to produce a new identical statement.
	 * @return Deep clone 
	 */
	public Object clone() {		
		CreateUpdateProcedureCommand copy = new CreateUpdateProcedureCommand();

        //Clone this class state
        if (this.block != null) {
            copy.setBlock(this.block.clone());
        }
        if (this.getSymbolMap() != null) {
            copy.setSymbolMap(new HashMap(this.getSymbolMap()));
        }
        copy.setUpdateProcedure(isUpdateProcedure());
        if (this.projectedSymbols != null) {
            copy.setProjectedSymbols(new ArrayList(this.projectedSymbols));
        }
        if (this.virtualGroup != null) {
        	copy.virtualGroup = this.virtualGroup.clone();
        }
        this.copyMetadataState(copy);
		return copy;
	}

    /**
     * Compare two CreateUpdateProcedureCommand for equality.  They will only evaluate to equal if
     * they are IDENTICAL: the commandTypes are same and the block objects are equal.
     * @param obj Other object
     * @return True if equal
     */
    public boolean equals(Object obj) {
    	// Quick same object test
    	if(this == obj) {
    		return true;
		}

		// Quick fail tests		
    	if(! (obj instanceof CreateUpdateProcedureCommand)) {
    		return false;
		}
    	
    	CreateUpdateProcedureCommand other = (CreateUpdateProcedureCommand)obj;
        
        // Compare the block
        return sameOptionAndHint(other) && EquivalenceUtil.areEqual(getBlock(), other.getBlock());
    } 

    /**
     * Get hashcode for CreateUpdateProcedureCommand.  WARNING: This hash code relies
     * on the hash codes of the block and the procedure type of this command. Hash code
     * is only valid after the command has been completely constructed.
     * @return Hash code
     */
    public int hashCode() {
    	// This hash code relies on the block and the procedure type for this command
    	int myHash = 0;
    	myHash = HashCodeUtil.hashCode(myHash, this.getBlock());
		return myHash;
	}

    /**
     * Returns a string representation of an instance of this class.
     * @return String representation of object
     */
    public String toString() {
    	return SQLStringVisitor.getSQLString(this);
    }

    // ========================================
    // 			Methods inherited from Command
    // ========================================

	/**
	 * Get the ordered list of all elements returned by this query.  These elements
	 * may be ElementSymbols or ExpressionSymbols but in all cases each represents a 
	 * single column.
	 * @return Ordered list of SingleElementSymbol
	 */
	public List getProjectedSymbols(){
        if(this.projectedSymbols != null){
            return this.projectedSymbols;
        }
        if(!isUpdateProcedure){
            if(this.resultsCommand == null){
                //user may have not entered any query yet
                return Collections.EMPTY_LIST;
            }
            List<? extends SingleElementSymbol> symbols = this.resultsCommand.getProjectedSymbols();
            if (this.resultsCommand instanceof StoredProcedure) {
            	StoredProcedure sp = (StoredProcedure)this.resultsCommand;
            	if (sp.isCallableStatement()) {
            		symbols = sp.getResultSetColumns();
            	}
            }
            setProjectedSymbols(symbols);
            return this.projectedSymbols;
        }
        this.projectedSymbols = Command.getUpdateCommandSymbol();
    	return this.projectedSymbols;        
	}  

    /**
     * @return
     */
    public boolean isUpdateProcedure() {
        return isUpdateProcedure;
    }

    /**
     * @param isUpdateProcedure
     */
    public void setUpdateProcedure(boolean isUpdateProcedure) {
        this.isUpdateProcedure = isUpdateProcedure;
    }

    /**
     * @param projSymbols
     */
    public void setProjectedSymbols(List projSymbols) {
        projectedSymbols = projSymbols;
    }

    /**
     * @return Command
     */
    public Command getResultsCommand() {
        return resultsCommand;
    }

    /**
     * @param command
     */
    public void setResultsCommand(Command command) {
        resultsCommand = command;
    }
	
	/**
	 * @see org.teiid.query.sql.lang.Command#areResultsCachable()
	 */
	public boolean areResultsCachable() {
		if(isUpdateProcedure()){
			return false;
		}
		return Query.areResultsCachable(getProjectedSymbols());
	}
    
    public GroupSymbol getVirtualGroup() {
        return this.virtualGroup;
    }

    public void setVirtualGroup(GroupSymbol virtualGroup) {
        this.virtualGroup = virtualGroup;
    }

} // END CLASS
