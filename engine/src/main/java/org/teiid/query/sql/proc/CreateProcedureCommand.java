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

import java.util.Collections;
import java.util.List;

import org.teiid.core.util.EquivalenceUtil;
import org.teiid.core.util.HashCodeUtil;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.visitor.SQLStringVisitor;


/**
 * <p> This class represents a update procedure in the storedprocedure language.
 * It extends the <code>Command</code> and represents the command for Insert , Update
 * and Delete procedures.</p>
 */
public class CreateProcedureCommand extends Command {
	
	// top level block for the procedure
	private Block block;
	
    private List projectedSymbols;
    private List<? extends Expression> resultSetColumns;
    
    private GroupSymbol virtualGroup;

	private int updateType = Command.TYPE_UNKNOWN;

	private ElementSymbol returnVariable;

	/**
	 * Constructor for CreateUpdateProcedureCommand.
	 */
	public CreateProcedureCommand() {
		super();
	}

	/**
	 * Constructor for CreateUpdateProcedureCommand.
	 * @param block The block on this command
	 * @param type The procedure type 
	 */
	public CreateProcedureCommand(Block block) {
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
		CreateProcedureCommand copy = new CreateProcedureCommand();

        //Clone this class state
        if (this.block != null) {
            copy.setBlock(this.block.clone());
        }
        if (this.projectedSymbols != null) {
            copy.projectedSymbols = LanguageObject.Util.deepClone(this.projectedSymbols, Expression.class);
        }
        if (this.resultSetColumns != null) {
            copy.resultSetColumns = LanguageObject.Util.deepClone(this.resultSetColumns, Expression.class);
        }
        if (this.virtualGroup != null) {
        	copy.virtualGroup = this.virtualGroup.clone();
        }
        if (this.returnVariable != null) {
        	copy.returnVariable = this.returnVariable;
        }
        copy.updateType = this.updateType; 
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
    	if(! (obj instanceof CreateProcedureCommand)) {
    		return false;
		}
    	
    	CreateProcedureCommand other = (CreateProcedureCommand)obj;
        
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
        //user may have not entered any query yet
        return Collections.EMPTY_LIST;
	}  
	
	public List<? extends Expression> getResultSetColumns() {
		return resultSetColumns;
	}
	
	public void setResultSetColumns(List<? extends Expression> resultSetColumns) {
		this.resultSetColumns = resultSetColumns;
	}
	
    /**
     * @param projSymbols
     */
    public void setProjectedSymbols(List projSymbols) {
        projectedSymbols = projSymbols;
    }

	/**
	 * @see org.teiid.query.sql.lang.Command#areResultsCachable()
	 */
	public boolean areResultsCachable() {
		return Query.areColumnsCachable(getProjectedSymbols());
	}
    
    public GroupSymbol getVirtualGroup() {
        return this.virtualGroup;
    }

    public void setVirtualGroup(GroupSymbol virtualGroup) {
        this.virtualGroup = virtualGroup;
    }

	public void setUpdateType(int type) {
		//we select the count as the last operation
		this.resultSetColumns = Command.getUpdateCommandSymbol();
		this.updateType = type;
	}
	
	public int getUpdateType() {
		return updateType;
	}

	public void setReturnVariable(ElementSymbol symbol) {
		this.returnVariable = symbol;
	}
	
	public ElementSymbol getReturnVariable() {
		return returnVariable;
	}
	
	@Override
	public boolean returnsResultSet() {
		return this.resultSetColumns != null && !this.resultSetColumns.isEmpty();
	}

} // END CLASS
