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

package com.metamatrix.query.sql.proc;

import com.metamatrix.query.sql.*;
import com.metamatrix.query.sql.lang.Command;
import com.metamatrix.query.sql.lang.SubqueryContainer;
import com.metamatrix.core.util.EquivalenceUtil;
import com.metamatrix.query.sql.visitor.SQLStringVisitor;

/**
 * <p> This class represents a variable assignment statement in the storedprocedure language.
 * It extends the <code>Statement</code> that could part of a block.  This statement has
 * a command that should be executed as part of the procedure.</p>
 */
public class CommandStatement extends Statement implements SubqueryContainer {

	// the command this statement represents
	Command command;

	/**
	 * Constructor for CommandStatement.
	 */
	public CommandStatement() {
		super();
	}

	/**
	 * Constructor for CommandStatement.
	 * @param value The <code>Command</code> on this statement
	 */
	public CommandStatement(Command value) {
		this.command = value;
	}

	/**
	 * Get the command on this statement.
	 * @return The <code>Command</code> on this statement
	 */
	public Command getCommand() {
		return command;	
	}

    /**
     * Sets the command. 
     * @see com.metamatrix.query.sql.lang.SubqueryLanguageObject#setCommand()
     */
    public void setCommand(Command command){
        this.command = command;
    }
	
	/**
	 * Return the type for this statement, this is one of the types
	 * defined on the statement object.
	 * @return The type of this statement
	 */
	public int getType() {
		return Statement.TYPE_COMMAND;
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
		return new CommandStatement((Command)this.command.clone());
	}
	
    /**
     * Compare two CommandStatements for equality.  They will only evaluate to equal if
     * they are IDENTICAL: the command objects are equal.
     * @param obj Other object
     * @return True if equal
     */
    public boolean equals(Object obj) {
    	// Quick same object test
    	if(this == obj) {
    		return true;
		}

		// Quick fail tests		
    	if(!(obj instanceof CommandStatement)) {
    		return false;
		}

        return EquivalenceUtil.areEqual(getCommand(), ((CommandStatement)obj).getCommand());
    } 

    /**
     * Get hashcode for CommandStatement.  WARNING: This hash code relies on the
     * hash code of the command on this statement.
     * @return Hash code
     */
    public int hashCode() {
    	// This hash code relies on the commands hash code
    	return this.getCommand().hashCode();
	}
      
    /**
     * Returns a string representation of an instance of this class.
     * @return String representation of object
     */
    public String toString() {
    	return SQLStringVisitor.getSQLString(this);
    }	

} // END CLASS
