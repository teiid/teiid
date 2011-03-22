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
import java.util.List;

import org.teiid.core.util.EquivalenceUtil;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.ProcedureReservedWords;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.visitor.SQLStringVisitor;


/**
 * <p> This class represents a group of <code>Statement</code> objects. The
 * statements are stored on this object in the order in which they are added.</p>
 */
public class Block implements LanguageObject {

	// list of statements on this block
	private List<Statement> statements;

	/**
	 * Constructor for Block.
	 */
	public Block() {
		statements = new ArrayList<Statement>();
	}

	/**
	 * Constructor for Block with a single <code>Statement</code>.
	 * @param statement The <code>Statement</code> to be added to the block
	 */
	public Block(Statement statement) {
		this();
		statements.add(statement);
	}

	/**
	 * Get all the statements contained on this block.
	 * @return A list of <code>Statement</code>s contained in this block
	 */
	public List<Statement> getStatements() {
		return statements;
	}

	/**
	 * Set the statements contained on this block.
	 * @param statements A list of <code>Statement</code>s contained in this block
	 */
	public void setStatements(List<Statement> statements) {
		this.statements = statements;
	}

	/**
	 * Add a <code>Statement</code> to this block.
	 * @param statement The <code>Statement</code> to be added to the block
	 */
	public void addStatement(Statement statement) {
		if (statement instanceof AssignmentStatement) {
			AssignmentStatement stmt = (AssignmentStatement)statement;
			Command cmd = stmt.getCommand();
			if (cmd != null) {
				statements.add(new CommandStatement(cmd));
				stmt.setCommand(null);
				stmt.setExpression(null);
				String fullName = ProcedureReservedWords.VARIABLES+ElementSymbol.SEPARATOR+ProcedureReservedWords.ROWCOUNT;
				if (stmt.getVariable().getShortName().equalsIgnoreCase(ProcedureReservedWords.ROWCOUNT) 
						&& stmt.getVariable().getCanonicalName().equals(fullName)) {
					return;
				}
				stmt.setExpression(new ElementSymbol(fullName));
			}
		}
		statements.add(statement);
	}
	
    // =========================================================================
    //                  P R O C E S S I N G     M E T H O D S
    // =========================================================================
        
    public void acceptVisitor(LanguageVisitor visitor) {
        visitor.visit(this);
    }
	
	/**
	 * Deep clone statement to produce a new identical block.
	 * @return Deep clone 
	 */
	public Block clone() {		
		Block copy = new Block();
		for (Statement statement : statements) {
			copy.addStatement((Statement)statement.clone());
		}
		return copy;
	}
	
    /**
     * Compare two queries for equality.  Blocks will only evaluate to equal if
     * they are IDENTICAL: statements in the block are equal and are in the same order.
     * @param obj Other object
     * @return True if equal
     */
    public boolean equals(Object obj) {
    	// Quick same object test
    	if(this == obj) {
    		return true;
		}

		// Quick fail tests		
    	if(!(obj instanceof Block)) {
    		return false;
		}

		// Compare the statements on the block
        return EquivalenceUtil.areEqual(getStatements(), ((Block)obj).getStatements());
    }    

    /**
     * Get hashcode for block.  WARNING: This hash code relies on the hash codes of the
     * statements present in the block.  If statements are added to the block or if
     * statements on the block change the hash code will change. Hash code is only valid
     * after the block has been completely constructed.
     * @return Hash code
     */
    public int hashCode() {
    	return statements.hashCode();
	}
      
    /**
     * Returns a string representation of an instance of this class.
     * @return String representation of object
     */
    public String toString() {
    	return SQLStringVisitor.getSQLString(this);
    }

}// END CLASS
