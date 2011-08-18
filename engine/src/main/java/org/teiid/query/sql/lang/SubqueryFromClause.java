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

import java.util.Collection;

import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.symbol.GroupSymbol;


/**
 * A FROM subpart that represents a subquery.  For example, the FROM clause: 
 * "FROM (SELECT a FROM b)" will have a SubqueryFromClause referencing the subquery.
 */
public class SubqueryFromClause extends FromClause implements SubqueryContainer{

    private GroupSymbol symbol;
    private Command command;
    private boolean table;
	
	/**
	 * Construct default object
	 */
	public SubqueryFromClause(String name) {
        setName(name);
	}
	
    /**
     * Construct object with specified command and name
     * @param command Command representing subquery, or stored procedure
     * @param name Alias of the subquery
     */
    public SubqueryFromClause(String name, Command command) {
        this(name);
        this.command = command;
    }
    
    public SubqueryFromClause(GroupSymbol symbol, Command command) {
        this.symbol = symbol;
        this.command = command;
    }
    
    public boolean isTable() {
		return table;
	}
    
    public void setTable(boolean table) {
		this.table = table;
	}

    /** 
     * Reset the alias for this subquery from clause and it's pseudo-GroupSymbol.  
     * WARNING: this will modify the hashCode and equals semantics and will cause this object
     * to be lost if currently in a HashMap or HashSet.
     * @param name New name
     * @since 4.3
     */
    public void setName(String name) {
        this.symbol = new GroupSymbol(name);
    }
		
	/**
	 * Set the command held by the clause
	 * @param command Command to hold
	 */
	public void setCommand(Command command) {
		this.command = command;
	} 
	
	/**
	 * Get command held by clause
	 * @return Command held by clause
	 */
	public Command getCommand() {
		return this.command;
	}	
    
    /**
     * Get name of this clause.
     * @return Name of clause
     */
    public String getName() {
        return this.symbol.getName();   
    }
    
    public String getOutputName() {
        return this.symbol.getOutputName();
    }

    /**
     * Get GroupSymbol representing the named subquery 
     * @return GroupSymbol representing the subquery
     */
    public GroupSymbol getGroupSymbol() {
        return this.symbol;    
    }

    /**
     * Collect all GroupSymbols for this from clause.
     * @param groups Groups to add to
     */
    public void collectGroups(Collection groups) {
        groups.add(getGroupSymbol());
    }
    
    public void acceptVisitor(LanguageVisitor visitor) {
        visitor.visit(this);
    }

	/**
	 * Check whether objects are equal
	 * @param obj Other object
	 * @return True if equal
	 */
	public boolean equals(Object obj) {
        if (!super.equals(obj)) {
            return false;
        }
		
		if(! (obj instanceof SubqueryFromClause)) { 
			return false;
		}		
		SubqueryFromClause sfc = (SubqueryFromClause) obj;
		
        return this.getName().equalsIgnoreCase(sfc.getName()) &&
            sfc.isOptional() == this.isOptional() && this.command.equals(sfc.command)
            && this.table == sfc.table;
	}
	
	/**
	 * Get hash code of object
	 * @return Hash code
	 */
	public int hashCode() {
		return this.symbol.hashCode();
	}
	
	/**
	 * Get deep clone of object
	 * @return Deep copy of the object
	 */
	public FromClause cloneDirect() {
        Command commandCopy = null;        
        if(this.command != null) {
            commandCopy = (Command) this.command.clone();
        }
        
        SubqueryFromClause clause = new SubqueryFromClause(this.symbol.clone(), commandCopy);
        clause.setTable(this.isTable());
        return clause;
	}

}
