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
import java.util.List;

import org.teiid.core.util.EquivalenceUtil;
import org.teiid.core.util.HashCodeUtil;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.ExpressionSymbol;
import org.teiid.query.sql.symbol.MultipleElementSymbol;
import org.teiid.query.sql.symbol.Symbol;
import org.teiid.query.sql.visitor.SQLStringVisitor;


/**
 * This class represents the SELECT clause of a query, which defines what elements
 * or expressions are returned from the query.
 */
public class Select implements LanguageObject {

    /** The set of symbols for the data elements to be selected. */
    private List<Expression> symbols = new ArrayList<Expression>();

    /** Flag for whether duplicate removal should be performed on the results */
    private boolean distinct;

    // =========================================================================
    //                         C O N S T R U C T O R S
    // =========================================================================

    /**
     * Constructs a default instance of this class.
     */
    public Select() {
        
    }

    /**
     * Constructs an instance of this class from an ordered set of symbols.
     * @param symbols The ordered list of symbols
     */
    public Select( List<? extends Expression> symbols ) {
        this.addSymbols(symbols);
    }

    // =========================================================================
    //                             M E T H O D S
    // =========================================================================

    /**
     * Returns the number of symbols in select.
     * @return Get count of number of symbols in select
     */
    public int getCount() {
        return symbols.size();
    }
    /**
	 * Checks for a Select * clause
	 * @return True if Select * is used
	 */
    public boolean isStar() {
		return (symbols.size() == 1 && symbols.get(0) instanceof MultipleElementSymbol && ((MultipleElementSymbol)symbols.get(0)).getGroup() == null);
    }

    /**
     * Returns an ordered list of the symbols in the select.
     * @param Get list of SelectSymbol in SELECT
     */
    public List<Expression> getSymbols() {
        return symbols;
    }
    
    /**
     * Sets an ordered list of the symbols in the select.  Reuses the current symbol list.
     * @param symbols list of SelectSymbol in SELECT
     */
    public void setSymbols(Collection<? extends Expression> symbols) {
    	this.symbols.clear();
    	this.addSymbols(symbols);
    }    

    /**
     * Returns the select symbol at the specified index.
     * @param index Index to get
     * @return The variable identifier at the index
     */
    public Expression getSymbol( int index ) {
        return symbols.get(index);
    }

    /**
     * @param symbol New symbol
     */
    public void addSymbol( Expression symbol ) {
    	if (!(symbol instanceof Symbol) && !(symbol instanceof MultipleElementSymbol)) {
    		symbol = new ExpressionSymbol("expr" + (this.symbols.size() + 1), symbol); //$NON-NLS-1$
    	}
    	this.symbols.add(symbol);
    }
    
    public void addSymbols( Collection<? extends Expression> toAdd) {
    	if (toAdd != null) {
    		for (Expression expression : toAdd) {
    			this.addSymbol(expression);
			}
    	}
    }
    
    /**
     * Remove all current symbols
     */
    public void clearSymbols() {
    	symbols.clear();
    }

    /**
     * Checks if a symbol is in the Select.
     * @param symbol Symbol to check for
     * @return True if the Select contains the symbol
     */
    public boolean containsSymbol( Expression symbol ) {
        return symbols.contains(symbol);
    }

	/**
	 * Set whether select is distinct.
	 * @param isDistinct True if SELECT is distinct
	 */
	public void setDistinct(boolean isDistinct) {
		this.distinct = isDistinct;
	}

	/**
	 * Checks whether the select is distinct
	 * @return True if select is distinct
	 */
	public boolean isDistinct() {
		return this.distinct;
	}

    public void acceptVisitor(LanguageVisitor visitor) {
        visitor.visit(this);
    }
		
	/**
	 * Get the ordered list of all elements returned by this select.  These elements
	 * may be ElementSymbols or ExpressionSymbols but in all cases each represents a 
	 * single column.
	 * @return Ordered list of SingleElementSymbol
	 */
	public List<Expression> getProjectedSymbols() { 
		ArrayList<Expression> projectedSymbols = new ArrayList<Expression>();
		for (Expression symbol : symbols) {
			if(symbol instanceof MultipleElementSymbol) { 
			    List<ElementSymbol> multiSymbols = ((MultipleElementSymbol)symbol).getElementSymbols();
			    if(multiSymbols != null) { 
			        projectedSymbols.addAll(multiSymbols);
			    }
			} else {
				projectedSymbols.add(symbol);
			}
		}		
		return projectedSymbols;
	}
	
    // =========================================================================
    //          O V E R R I D D E N     O B J E C T     M E T H O D S
    // =========================================================================

	/**
	 * Return a deep copy of this Select.
	 * @return Deep clone
	 */
	public Select clone() {
		Select copy = new Select(LanguageObject.Util.deepClone(this.symbols, Expression.class));
		copy.setDistinct( isDistinct() );
		return copy;
	}

	/**
	 * Compare two Selects for equality.  Order is important in the select (for
	 * determining the order of the returned columns), so this is a compare
	 * with order, not just a set comparison.
	 * @param obj Other object
	 * @return True if equal
	 */
	public boolean equals(Object obj) {
		if(obj == this) {
			return true;
		}

		if(!(obj instanceof Select)) {
			return false;
		}

		Select other = (Select) obj;
        
        return other.isDistinct() == isDistinct() &&
               EquivalenceUtil.areEqual(getSymbols(), other.getSymbols());
	}

	/**
	 * Get hashcode for Select.  WARNING: The hash code relies on the variables
	 * in the select, so changing the variables will change the hash code, causing
	 * a select to be lost in a hash structure.  Do not hash a Select if you plan
	 * to change it.
	 * @return Hash code
	 */
	public int hashCode() {
		return HashCodeUtil.hashCode(0, getSymbols());
	}

    /**
     * Returns a string representation of an instance of this class.
     * @return String representation of object
     */
    public String toString() {
    	return SQLStringVisitor.getSQLString(this);
    }
        
}  // END CLASS
