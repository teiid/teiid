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
import org.teiid.core.util.HashCodeUtil;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.visitor.SQLStringVisitor;


/**
 * This class represents the GROUP BY clause of a query, which defines the expressions
 * that should be used for grouping the results of the query.  The groups
 * produced in the query are grouped on all symbols listed contained in the
 * GROUP BY clause.  The GROUP BY clause may not contain aliased elements,
 * expressions, or constants.
 */
public class GroupBy implements LanguageObject {

    /** The set of expressions for the data elements to be group. */
    private List<Expression> symbols; 

    // =========================================================================
    //                         C O N S T R U C T O R S
    // =========================================================================

    /**
     * Constructs a default instance of this class.
     */
    public GroupBy() {
        symbols = new ArrayList<Expression>();
    }

    /**
     * Constructs an instance of this class from an ordered set of symbols.
     * @param symbols The ordered list of {@link org.teiid.query.sql.symbol.ElementSymbol}s
     */
    public GroupBy( List<? extends Expression> symbols ) {
        this.symbols = new ArrayList<Expression>( symbols );
    }

    // =========================================================================
    //                             M E T H O D S
    // =========================================================================

    /**
     * Returns the number of symbols in the GROUP BY
     * @return Count of the number of symbols in GROUP BY
     */
    public int getCount() {
        return symbols.size();
    }

    /**
     * Returns an ordered list of the symbols in the GROUP BY
     * @return List of {@link org.teiid.query.sql.symbol.ElementSymbol}s
     */
    public List<Expression> getSymbols() {
        return symbols;
    }

    /**
     * Adds a new symbol to the list of symbols.
     * @param symbol Symbol to add to GROUP BY
     */
    public void addSymbol( Expression symbol ) {
    	if(symbol != null) {
	        symbols.add(symbol);
        }
    }

    public void acceptVisitor(LanguageVisitor visitor) {
        visitor.visit(this);
    }

    // =========================================================================
    //          O V E R R I D D E N     O B J E C T     M E T H O D S
    // =========================================================================

	/**
	 * Return a deep copy of this object
	 * @return Deep copy of object
	 */
	public Object clone() {
		return new GroupBy(LanguageObject.Util.deepClone(this.symbols, Expression.class));
	}

	/**
	 * Compare two GroupBys for equality.  Order is important in the comparison.
	 * @param obj Other object
	 * @return True if equal
	 */
	public boolean equals(Object obj) {
		if(obj == this) {
			return true;
		}

		if(!(obj instanceof GroupBy)) {
			return false;
		}

        return EquivalenceUtil.areEqual(getSymbols(), ((GroupBy)obj).getSymbols());
	}

	/**
	 * Get hashcode for GroupBy.  WARNING: The hash code relies on the variables
	 * in the group by, so changing the variables will change the hash code, causing
	 * a group by to be lost in a hash structure.  Do not hash a GroupBy if you plan
	 * to change it.
	 * @return Hash code for object
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
