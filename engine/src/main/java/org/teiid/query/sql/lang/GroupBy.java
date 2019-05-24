/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
    private boolean rollup;

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
        GroupBy clone = new GroupBy(LanguageObject.Util.deepClone(this.symbols, Expression.class));
        clone.rollup = this.rollup;
        return clone;
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

        GroupBy other = (GroupBy)obj;
        return EquivalenceUtil.areEqual(getSymbols(), other.getSymbols())
                && this.rollup == other.rollup;
    }

    /**
     * Get hashcode for GroupBy.  WARNING: The hash code relies on the variables
     * in the group by, so changing the variables will change the hash code, causing
     * a group by to be lost in a hash structure.  Do not hash a GroupBy if you plan
     * to change it.
     * @return Hash code for object
     */
    public int hashCode() {
        return HashCodeUtil.hashCode(rollup?1:0, getSymbols());
    }

    /**
     * Returns a string representation of an instance of this class.
     * @return String representation of object
     */
    public String toString() {
        return SQLStringVisitor.getSQLString(this);
    }

    public boolean isRollup() {
        return rollup;
    }

    public void setRollup(boolean rollup) {
        this.rollup = rollup;
    }

}  // END CLASS
