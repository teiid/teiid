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

import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.core.util.EquivalenceUtil;
import com.metamatrix.core.util.HashCodeUtil;
import com.metamatrix.query.QueryPlugin;
import com.metamatrix.query.sql.LanguageVisitor;
import com.metamatrix.query.sql.symbol.Expression;
import com.metamatrix.query.sql.util.ValueIterator;
import com.metamatrix.query.sql.util.ValueIteratorSource;
import com.metamatrix.query.util.ErrorMessageKeys;


/** 
 * The DependentSetCriteria is missing the value set until it is filled during
 * processing.  This allows a criteria to contain a dynamic set of values provided
 * by a separate processing node.
 * @since 5.0.1
 */
public class DependentSetCriteria extends AbstractSetCriteria {

    /**
     * Specifies who will provide the value iterator later during execution.  The
     * ValueIterator is typically not ready yet, so we can't hold it directly.
     */
    private ValueIteratorSource valueIteratorSource;
    
    /**
     * Specifies the expression whose values we want to return in the iterator
     */
    private Expression valueExpression;
    
    /** 
     * Construct with the left expression 
     */
    public DependentSetCriteria(Expression expr) {
        setExpression(expr);
    }    

    /** 
 	 * Get the valute iterator source, which will provide the iterator
     * when it is ready during processing.   
     * @return Returns the valueIteratorSource.
     */
    public ValueIteratorSource getValueIteratorSource() {
        return this.valueIteratorSource;
    }

    
    /** 
     * Set the value iterator source, which will provide value iterators during processing.
     * @param valueIteratorSource The valueIteratorSource to set.
     */
    public void setValueIteratorSource(ValueIteratorSource valueIteratorSource) {
        this.valueIteratorSource = valueIteratorSource;
    }    
    
    /** 
     * Get the independent value expression
     * @return Returns the valueExpression.
     */
    public Expression getValueExpression() {
        return this.valueExpression;
    }

    
    /**
     * Set the independent value expression 
     * @param valueExpression The valueExpression to set.
     */
    public void setValueExpression(Expression valueExpression) {
        this.valueExpression = valueExpression;
    }
    
    /**
     * Returns a ValueIterator to obtain the values in this IN criteria's value set.
     * This method can only be safely called if the ValueIteratorSource is ready.
     * @return this object's ValueIterator instance 
     * @throws MetaMatrixRuntimeException if the subquery for this set criteria
     * has not yet been processed and no value iterator is available
     * @see com.metamatrix.query.sql.lang.AbstractSetCriteria#getValueIterator()
     */
    public ValueIterator getValueIterator() {
        ValueIterator valueIterator = this.valueIteratorSource.getValueIterator(this.valueExpression);
        
        if(valueIterator == null) {
            throw new MetaMatrixRuntimeException(ErrorMessageKeys.SQL_0012, QueryPlugin.Util.getString(ErrorMessageKeys.SQL_0012));
        }
        
        valueIterator.reset();
        return valueIterator;
    }

    public void acceptVisitor(LanguageVisitor visitor) {
        visitor.visit(this);
    }

    /**
     * Get hash code.  WARNING: The hash code is based on data in the criteria.
     * If data values are changed, the hash code will change - don't hash this
     * object and change values.
     * @return Hash code
     */
    public int hashCode() {
        int hc = 0;
        hc = HashCodeUtil.hashCode(hc, getExpression());
        return hc;
    }

    /**
     * Override equals() method.
     * @param obj Other object
     * @return True if equal
     */
    public boolean equals(Object obj) {
        // Use super.equals() to check obvious stuff and variable
        if(obj == this) {
            return true;
        }

        if(!(obj instanceof DependentSetCriteria)) {
            return false;
        }

        DependentSetCriteria sc = (DependentSetCriteria)obj;
        if (isNegated() != sc.isNegated()) {
            return false;
        }
        
        return EquivalenceUtil.areEqual(getExpression(), sc.getExpression()) && 
                EquivalenceUtil.areEqual(getValueExpression(), sc.getValueExpression());
    }

    /**
     * Deep copy of object.  The values iterator source of this object
     * will not be cloned - it will be passed over as is and shared with 
     * the original object, just like Reference.
     * @return Deep copy of object
     */
    public Object clone() {
        Expression copy = null;
        if(getExpression() != null) {
            copy = (Expression) getExpression().clone();
        }

        DependentSetCriteria criteriaCopy = new DependentSetCriteria(copy);
        criteriaCopy.setNegated(isNegated());
        criteriaCopy.setValueIteratorSource(getValueIteratorSource());
        criteriaCopy.setValueExpression((Expression) getValueExpression().clone());
        return criteriaCopy;
    }
    
    /** 
     * This method is not supported for DependentSetCriteria as it will obtain it's 
     * value iterators for the ValueIteratorSource.
     * @see com.metamatrix.query.sql.lang.AbstractSetCriteria#setValueIterator(com.metamatrix.query.sql.util.ValueIterator)
     * @throws UnsupportedOperationException Always
     */
    public void setValueIterator(ValueIterator valueIterator) {
        throw new UnsupportedOperationException("DependentSetCriteria.setValueIterator() should never be called as the value iterator is produced dynamically."); //$NON-NLS-1$
    }
    
}
