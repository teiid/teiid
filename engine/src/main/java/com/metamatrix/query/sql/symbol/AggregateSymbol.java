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

package com.metamatrix.query.sql.symbol;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.core.util.EquivalenceUtil;
import com.metamatrix.core.util.HashCodeUtil;
import com.metamatrix.query.QueryPlugin;
import com.metamatrix.query.sql.LanguageVisitor;
import com.metamatrix.query.sql.ReservedWords;
import com.metamatrix.query.util.ErrorMessageKeys;

/**
 * <p>An aggregate symbol represents an aggregate function in the SELECT or HAVING clauses.  It
 * extends ExpressionSymbol as they have many things in common.  The aggregate symbol is
 * typically something like <code>SUM(stock.quantity * 2)</code>.  There are five supported
 * aggregate functions: COUNT, SUM, AVG, MIN, and MAX.  Aggregate functions contain an expression -
 * this data is managed by the super class, ExpressionSymbol.  Aggregate functions may also
 * specify a DISTINCT flag to indicate that duplicates should be ignored.  The DISTINCT flag
 * may be set for all five aggregate functions but is ignored for the computation of MIN and MAX.
 * One special use of an aggregate symbol is for the symbol <code>COUNT(*)</code>.  The * expression
 * is encoded by setting the expression to null.  This may ONLY be used with the COUNT function.</p>
 *
 * <p>The type of an aggregate symbol depends on the function and the type of the underlying
 * expression.  The type of a COUNT function is ALWAYS integer.  MIN and MAX functions take the
 * type of their contained expression.  AVG and SUM vary depending on the type of the expression.
 * If the expression is of a type other than biginteger, the aggregate function returns type long.
 * For the case of biginteger, the aggregate function returns type biginteger.  Similarly, all
 * floating point expressions not of type bigdecimal return type double and bigdecimal maps to
 * bigdecimal.</p>
 */
public class AggregateSymbol extends ExpressionSymbol {

	private String aggregate;
	private boolean distinct;

	private static final Class COUNT_TYPE = DataTypeManager.getDataTypeClass(DataTypeManager.DefaultDataTypes.INTEGER);
	private static final Set AGGREGATE_FUNCTIONS;
	private static final Map SUM_TYPES;
    private static final Map AVG_TYPES;

	static {
		AGGREGATE_FUNCTIONS = new HashSet();
		AGGREGATE_FUNCTIONS.add(ReservedWords.COUNT);
		AGGREGATE_FUNCTIONS.add(ReservedWords.SUM);
		AGGREGATE_FUNCTIONS.add(ReservedWords.AVG);
		AGGREGATE_FUNCTIONS.add(ReservedWords.MIN);
		AGGREGATE_FUNCTIONS.add(ReservedWords.MAX);

		SUM_TYPES = new HashMap();
		SUM_TYPES.put(DataTypeManager.DefaultDataClasses.BYTE, DataTypeManager.DefaultDataClasses.LONG);
		SUM_TYPES.put(DataTypeManager.DefaultDataClasses.SHORT, DataTypeManager.DefaultDataClasses.LONG);
		SUM_TYPES.put(DataTypeManager.DefaultDataClasses.INTEGER, DataTypeManager.DefaultDataClasses.LONG);
		SUM_TYPES.put(DataTypeManager.DefaultDataClasses.LONG, DataTypeManager.DefaultDataClasses.LONG);
		SUM_TYPES.put(DataTypeManager.DefaultDataClasses.BIG_INTEGER, DataTypeManager.DefaultDataClasses.BIG_INTEGER);
		SUM_TYPES.put(DataTypeManager.DefaultDataClasses.FLOAT, DataTypeManager.DefaultDataClasses.DOUBLE);
		SUM_TYPES.put(DataTypeManager.DefaultDataClasses.DOUBLE, DataTypeManager.DefaultDataClasses.DOUBLE);
		SUM_TYPES.put(DataTypeManager.DefaultDataClasses.BIG_DECIMAL, DataTypeManager.DefaultDataClasses.BIG_DECIMAL);
        
        AVG_TYPES = new HashMap();
        AVG_TYPES.put(DataTypeManager.DefaultDataClasses.BYTE, DataTypeManager.DefaultDataClasses.DOUBLE);
        AVG_TYPES.put(DataTypeManager.DefaultDataClasses.SHORT, DataTypeManager.DefaultDataClasses.DOUBLE);
        AVG_TYPES.put(DataTypeManager.DefaultDataClasses.INTEGER, DataTypeManager.DefaultDataClasses.DOUBLE);
        AVG_TYPES.put(DataTypeManager.DefaultDataClasses.LONG, DataTypeManager.DefaultDataClasses.DOUBLE);
        AVG_TYPES.put(DataTypeManager.DefaultDataClasses.BIG_INTEGER, DataTypeManager.DefaultDataClasses.BIG_DECIMAL);
        AVG_TYPES.put(DataTypeManager.DefaultDataClasses.FLOAT, DataTypeManager.DefaultDataClasses.DOUBLE);
        AVG_TYPES.put(DataTypeManager.DefaultDataClasses.DOUBLE, DataTypeManager.DefaultDataClasses.DOUBLE);
        AVG_TYPES.put(DataTypeManager.DefaultDataClasses.BIG_DECIMAL, DataTypeManager.DefaultDataClasses.BIG_DECIMAL);        
	}

    /**
     * Constructor used for cloning 
     * @param name
     * @param canonicalName
     * @since 4.3
     */
    protected AggregateSymbol(String name, String canonicalName, String aggregateFunction, boolean isDistinct, Expression expression) {
        super(name, canonicalName, expression);
        
        setAggregateFunction(aggregateFunction);
        this.distinct = isDistinct;
    }
    
	/**
	 * Construct an aggregate symbol with all given data.
	 * @param name Name of the function
	 * @param aggregateFunction Aggregate function type ({@link com.metamatrix.query.sql.ReservedWords#COUNT}, etc)
	 * @param isDistinct True if DISTINCT flag is set
	 * @param expression Contained expression
	 */
	public AggregateSymbol(String name, String aggregateFunction, boolean isDistinct, Expression expression) {
		super(name, expression);

		setAggregateFunction(aggregateFunction);
		this.distinct = isDistinct;
	}

	/**
	 * Set the aggregate function.  If the aggregate function is an invalid value, an
	 * IllegalArgumentException is thrown.
	 * @param aggregateFunction Aggregate function type
	 * @see com.metamatrix.query.sql.ReservedWords#COUNT
	 * @see com.metamatrix.query.sql.ReservedWords#SUM
	 * @see com.metamatrix.query.sql.ReservedWords#AVG
	 * @see com.metamatrix.query.sql.ReservedWords#MIN
	 * @see com.metamatrix.query.sql.ReservedWords#MAX
	 */
	private void setAggregateFunction(String aggregateFunction) {
		// Validate aggregate
		if(! AGGREGATE_FUNCTIONS.contains(aggregateFunction)) {
            throw new IllegalArgumentException(QueryPlugin.Util.getString(ErrorMessageKeys.SQL_0013, new Object[] {aggregateFunction, AGGREGATE_FUNCTIONS}));
		}
		this.aggregate = aggregateFunction;
	}

	/**
	 * Get the aggregate function type - this will map to one of the reserved words
	 * for the aggregate functions.
	 * @return Aggregate function type
	 * @see com.metamatrix.query.sql.ReservedWords#COUNT
	 * @see com.metamatrix.query.sql.ReservedWords#SUM
	 * @see com.metamatrix.query.sql.ReservedWords#AVG
	 * @see com.metamatrix.query.sql.ReservedWords#MIN
	 * @see com.metamatrix.query.sql.ReservedWords#MAX
	 */
	public String getAggregateFunction() {
		return this.aggregate;
	}

	/**
	 * Get the distinct flag.  If true, aggregate symbol will remove duplicates during
	 * computation.
	 * @return True if duplicates should be removed during computation
	 */
	public boolean isDistinct() {
		return this.distinct;
	}

	/**
	 * Get the type of the symbol, which depends on the aggregate function and the
	 * type of the contained expression
	 * @return Type of the symbol
	 */
	public Class getType() {
		if(this.aggregate.equals(ReservedWords.COUNT)) {
			return COUNT_TYPE;
		} else if(this.aggregate.equals(ReservedWords.SUM) ) {
			Class expressionType = this.getExpression().getType();
			return (Class) SUM_TYPES.get(expressionType);
        } else if (this.aggregate.equals(ReservedWords.AVG)) {
            Class expressionType = this.getExpression().getType();
            return (Class) AVG_TYPES.get(expressionType);
		} else {
			return this.getExpression().getType();
		}
	}

    public void acceptVisitor(LanguageVisitor visitor) {
        visitor.visit(this);
    }

	/**
	 * Return a deep copy of this object
	 */
	public Object clone() {
		AggregateSymbol copy = null;
		if(getExpression() != null) {
			copy = new AggregateSymbol(getName(), getCanonical(), getAggregateFunction(), isDistinct(), (Expression) getExpression().clone());
		} else {
			copy = new AggregateSymbol(getName(), getCanonical(), getAggregateFunction(), isDistinct(), null);
		}

		return copy;
	}
    
    /** 
     * @see com.metamatrix.query.sql.symbol.ExpressionSymbol#hashCode()
     */
    public int hashCode() {
        int hasCode = HashCodeUtil.hashCode(aggregate.hashCode(), distinct);
        return HashCodeUtil.hashCode(hasCode, this.getExpression());
    }
    
    /** 
     * @see com.metamatrix.query.sql.symbol.ExpressionSymbol#equals(java.lang.Object)
     */
    public boolean equals(Object obj) {
        if (!(obj instanceof AggregateSymbol)) {
            return false;
        }
        
        AggregateSymbol other = (AggregateSymbol)obj;
        
        return this.aggregate.equals(other.aggregate)
               && this.distinct == other.distinct
               && EquivalenceUtil.areEqual(this.getExpression(), other.getExpression());
    }

}
