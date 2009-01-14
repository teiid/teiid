/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.ExpressionEvaluationException;
import com.metamatrix.query.eval.Evaluator;
import com.metamatrix.query.eval.LookupEvaluator;
import com.metamatrix.query.sql.LanguageVisitor;
import com.metamatrix.query.sql.visitor.SQLStringVisitor;
import com.metamatrix.query.util.CommandContext;

/**
 * This class represents a reference within a SQL statement to some other 
 * source of data.  This reference may resolve to many different values
 * during evaluation.  For any particular bound value, it is treated as a constant.
 */
public class Reference implements Expression {

    private int refIndex;
    private Expression expression;
    //set true for correlated references only when determining if the subquery can be pushed down
    private boolean correlated;
    private boolean positional;

    transient private Map elements;       // Elements needed for evaluation, may be null
    transient private List tuple;         // Current data values for elements

    /**
     * Constructor for Reference.
     */
    public Reference(int refIndex) {
        this.refIndex = refIndex;
        this.positional = true;
    }
    
    /**
     * Constructor for Reference.
     */
    public Reference(int refIndex, Expression expression) {
        this.refIndex = refIndex;
        this.expression = expression;
        this.positional = false;
    }    

    public boolean isResolved() {
        return (expression != null);
    }

    public int getIndex() {
        return this.refIndex;
    }

    public void setExpression(Expression expression) { 
        this.expression = expression;
    }

    public Expression getExpression() { 
        return this.expression;    
    }

    public Class getType() {
        if(expression == null) { 
            return null;
        }
        return expression.getType();
    }
    
    /**
     * Set value provider for this reference
     * @param provider Provider of values for this reference
     */
    public void setData(Map elements, List tuple) {
        this.elements = elements;
        this.tuple = tuple;    
    }

    public Map getDataElements() {
        return this.elements;
    }
    
    public List getTuple() {
        return this.tuple;
    }

    public Object getValue(LookupEvaluator dataMgr, CommandContext context) throws ExpressionEvaluationException, MetaMatrixComponentException {
        return new Evaluator(elements, dataMgr, context).evaluate(expression, tuple);
    }
    
    public void acceptVisitor(LanguageVisitor visitor) {
        visitor.visit(this);
    }

    /**
     * @see java.lang.Object#clone()
     */
    public Object clone() {
        return this;
    }

    /**
     * Compare this constant to another constant for equality.
     * @param obj Other object
     * @return True if constants are equal
     */
    public boolean equals(Object obj) {
        if(this == obj) {
            return true;
        }
        
        if(!(obj instanceof Reference)) {
            return false;
        }
        Reference other = (Reference) obj;

        if (this.positional != other.positional) {
            return false;
        }
        
        if (this.positional) {
            return other.getIndex() == getIndex();
        }
        
        // Compare based on name
        return this.expression.equals(other.expression);
    }
    
    /**
     * Define hash code to be that of the underlying object to make it stable.
     * @return Hash code, based on value
     */
    public int hashCode() { 
        return getIndex();
    }
    
    /**
     * Return a String representation of this object using SQLStringVisitor.
     * @return String representation using SQLStringVisitor
     */
    public String toString() {
        return SQLStringVisitor.getSQLString(this);    
    }

    public boolean isCorrelated() {
        return this.correlated;
    }

    public void setCorrelated(boolean correlated) {
        this.correlated = correlated;
    }
    
    public void setValue(Object value) {
        HashMap symbolMap = new HashMap(1);
        symbolMap.put(this.getExpression(), new Integer(0));

        if (value instanceof Constant) {
            value = ((Constant)value).getValue();
        }

        this.setData(symbolMap, Arrays.asList(new Object[] {
            value
        }));
    }

    public boolean isPositional() {
        return this.positional;
    }

    public void setPositional(boolean positional) {
        this.positional = positional;
    }
}

