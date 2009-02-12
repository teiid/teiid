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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import com.metamatrix.api.exception.MetaMatrixException;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.dqp.message.ParameterInfo;
import com.metamatrix.query.QueryPlugin;
import com.metamatrix.query.eval.Evaluator;
import com.metamatrix.query.sql.symbol.ElementSymbol;
import com.metamatrix.query.sql.symbol.Expression;
import com.metamatrix.query.util.ErrorMessageKeys;

/**
* Represents a StoredProcedure's parameter for encapsulation in the Query framework
* This is basically a holder object set from the Server's implementation of
* a stored procedure.
* The connector will utilize this class to set the appropriate values at the
* datasource layer.
*/
public class SPParameter implements Serializable, Cloneable {

    /** Constant identifying an IN parameter */
    public static final int IN = ParameterInfo.IN;

    /** Constant identifying an OUT parameter */
    public static final int OUT = ParameterInfo.OUT;

    /** Constant identifying an INOUT parameter */
    public static final int INOUT = ParameterInfo.INOUT;

    /** Constant identifying a RETURN parameter */
    public static final int RETURN_VALUE = ParameterInfo.RETURN_VALUE;

    /** Constant identifying a RESULT SET parameter */
    public static final int RESULT_SET = ParameterInfo.RESULT_SET;

    // Basic state
    private String name;        // Param name, qualified by full procedure name
    private int parameterType = ParameterInfo.IN;
    private Class classType;
    private Expression expression;
    private int index;
    private List resultSetColumns;      //contains List of columns if it is result set
    private List resultSetIDs;          // contains List of metadataIDs for each column in the result set
    private Object metadataID;          // metadataID for the actual metadata ID
    private boolean usingDefault;

    /**
     * Constructor used when constructing a parameter during execution.  In this case we
     * know what the parameter is being filled with but no metadata about the parameter.
     *
     * @param index the positional index of this parameter
     * @param value the Value of this parameter
     */
    public SPParameter(int index, Expression expression){
        setIndex(index);
        setExpression(expression);
    }

    /**
     * Constructor used when constructing a parameter from metadata.
     * In this case we specify the description of the parameter but
     * no notion of what it is being filled with.
     *
     * @param index Parameter index
     * @param parameterType Type of parameter based on class constant - IN, OUT, etc
     * @param name Full name of parameter (including proc name)
     */
    public SPParameter(int index, int parameterType, String name) {
        setIndex(index);
        setParameterType(parameterType);
        setName(name);
    }

    /**
     * Get full parameter name, including procedure name.  If unknown, null is returned.
     * @return Parameter name
     */
    public String getName() {
        return this.name;
    }

    /**
     * Set full parameter name, including procedure name
     * @param name Parameter name
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Set parameter type according to class constants.
     * @param parameterType Type to set
     * @see ParameterInfo#IN
     * @see ParameterInfo#OUT
     * @see ParameterInfo#INOUT
     * @see ParameterInfo#RESULT_SET
     * @see ParameterInfo#RETURN_VALUE
     */
    public void setParameterType(int parameterType){
        // validate against above types
        if(parameterType < ParameterInfo.IN || parameterType > ParameterInfo.RESULT_SET) {
            throw new IllegalArgumentException(QueryPlugin.Util.getString(ErrorMessageKeys.SQL_0006, parameterType));
        }
        this.parameterType = parameterType;
    }

    /**
     * Get type of parameter according to class constants.
     * @return Parameter type
     * @see ParameterInfo#IN
     * @see ParameterInfo#OUT
     * @see ParameterInfo#INOUT
     * @see ParameterInfo#RESULT_SET
     * @see ParameterInfo#RETURN_VALUE
     */
    public int getParameterType(){
        return this.parameterType;
    }

    /**
     * Set class type - MetaMatrix runtime types.
     * @param classType See {@link com.metamatrix.common.types.DataTypeManager.DefaultDataClasses}
     * for types
     */
    public void setClassType(Class classType){
        this.classType = classType;
    }

    /**
     * Get class type - MetaMatrix runtime types.
     * @return MetaMatrix runtime type description
     */
    public Class getClassType(){
        return this.classType;
    }

    /**
     * Set the expression defining this parameter
     * @param expression The expression defining this parameter's value
     */
    public void setExpression(Expression expression){
        this.expression = expression;
    }

    /**
     * Return the expression defining the value of this parameter
     * @return Expression defining the value of this parameter
     */
    public Expression getExpression(){
        return this.expression;
    }

    /**
     * This is a helper method to get the value of this parameter when the expression
     * is a constant.  It may throw IllegalStateException if used when the expression
     * is not a Constant.
     * @return Value of this parameter
     */
    public Object getValue() {
        if(this.expression == null) {
            return null;
        }
        try {
            return Evaluator.evaluate(this.expression);
        } catch (MetaMatrixException err) {
            throw new MetaMatrixRuntimeException(err);
        }
    }

    /**
     * Set the positional index of this parameter
     * @param index The positional index of this parameter
     */
    public void setIndex(int index){
        this.index = index;
    }

    /**
     * Return the index of this parameter
     * @return The index
     */
    public int getIndex(){
        return this.index;
    }

    /**
     * Add a result set column if this parameter is a return
     * result set.
     * @param name Name of column
     * @param type Type of column
     */
    public void addResultSetColumn(String name, Class type, Object id) {
        if(resultSetColumns == null){
            resultSetColumns = new ArrayList();
            resultSetIDs = new ArrayList();
        }

        ElementSymbol rsColumn = new ElementSymbol(name);
        rsColumn.setType(type);
        rsColumn.setMetadataID(id);

        resultSetColumns.add(rsColumn);
        
        resultSetIDs.add(id);
    }

    /**
     * Get the result set columns.  If none exist, return empty list.
     * @return List of ElementSymbol representing result set columns
     */
    public List getResultSetColumns(){
        if(resultSetColumns == null){
            return Collections.EMPTY_LIST;
        }
        return resultSetColumns;
    }
    
    /**
     * Get the result set metadata IDs.  If none exist, return empty list.
     * @return List of Object representing result set metadata IDs
     */
    public List getResultSetIDs() {
        if(resultSetIDs == null) { 
            return Collections.EMPTY_LIST;
        }
        return this.resultSetIDs;
    }

    /**
     * Get a particular result set column at the specified position.
     * @param position Position of the result set column
     * @return Element symbol representing the result set column at position
     * @throws IllegalArgumentException If column doesn't exist
     */
    public ElementSymbol getResultSetColumn(int position){
        if(resultSetColumns == null){
            throw new IllegalArgumentException(QueryPlugin.Util.getString(ErrorMessageKeys.SQL_0009));
        }

        //position is 1 based
        position--;
        if(position >= 0 && position < resultSetColumns.size()) {
            return (ElementSymbol) resultSetColumns.get(position);
        }
        throw new IllegalArgumentException(QueryPlugin.Util.getString(ErrorMessageKeys.SQL_0010, new Integer(position + 1)));
    }

    /**
     * Get actual metadataID for this parameter
     * @return Actual metadata ID for this parameter
     */
    public Object getMetadataID() {
        return this.metadataID;
    }

    /**
     * Set actual metadataID for this parameter
     * @param metadataID Actual metadataID
     */
    public void setMetadataID(Object metadataID) {
        this.metadataID = metadataID;
    }

    /**
     * Check whether the parameter is internal or not.  This value is derived from
     * the parameter type.
     * @return True if parameter is a return value, false otherwise
     */
    public boolean isInternal() {
        return (this.parameterType == ParameterInfo.RETURN_VALUE || this.parameterType == ParameterInfo.RESULT_SET);
    }

    /**
     * Get element symbol representing this parameter.  The symbol will have the
     * same name and type as the parameter.
     * @return Element symbol representing the parameter
     */
    public ElementSymbol getParameterSymbol() {
        ElementSymbol symbol = new ElementSymbol(this.name);
        symbol.setType(this.classType);
        symbol.setMetadataID(this.metadataID);
		return symbol;
    }

    /**
     * Checks whether another parameter equals this one based on the index.
     * @see java.lang.Object#equals(Object)
     */
    public boolean equals(Object obj) {
		if(obj == this) {
			return true;
		}

		if(obj == null || !(obj instanceof SPParameter)){
			return false;
		}

		//the parameters are considered equal if the indexes are the same
        SPParameter other = (SPParameter) obj;
        if (this.getIndex() != other.getIndex()) {
            return false;
        }
        
        // If indexes match, check associated IDs if existent
        if (this.getMetadataID() != null && other.getMetadataID() != null) {
            return this.getMetadataID().equals(other.getMetadataID());
        }
        
        return true;
	}

    /**
     * @see java.lang.Object#hashCode()
     */
    public int hashCode() {
        return this.index;
    }

    /**
     * @see java.lang.Object#clone()
     */
    public Object clone() {
        SPParameter copy = new SPParameter(this.index, this.parameterType, this.name);
        copy.setClassType(this.classType);
        if(this.expression != null) {
            copy.setExpression((Expression)this.expression.clone());
        }
        if(this.resultSetColumns != null) {
            Iterator iter = this.resultSetColumns.iterator();
            Iterator idIter = this.resultSetIDs.iterator();
            while(iter.hasNext()) {
                ElementSymbol column = (ElementSymbol) iter.next();
                copy.addResultSetColumn(column.getName(), column.getType(), idIter.next());
            }
        }

        copy.setMetadataID(this.getMetadataID());
        copy.setUsingDefault(this.usingDefault);

        return copy;
    }

    /**
     * @see java.lang.Object#toString()
     */
    public String toString() {
        if(this.expression != null) {
            return this.expression.toString();
        }
        return "?"; //$NON-NLS-1$
    }

	public boolean isUsingDefault() {
		return usingDefault;
	}

	public void setUsingDefault(boolean usingDefault) {
		this.usingDefault = usingDefault;
	}

}
