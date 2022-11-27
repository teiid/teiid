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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.teiid.client.metadata.ParameterInfo;
import org.teiid.core.util.EquivalenceUtil;
import org.teiid.query.QueryPlugin;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;


/**
* Represents a StoredProcedure's parameter for encapsulation in the Query framework
* This is basically a holder object set from the Server's implementation of
* a stored procedure.
* The connector will utilize this class to set the appropriate values at the
* datasource layer.
*/
public class SPParameter implements Cloneable {

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
    private int parameterType = ParameterInfo.IN;
    private Expression expression;
    private int index;
    private List<ElementSymbol> resultSetColumns;      //contains List of columns if it is result set
    private List<Object> resultSetIDs;          // contains List of metadataIDs for each column in the result set
    private boolean usingDefault;
    private boolean varArg;
    private ElementSymbol parameterSymbol;

    /**
     * Constructor used when constructing a parameter during execution.  In this case we
     * know what the parameter is being filled with but no metadata about the parameter.
     *
     * @param index the positional index of this parameter
     * @param expression the Value of this parameter
     */
    public SPParameter(int index, Expression expression){
        setIndex(index);
        setExpression(expression);
        this.parameterSymbol = new ElementSymbol(""); //$NON-NLS-1$
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
        this.parameterSymbol = new ElementSymbol(name);
    }

    private SPParameter() {

    }

    /**
     * Get full parameter name,.  If unknown, null is returned.
     * @return Parameter name
     */
    public String getName() {
        return this.parameterSymbol.getName();
    }

    /**
     * Set full parameter name
     * @param name Parameter name
     */
    public void setName(String name) {
        ElementSymbol es = new ElementSymbol(name);
        es.setMetadataID(parameterSymbol.getMetadataID());
        es.setType(parameterSymbol.getType());
        this.parameterSymbol = es;
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
            throw new IllegalArgumentException(QueryPlugin.Util.getString("ERR.015.010.0006", parameterType)); //$NON-NLS-1$
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
     * @param classType See {@link org.teiid.core.types.DataTypeManager.DefaultDataClasses}
     * for types
     */
    public void setClassType(Class<?> classType){
        this.parameterSymbol.setType(classType);
    }

    /**
     * Get class type - MetaMatrix runtime types.
     * @return MetaMatrix runtime type description
     */
    public Class<?> getClassType(){
        return this.parameterSymbol.getType();
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
     * @param colName Name of column
     * @param type Type of column
     */
    public void addResultSetColumn(String colName, Class<?> type, Object id) {
        if(resultSetColumns == null){
            resultSetColumns = new ArrayList<ElementSymbol>();
            resultSetIDs = new ArrayList<Object>();
        }

        ElementSymbol rsColumn = new ElementSymbol(colName);
        rsColumn.setType(type);
        rsColumn.setMetadataID(id);

        resultSetColumns.add(rsColumn);

        resultSetIDs.add(id);
    }

    /**
     * Get the result set columns.  If none exist, return empty list.
     * @return List of ElementSymbol representing result set columns
     */
    public List<ElementSymbol> getResultSetColumns(){
        if(resultSetColumns == null){
            return Collections.emptyList();
        }
        return resultSetColumns;
    }

    /**
     * Get the result set metadata IDs.  If none exist, return empty list.
     * @return List of Object representing result set metadata IDs
     */
    public List<Object> getResultSetIDs() {
        if(resultSetIDs == null) {
            return Collections.emptyList();
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
            throw new IllegalArgumentException(QueryPlugin.Util.getString("ERR.015.010.0009")); //$NON-NLS-1$
        }

        //position is 1 based
        position--;
        if(position >= 0 && position < resultSetColumns.size()) {
            return resultSetColumns.get(position);
        }
        throw new IllegalArgumentException(QueryPlugin.Util.getString("ERR.015.010.0010", new Integer(position + 1))); //$NON-NLS-1$
    }

    /**
     * Get actual metadataID for this parameter
     * @return Actual metadata ID for this parameter
     */
    public Object getMetadataID() {
        return this.parameterSymbol.getMetadataID();
    }

    /**
     * Set actual metadataID for this parameter
     * @param metadataID Actual metadataID
     */
    public void setMetadataID(Object metadataID) {
        this.parameterSymbol.setMetadataID(metadataID);
    }

    /**
     * Get element symbol representing this parameter.  The symbol will have the
     * same name and type as the parameter.
     * @return Element symbol representing the parameter
     */
    public ElementSymbol getParameterSymbol() {
        return parameterSymbol;
    }

    /**
     * Checks whether another parameter equals this one based on the index.
     * @see java.lang.Object#equals(Object)
     */
    public boolean equals(Object obj) {
        if(obj == this) {
            return true;
        }

        if(!(obj instanceof SPParameter)){
            return false;
        }

        //the parameters are considered equal if the indexes are the same
        SPParameter other = (SPParameter) obj;
        if (this.getIndex() != other.getIndex()) {
            return false;
        }

        // If indexes match, check associated IDs if existent
        return EquivalenceUtil.areEqual(this.expression, other.expression);
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
        SPParameter copy = new SPParameter();
        copy.index = this.index;
        copy.parameterType = this.parameterType;
        copy.parameterSymbol = this.parameterSymbol.clone();
        if(this.expression != null) {
            copy.setExpression((Expression)this.expression.clone());
        }
        if(this.resultSetColumns != null) {
            Iterator<ElementSymbol> iter = this.resultSetColumns.iterator();
            Iterator<Object> idIter = this.resultSetIDs.iterator();
            while(iter.hasNext()) {
                ElementSymbol column = iter.next();
                copy.addResultSetColumn(column.getName(), column.getType(), idIter.next());
            }
        }
        copy.setUsingDefault(this.usingDefault);
        copy.varArg = this.varArg;
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

    public void setVarArg(boolean varArg) {
        this.varArg = varArg;
    }

    public boolean isVarArg() {
        return varArg;
    }

}
