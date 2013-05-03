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
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.teiid.client.metadata.ParameterInfo;
import org.teiid.query.QueryPlugin;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.visitor.SQLStringVisitor;


/**
 * Represents a StoredProcedure statement of the form:
 */
public class StoredProcedure extends ProcedureContainer {

    // =========================================================================
    //                         C O N S T R U C T O R S
    // =========================================================================

    /** Used as parameters */
	private Map<Integer, SPParameter> mapOfParameters = new TreeMap<Integer, SPParameter>();

    /** Used to reference result set parameter if there is any */
    private Integer resultSetParameterKey;

    private String procedureName;

    private Object procedureID;
    private Object modelID;
    private String callableName;

    //stored procedure is treated as a group
    private GroupSymbol group;
    
    //whether parameters should be displayed in traditional indexed
    //manor, or as named parameters
    private boolean displayNamedParameters;
    private boolean calledWithReturn;
    private boolean isCallableStatement;
    private boolean isProcedureRelational;
    
    /**
     * Constructs a default instance of this class.
     */
    public StoredProcedure() {
    }

	/**
	 * Return type of command.
	 * @return TYPE_STORED_PROCEDURE
	 */
	public int getType() {
		return Command.TYPE_STORED_PROCEDURE;
	}

    private SPParameter getResultSetParameter(){
        if (this.resultSetParameterKey != null){
            return mapOfParameters.get(resultSetParameterKey);
        }
        return null;
    }


    // =========================================================================
    //                             M E T H O D S
    // =========================================================================
    /**
    * Set this stored procedure's name
    *
    * @param procedureName the stored procedure's name
    * @throws IllegalArgumentExcecption if the procedureName is invalid.
    */
    public void setProcedureName(String procedureName){
        this.procedureName = procedureName;
    }

    /**
    * Get this stored procedure's name
    *
    * @return procedureName the stored procedure's name
    */
    public String getProcedureName(){
        return this.procedureName;
    }
    public String getProcedureCallableName(){
        return this.callableName != null?this.callableName:this.procedureName;
    }
    public void setProcedureCallableName(String callableName){
        this.callableName = callableName;
    }
    public Object getModelID(){
        return modelID;
    }
    public void setModelID(Object modelID){
        this.modelID = modelID;
    }
    public void setProcedureID(Object procedureID){
        this.procedureID = procedureID;
    }
    public Object getProcedureID(){
        return this.procedureID;
    }

    /**
    * Set a stored procedure's parameter
    *
    * @param index the index of the parameter to set
    * @param parameter <code>StoredProcedureParameter</code> the parameter
    * @throws IllegalArgumentExcecption if the parameters (index and parameter)
    *   are invalid.
    */
    public void setParameter(SPParameter parameter){
        if(parameter == null){
            throw new IllegalArgumentException(QueryPlugin.Util.getString("ERR.015.010.0011")); //$NON-NLS-1$
        }

        Integer key = parameter.getIndex();
        if(parameter.getParameterType() == ParameterInfo.RESULT_SET){
        	resultSetParameterKey = key;
        }

        mapOfParameters.put(key, parameter);
    }
    
    /**
    * Returns a List of SPParameter objects for this stored procedure
    *
    */
    public Collection<SPParameter> getParameters(){
        return mapOfParameters.values();
    }
    
    public Map<Integer, SPParameter> getMapOfParameters() {
		return mapOfParameters;
	}

    public SPParameter getParameter(int index){
        return mapOfParameters.get(index);
    }

    public int getNumberOfColumns(){
        SPParameter resultSetParameter = getResultSetParameter();
    	if(resultSetParameter != null){
        	return resultSetParameter.getResultSetColumns().size();
    	}
    	return 0;
    }

    public ElementSymbol getResultSetColumn(int index){
        SPParameter resultSetParameter = getResultSetParameter();
    	if(resultSetParameter != null){
        	return resultSetParameter.getResultSetColumn(index);
    	}
    	return null;
    }

    public List<ElementSymbol> getResultSetColumns(){
        SPParameter resultSetParameter = getResultSetParameter();
        if(resultSetParameter != null){
            List<ElementSymbol> result = new ArrayList<ElementSymbol>(resultSetParameter.getResultSetColumns().size());
            for (Iterator<ElementSymbol> i = resultSetParameter.getResultSetColumns().iterator(); i.hasNext();) {
                ElementSymbol symbol = i.next().clone();
                symbol.setGroupSymbol(getGroup());
                result.add(symbol);
            }
        	return result;
    	}
    	return Collections.emptyList();
    }

    public void acceptVisitor(LanguageVisitor visitor) {
        visitor.visit(this);
    }

    public Object clone() {
        StoredProcedure copy = new StoredProcedure();
        copy.setModelID(getModelID());
        copy.setProcedureName(getProcedureName());
        copy.setProcedureCallableName(getProcedureCallableName());
        copy.setProcedureID(getProcedureID());
        copy.setGroup(getGroup().clone());
        copy.callableName = callableName;
        copy.calledWithReturn = calledWithReturn;
        Collection<SPParameter> params = getParameters();
        for (SPParameter spParameter : params) {
            copy.setParameter((SPParameter)spParameter.clone());
        }
        copy.resultSetParameterKey = resultSetParameterKey;
        this.copyMetadataState(copy);
        copy.displayNamedParameters = displayNamedParameters;
        copy.isCallableStatement = isCallableStatement;
        copy.isProcedureRelational = isProcedureRelational;
        return copy;
    }

    public boolean returnsResultSet(){
        SPParameter param = getResultSetParameter();
        return param != null && !param.getResultSetColumns().isEmpty();
    }

    public boolean returnsScalarValue(){
    	for (SPParameter param : this.mapOfParameters.values()) {
        	if (param.getParameterType() == SPParameter.RETURN_VALUE) {
        		return true;
        	}
    	}
    	return false;
    }
    
    public boolean returnParameters() {
    	return isCallableStatement || !returnsResultSet();
    }

    /**
	 * Get the ordered list of all elements returned by this query.  These elements
	 * may be ElementSymbols or ExpressionSymbols but in all cases each represents a
	 * single column.
	 * @return Ordered list of ElementSymbol
	 */
	public List getProjectedSymbols(){
		if (!returnParameters()) {
			return getResultSetColumns();
		}
		//add result set columns
		List<ElementSymbol> result = new ArrayList<ElementSymbol>(getResultSetColumns());
		//add out/inout parameter symbols
		for (SPParameter parameter : mapOfParameters.values()) {
			if(parameter.getParameterType() == ParameterInfo.RETURN_VALUE){
                ElementSymbol symbol = parameter.getParameterSymbol();
                symbol.setGroupSymbol(this.getGroup());
                result.add(0, symbol);
	        } else if(parameter.getParameterType() == ParameterInfo.INOUT || parameter.getParameterType() == ParameterInfo.OUT){
                ElementSymbol symbol = parameter.getParameterSymbol();
                symbol.setGroupSymbol(this.getGroup());
	        	result.add(symbol);
	        }
		}
		return result;
	}

	/**
     * Returns a string representation of an instance of this class.
     * @return String representation of object
     */
    public String toString() {
    	return SQLStringVisitor.getSQLString(this);
    }
    
    /** 
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        return this.getGroup().hashCode();
    }

    public boolean equals(Object obj) {
    	// Quick same object test
    	if(this == obj) {
    		return true;
		}

		// Quick fail tests
    	if(!(obj instanceof StoredProcedure)) {
    		return false;
		}
    	
    	StoredProcedure other = (StoredProcedure)obj;
    	
		return sameOptionAndHint(other) && 
		this.getGroup().equals(other.getGroup()) &&
		this.mapOfParameters.equals(other.mapOfParameters);
    }

    public void clearParameters(){
    	this.mapOfParameters.clear();
    }

    public void setGroup(GroupSymbol group){
    	this.group = group;
    }

    public GroupSymbol getGroup() {
        if(group == null) {
            return new GroupSymbol(this.getProcedureCallableName());
        }
        return group;
    }
	
	/**
	 * @see org.teiid.query.sql.lang.Command#areResultsCachable()
	 */
	public boolean areResultsCachable() {
		if (getUpdateCount() > 0) {
			return false;
		}
		return Query.areColumnsCachable(getProjectedSymbols());
	}
    
    /** 
     * Indicates whether parameters should be displayed in traditional 
     * indexed manor, or as named parameters
     * @return Returns whether to display parameters as named or not
     * @since 4.3
     */
    public boolean displayNamedParameters() {
        return this.displayNamedParameters;
    }

    
    /** 
     * Indicate whether parameters should be displayed in traditional 
     * indexed manor, or as named parameters
     * @param namedParameters whether to display parameters as named or not
     * @since 4.3
     */
    public void setDisplayNamedParameters(boolean displayNamedParameters) {
        this.displayNamedParameters = displayNamedParameters;
    }

    public List<SPParameter> getInputParameters() {
    	List<SPParameter> parameters = new ArrayList<SPParameter>(getParameters());
    	Iterator<SPParameter> params = parameters.iterator();
    	while (params.hasNext()) {
    		SPParameter param = params.next();
    		if(param.getParameterType() != ParameterInfo.IN && param.getParameterType() != ParameterInfo.INOUT) {
    			params.remove();
    		}
    	}
    	return parameters;
    }
    
    public boolean isProcedureRelational() {
		return isProcedureRelational;
	}
    
    public void setProcedureRelational(boolean isProcedureRelational) {
		this.isProcedureRelational = isProcedureRelational;
	}

	public boolean isCallableStatement() {
		return isCallableStatement;
	}

	public void setCallableStatement(boolean isCallableStatement) {
		this.isCallableStatement = isCallableStatement;
	}
	    
	public LinkedHashMap<ElementSymbol, Expression> getProcedureParameters() {
		LinkedHashMap<ElementSymbol, Expression> map = new LinkedHashMap<ElementSymbol, Expression>();
	    for (SPParameter element : this.getInputParameters()) {
	        map.put(element.getParameterSymbol(), element.getExpression());            
	    } // for
	    
	    return map;
	}

	public void setCalledWithReturn(boolean calledWithReturn) {
		this.calledWithReturn = calledWithReturn;
	}

	public boolean isCalledWithReturn() {
		return calledWithReturn;
	}

}


