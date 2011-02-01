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
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
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
        return this.callableName;
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

        Integer key = new Integer(parameter.getIndex());
        if(parameter.getParameterType() == ParameterInfo.RESULT_SET){
        	resultSetParameterKey = key;
        }

        mapOfParameters.put(key, parameter);
    }
    
    /**
    * Returns a List of SPParameter objects for this stored procedure
    *
    */
    public List<SPParameter> getParameters(){
        List<SPParameter> listOfParameters = new ArrayList<SPParameter>(mapOfParameters.values());
        return listOfParameters;
    }
    
    public Map<Integer, SPParameter> getMapOfParameters() {
		return mapOfParameters;
	}

    public SPParameter getParameter(int index){
        return mapOfParameters.get(new Integer(index));
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

    public List getResultSetColumns(){
        SPParameter resultSetParameter = getResultSetParameter();
        if(resultSetParameter != null){
            List result = new LinkedList();
            for (Iterator i = resultSetParameter.getResultSetColumns().iterator(); i.hasNext();) {
                ElementSymbol symbol = (ElementSymbol)((ElementSymbol)i.next()).clone();
                symbol.setGroupSymbol(getGroup());
                result.add(symbol);
            }
        	return result;
    	}
    	return Collections.EMPTY_LIST;
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
        if (getTemporaryMetadata() != null){
            copy.setTemporaryMetadata(new HashMap(getTemporaryMetadata()));
        }
        copy.callableName = callableName;
        copy.calledWithReturn = calledWithReturn;
        List<SPParameter> params = getParameters();
        for(int i=0; i<params.size(); i++) {
            copy.setParameter((SPParameter)params.get(i).clone());
        }
        copy.resultSetParameterKey = resultSetParameterKey;
        this.copyMetadataState(copy);
        copy.displayNamedParameters = displayNamedParameters;
        copy.isCallableStatement = isCallableStatement;
        copy.isProcedureRelational = isProcedureRelational;
        return copy;
    }

    public boolean returnsResultSet(){
        return !getResultSetColumns().isEmpty();
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
	 * @return Ordered list of SingleElementSymbol
	 */
	public List getProjectedSymbols(){
		List<ElementSymbol> result = new ArrayList<ElementSymbol>();
		//add result set columns
		List rsColumns = getResultSetColumns();
		result.addAll(rsColumns);
		if (!returnParameters()) {
			return result;
		}
		//add out/inout parameter symbols
		for (SPParameter parameter : mapOfParameters.values()) {
			if(parameter.getParameterType() == ParameterInfo.RETURN_VALUE){
                ElementSymbol symbol = parameter.getParameterSymbol();
                symbol.setGroupSymbol(getGroup());
                result.add(0, symbol);
	        } else if(parameter.getParameterType() == ParameterInfo.INOUT || parameter.getParameterType() == ParameterInfo.OUT){
                ElementSymbol symbol = parameter.getParameterSymbol();
                symbol.setGroupSymbol(getGroup());
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
            return new GroupSymbol(this.getProcedureName());
        }
        return group;
    }
	
	/**
	 * @see org.teiid.query.sql.lang.Command#areResultsCachable()
	 */
	public boolean areResultsCachable() {
		return Query.areResultsCachable(getProjectedSymbols());
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

    /** 
     * Return the full parameter name for
     * the indicated parameter of this stored procedure.
     * @param param
     * @return
     * @since 4.3
     */
    public String getParamFullName(SPParameter param) {
        String paramName = param.getName();
        if(paramName.lastIndexOf(".") < 0) { //$NON-NLS-1$
            paramName = this.getProcedureName() + "." + paramName; //$NON-NLS-1$
        }
        return paramName;
    }
    
    public List<SPParameter> getInputParameters() {
    	List<SPParameter> parameters = getParameters();
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
	    
	/** 
	 * @see org.teiid.query.sql.lang.ProcedureContainer#getProcedureParameters()
	 * @since 5.0
	 */
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


