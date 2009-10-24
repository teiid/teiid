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

package org.teiid.connector.metadata.runtime;

import java.util.List;


/**
 * ProcedureRecordImpl
 */
public class ProcedureRecordImpl extends AbstractMetadataRecord {
    
	public enum Type {
		Function,
		UDF,
		StoredProc,
		StoredQuery
	}
	
	private List<String> parameterIDs;
    private boolean isFunction;
    private boolean isVirtual;
    private String resultSetID;
    private int updateCount = 1;
    private List<ProcedureParameterRecordImpl> parameters;
    private ColumnSetRecordImpl resultSet;
    private String queryPlan;
    
    public List<String> getParameterIDs() {
        return parameterIDs;
    }

    public boolean isFunction() {
        return isFunction;
    }

    public boolean isVirtual() {
        return this.isVirtual;
    }

    public String getResultSetID() {
        return resultSetID;
    }

    public Type getType() {
    	if (isFunction()) {
        	if (isVirtual()) {
        		return Type.UDF;
        	}
        	return Type.Function;
        }
        if (isVirtual()) {
            return Type.StoredQuery;
        }
        return Type.StoredProc;
    }
    
    public int getUpdateCount() {
        return this.updateCount;
    }
    
	public List<ProcedureParameterRecordImpl> getParameters() {
		return parameters;
	}

	public void setParameters(List<ProcedureParameterRecordImpl> parameters) {
		this.parameters = parameters;
	}

	public String getQueryPlan() {
		return queryPlan;
	}

	public void setQueryPlan(String queryPlan) {
		this.queryPlan = queryPlan;
	}

    /**
     * @param list
     */
    public void setParameterIDs(List<String> list) {
        parameterIDs = list;
    }

    /**
     * @param object
     */
    public void setResultSetID(String object) {
        resultSetID = object;
    }

    /**
     * @param b
     */
    public void setFunction(boolean b) {
        isFunction = b;
    }

    /**
     * @param b
     */
    public void setVirtual(boolean b) {
        isVirtual = b;
    }
    
    public void setUpdateCount(int count) {
    	this.updateCount = count;
    }

	public void setResultSet(ColumnSetRecordImpl resultSet) {
		this.resultSet = resultSet;
	}

	public ColumnSetRecordImpl getResultSet() {
		return resultSet;
	}

}