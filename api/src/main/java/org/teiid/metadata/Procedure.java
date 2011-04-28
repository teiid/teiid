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

package org.teiid.metadata;

import java.util.LinkedList;
import java.util.List;


/**
 * ProcedureRecordImpl
 */
public class Procedure extends AbstractMetadataRecord {
    
	private static final long serialVersionUID = 7714869437683360834L;

	public enum Type {
		Function,
		UDF,
		StoredProc,
		StoredQuery
	}
	
    private boolean isFunction;
    private boolean isVirtual;
    private int updateCount = 1;
    private List<ProcedureParameter> parameters = new LinkedList<ProcedureParameter>();
    private ColumnSet<Procedure> resultSet;
    private String queryPlan;
    
    private Schema parent;
    private transient long lastModified;
    
    public void setParent(Schema parent) {
		this.parent = parent;
	}
    
    public boolean isFunction() {
        return isFunction;
    }

    public boolean isVirtual() {
        return this.isVirtual;
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
    
	public List<ProcedureParameter> getParameters() {
		return parameters;
	}

	public void setParameters(List<ProcedureParameter> parameters) {
		this.parameters = parameters;
	}

	public String getQueryPlan() {
		return queryPlan;
	}

	public void setQueryPlan(String queryPlan) {
		this.queryPlan = queryPlan;
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

	public void setResultSet(ColumnSet<Procedure> resultSet) {
		this.resultSet = resultSet;
		if (resultSet != null) {
			resultSet.setParent(this);
		}
	}

	public ColumnSet<Procedure> getResultSet() {
		return resultSet;
	}
	
	@Override
	public Schema getParent() {
		return parent;
	}
	
	public long getLastModified() {
		return lastModified;
	}
	
	public void setLastModified(long lastModified) {
		this.lastModified = lastModified;
	}

}