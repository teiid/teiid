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

package org.teiid.query.mapping.xml;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.teiid.query.processor.ProcessorPlan;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.lang.Insert;
import org.teiid.query.sql.lang.OrderBy;
import org.teiid.query.sql.symbol.ElementSymbol;


/** 
 * Represents a result set used in an XML query - this can be based on either a mapping class query
 * or a staging table.  All info about the result set is stored in this object - some is only applicable
 * for certain kinds of result sets.
 */
public class ResultSetInfo {

    private String resultSetName;
    
    // The result set command
    private Command rsCommand;
    
    // The processor plan output for the result set 
    private ProcessorPlan rsPlan;
    
    // Row limit, may be null if no limit
    private int userRowLimit = -1;
    
    // whether or not to throw exception on row limit
    private boolean exceptionOnRowLimit = false;
           
    private OrderBy orderBy;
    
    private Criteria criteria;
    
    private Set<MappingSourceNode> criteriaResultSets = new HashSet<MappingSourceNode>();
    
    private boolean criteriaRaised = false;
    
    private ElementSymbol mappingClassSymbol;
	private boolean inputSet;
	private boolean isCritNullDependent;

	//auto-staging related info
	private String stagingRoot;
	private String tempTable;
	private Command tempSelect;
	private Insert tempInsert;
	private Command tempDrop;
	private boolean isAutoStaged;
	private List<ElementSymbol> fkColumns;
    
    public ResultSetInfo(String resultName) {
        this.resultSetName = resultName;
    }    
    
    public String getResultSetName() {
        return this.resultSetName;
    }
    
    public Command getCommand() {
        return this.rsCommand;
    }
    
    public void setCommand(Command cmd) {
        this.rsCommand = cmd;
    }
        
    public ProcessorPlan getPlan() {
        return rsPlan;
    }
    
    public void setPlan(ProcessorPlan plan) {
        this.rsPlan = plan;
    }
    
    public int getUserRowLimit() {
        return userRowLimit;
    }
    
    public void setUserRowLimit(int limit, boolean throwException) {
        this.userRowLimit = limit;
        this.exceptionOnRowLimit = throwException;
    }
    
    public boolean exceptionOnRowlimit() {
        return exceptionOnRowLimit;
    }
    
    public Criteria getCriteria() {
        return this.criteria;
    }

    public void setCriteria(Criteria criteria) {
        this.criteria = criteria;
    }
    
    public OrderBy getOrderBy() {
        return this.orderBy;
    }

    public void setOrderBy(OrderBy orderBy) {
        this.orderBy = orderBy;
    }
    
    public Set<MappingSourceNode> getCriteriaResultSets() {
        return this.criteriaResultSets;
    }

    public void addToCriteriaResultSets(Set<MappingSourceNode> criteriaResultSets) {
        this.criteriaResultSets.addAll(criteriaResultSets);
    }    
    
    public boolean isCriteriaRaised() {
        return this.criteriaRaised;
    }
    
    public void setCriteriaRaised(boolean criteriaRaised) {
        this.criteriaRaised = criteriaRaised;
    }    
    
    public Object clone() {
        ResultSetInfo clone = new ResultSetInfo(this.resultSetName);
        clone.rsPlan = this.rsPlan;
        clone.userRowLimit = this.userRowLimit;
        clone.exceptionOnRowLimit = this.exceptionOnRowLimit;
        clone.rsCommand = (Command)this.rsCommand.clone();
        clone.criteriaRaised = this.criteriaRaised;
        clone.mappingClassSymbol = this.mappingClassSymbol;
        clone.tempInsert = this.tempInsert;
        clone.tempSelect = this.tempSelect;
        clone.tempTable = this.tempTable;
        clone.tempDrop = this.tempDrop;
        clone.isAutoStaged = this.isAutoStaged;
        clone.fkColumns = this.fkColumns;
        return clone;
    }
    
    public String toString() {
        return resultSetName + ", resultSetObject " + rsCommand; //$NON-NLS-1$
    }

    public ElementSymbol getMappingClassSymbol() {
        return this.mappingClassSymbol;
    }

    public void setMappingClassSymbol(ElementSymbol mappingClassSymbol) {
        this.mappingClassSymbol = mappingClassSymbol;
    }

	public boolean hasInputSet() {
		return inputSet;
	}

	public void setInputSet(boolean inputSet) {
		this.inputSet = inputSet;
	}
	
	public void setCritNullDependent(boolean isCritNullDependent) {
		this.isCritNullDependent = isCritNullDependent;
	}
	
	public boolean isCritNullDependent(){
		return this.isCritNullDependent;
	}
	
	public String getStagingRoot() {
		return stagingRoot;
	}
	
	public void setStagingRoot(String stagingRoot) {
		this.stagingRoot = stagingRoot;
	}

	public void setTempSelect(Command tempSelect) {
		this.tempSelect = tempSelect;
	}
	
	public Command getTempSelect() {
		return tempSelect;
	}
	
	public void setTempInsert(Insert tempInsert) {
		this.tempInsert = tempInsert;
	}
	
	public Insert getTempInsert() {
		return tempInsert;
	}
	
	public Command getTempDrop() {
		return tempDrop;
	}
	
	public void setTempDrop(Command tempDrop) {
		this.tempDrop = tempDrop;
	}

	public String getTempTable() {
		return tempTable;
	}
	
	public void setTempTable(String rsTempTable) {
		this.tempTable = rsTempTable;
	}
	
	public boolean isAutoStaged() {
		return isAutoStaged;
	}
	
	public void setAutoStaged(boolean isAutoStaged) {
		this.isAutoStaged = isAutoStaged;
	}

	public void setFkColumns(List<ElementSymbol> fkColumns) {
		this.fkColumns = fkColumns;
	}

	public List<ElementSymbol> getFkColumns() {
		return this.fkColumns;
	}
}
