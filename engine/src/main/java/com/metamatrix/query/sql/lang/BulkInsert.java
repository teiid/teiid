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

package com.metamatrix.query.sql.lang;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.core.util.EquivalenceUtil;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.sql.LanguageVisitor;
import com.metamatrix.query.sql.symbol.ElementSymbol;
import com.metamatrix.query.sql.symbol.Expression;
import com.metamatrix.query.sql.symbol.GroupSymbol;

/**
 * Bulk insert is variation of Insert command, where a single table is being 
 * inserted with multiple values. The purpose of this class from Insert is to
 * minimize the memory footprint and avoid multiple planning and performace.
 */
public class BulkInsert extends Insert {
    private List rowValues = null;

    /**
     * Default ctor
     * @param group - Group Name
     * @param variables - Column Names
     */    
    public BulkInsert(GroupSymbol group, List variables, List rows){
        super(group,variables, null);
        this.rowValues = rows;
    }

    /**
     * Default ctor
     * @param group - Group Name
     * @param variables - Column Names
     */
    public BulkInsert(GroupSymbol group, List variables){
        super(group,variables, null);
    }
    
    /**
     * Get the list of rows of column values.  
     * @return List(row)-->list of column values; never null
     */
    public List getRows() {
        if (rowValues == null) {
            return Collections.EMPTY_LIST;    
        }
        return rowValues;
        
    }

    /**
     * Get the list of rows of column values.  
     * (row)-->list of column values
     */
    public void setRows(List rows) {
        rowValues = rows;
    }
    
    /**
     * @see com.metamatrix.query.sql.lang.Insert#addValue(com.metamatrix.query.sql.symbol.Expression)
     */
    public void addValue(Expression value) {
        throw new UnsupportedOperationException("This operation not allowed in BulkInsert"); //$NON-NLS-1$
    }
    /**
     * @see com.metamatrix.query.sql.lang.Insert#getValues()
     */
    public List getValues() {
        throw new UnsupportedOperationException("This operation not allowed in BulkInsert"); //$NON-NLS-1$    
    }
    /**
     * @see com.metamatrix.query.sql.lang.Insert#setValues(java.util.List)
     */
    public void setValues(List values) {
        throw new UnsupportedOperationException("This operation not allowed in BulkInsert"); //$NON-NLS-1$
    }
    
    /**
     * @see java.lang.Object#clone()
     */
    public Object clone() {
	    GroupSymbol group = getGroup();
	    GroupSymbol copyGroup = null;
	    if(group != null) { 
	    	copyGroup = (GroupSymbol) group.clone();    
	    }
	    
	    List copyVars = null;
	    if(getVariables() != null) { 
	    	copyVars = new ArrayList(getVariables().size());
	    	Iterator iter = getVariables().iterator();
	    	while(iter.hasNext()) { 
	    		ElementSymbol element = (ElementSymbol) iter.next();
	    		copyVars.add( element.clone() );    
	    	}    
	    }
	    	    
	    // now make a new copy of the original
	    BulkInsert copy = new BulkInsert(copyGroup, copyVars);
	    copy.rowValues = getRows();	    
        copyMetadataState(copy);
		return copy;        
    }

    /**
     * @see java.lang.Object#equals(java.lang.Object)
     */
    public boolean equals(Object obj) {
    	// Quick same object test
    	if(this == obj) {
    		return true;
		}

		// Quick fail tests
    	if(!(obj instanceof BulkInsert)) {
    		return false;
		}

    	BulkInsert other = (BulkInsert) obj;
        
        return EquivalenceUtil.areEqual(getGroup(), other.getGroup()) &&               
               EquivalenceUtil.areEqual(getVariables(), other.getVariables()) &&
               EquivalenceUtil.areEqual(getRows(), other.getRows());        
    }
    
    public void acceptVisitor(LanguageVisitor visitor) {
        visitor.visit(this);
    }   
    
    /** 
     * @see com.metamatrix.query.sql.lang.PreparedBatchUpdate#updatingModelCount(com.metamatrix.query.metadata.QueryMetadataInterface)
     */
    public int updatingModelCount(QueryMetadataInterface metadata) throws MetaMatrixComponentException {
        return 2; // return 2 since we may be multibatch
    }
}
