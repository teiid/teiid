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

package com.metamatrix.connector.xml.base;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

/**
 * @author JChoate
 *
 */
public class ExecutionInfo {
// TODO: Massivly refactor.  This class was defined when the multiple reqests to an XML service
// (read HTTP) to satisfy an IN clause were abstacted beneath a single call to Executor.getResult().
// This case is now satified within the execute call by making a single call to Executor.getResult()
// for each paramter of the IN clause.
	
    private List m_columns;
    private int m_columnCount;
    private List m_params;
    private List m_criteria;
    private Properties m_otherProps;
    private String m_tablePath;
    private String m_location;
    
    public ExecutionInfo() { 
        m_columnCount = 0;
        m_columns = new ArrayList();
        m_params = new ArrayList();
        m_criteria = new ArrayList();
        m_otherProps = new Properties();
        m_tablePath = new String(""); //$NON-NLS-1$
    }
    
    
    public String getTableXPath() {
        return m_tablePath;   
    }
    
    public String getLocation()
    {
        return m_location;   
    }
    
    public List getRequestedColumns() {
        return m_columns;   
    }
    
    
    public int getColumnCount() {
     return m_columnCount;   
    }
    
    
    public List getParameters() {
        return m_params;
    }
    
    
    public List getCriteria() {
        return m_criteria;
    }
    

    public Properties getOtherProperties() {
        return m_otherProps;
    }

    public void setTableXPath(String path)  {
        if (path == null || path.trim().length() == 0) {
         m_tablePath = null;   
        } else {
        	m_tablePath = path;
        }
    }
    
    public void setLocation(String location)
    {
        m_location = location;   
    }
    
    public void setRequestedColumns(List columns) {
        m_columns = columns;   
    }
    
    
    public void setColumnCount(int count) {
      m_columnCount = count;   
    }
    
    
    public void setParameters(List params) {
        m_params = params;
    }
    
    
    public void setCriteria(List criteria) {
        m_criteria = criteria;
    }
    

    public void setOtherProperties(Properties props) {
    	if (props == null) {
    		m_otherProps = new Properties();
    	} else {
    		m_otherProps = props;	
    	}        
    }

	// It is not enforced that there is only one ResponseId in the parameters, but it is
	// a valid assumption and the modelgenerators will never create a model that has more 
	// than one.  We could enforce this with a real metamodel.
	public CriteriaDesc getResponseIDCriterion() {
		CriteriaDesc result = null;
		Iterator iter = m_params.iterator();
		while(iter.hasNext()) {
			CriteriaDesc criterion = (CriteriaDesc)iter.next();
			if(criterion.isResponseId()) {
				result = criterion;
				break;
			}
		}
		return result;
	}    
}
