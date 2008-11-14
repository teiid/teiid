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

import com.metamatrix.connector.xml.IQueryPreprocessor;
import com.metamatrix.data.api.ConnectorEnvironment;
import com.metamatrix.data.api.ConnectorLogger;
import com.metamatrix.data.api.ExecutionContext;
import com.metamatrix.data.exception.ConnectorException;
import com.metamatrix.data.language.IElement;
import com.metamatrix.data.language.IExpression;
import com.metamatrix.data.language.IFrom;
import com.metamatrix.data.language.IGroup;
import com.metamatrix.data.language.ILiteral;
import com.metamatrix.data.language.IQuery;
import com.metamatrix.data.language.ISelect;
import com.metamatrix.data.language.ISelectSymbol;
import com.metamatrix.data.metadata.runtime.Element;
import com.metamatrix.data.metadata.runtime.Group;
import com.metamatrix.data.metadata.runtime.MetadataID;
import com.metamatrix.data.metadata.runtime.RuntimeMetadata;

/**
 * @author JChoate
 *  
 */
public class QueryAnalyzer {

    private IQuery m_query;
    
    private IQueryPreprocessor preprocessor;

    private RuntimeMetadata m_metadata;

    private Group m_table;

    private ExecutionInfo m_info;

	private ConnectorLogger logger;

	private ConnectorEnvironment connectorEnv;

	private ExecutionContext exeContext;
    
    public QueryAnalyzer(IQuery query, RuntimeMetadata metadata, IQueryPreprocessor preprocessor, 
    		ConnectorLogger logger, ExecutionContext exeContext, ConnectorEnvironment connectorEnv) throws ConnectorException {
        setMetaData(metadata);
        setQuery(query);
        setPreprocessor(preprocessor);
        setLogger(logger);
        setExecutionContext(exeContext);
        setConnectorEnvironment(connectorEnv);
        m_info = new ExecutionInfo();
        analyze();
    }

    private void setConnectorEnvironment(ConnectorEnvironment connectorEnv) {
		this.connectorEnv = connectorEnv;
	}

	private void setExecutionContext(ExecutionContext exeContext) {
		this.exeContext = exeContext;
	}

	private void setLogger(ConnectorLogger logger) {
		this.logger = logger;
	}

	private void setPreprocessor(IQueryPreprocessor preprocessor) {
		this.preprocessor = preprocessor;
	}

	public final void analyze() throws ConnectorException {
    	IQuery newQuery = preprocessor.preprocessQuery(m_query, m_metadata, exeContext, connectorEnv, logger);
    	logger.logTrace("XML Connector Framework: executing command: " + newQuery);
    	setQuery(newQuery);
    	setTable();
        setRequestedColumns();
        setParametersAndCriteria();
        setOtherProperties();
    }

    private void setMetaData(RuntimeMetadata metadata) {
        m_metadata = metadata;
    }

    private void setQuery(IQuery query) {
        m_query = query;
    }

    public ExecutionInfo getExecutionInfo() {
        return m_info;
    }

    private void setTable() throws ConnectorException {
        IFrom from = m_query.getFrom();
        List fromItems = from.getItems();
        //better be only one
        IGroup group = (IGroup) fromItems.get(0);
        MetadataID id = group.getMetadataID();
        m_table = (Group) m_metadata.getObject(id);
        m_info.setTableXPath(m_table.getNameInSource());
    }

    private void setRequestedColumns() throws ConnectorException {

        ArrayList columns = new ArrayList();
        //get the request items
        ISelect select = m_query.getSelect();
        List selectSymbols = select.getSelectSymbols();
        Iterator symbolsIterator = selectSymbols.iterator();

        //setup column numbers
        int projectedColumnCount = 0;

        //add projected fields into XPath array and element array for later
        // lookup
        while (symbolsIterator.hasNext()) {
            ISelectSymbol selectSymbol = (ISelectSymbol) symbolsIterator.next();
            IExpression expr = selectSymbol.getExpression();
            OutputXPathDesc xpath = null;

            //build the appropriate structure
                if (expr instanceof ILiteral) {
                    xpath = new OutputXPathDesc((ILiteral) expr);
                } else if (expr instanceof IElement) {
                    MetadataID elementID = (MetadataID) ((IElement) expr)
                            .getMetadataID();
                    Element element = (Element) m_metadata.getObject(elementID);
                    xpath = new OutputXPathDesc(element);
                }
                if (xpath != null) {
                	xpath.setColumnNumber(projectedColumnCount);
                }

            // put xpath object into linked list
            columns.add(xpath);
            ++projectedColumnCount;
        }

        //set the column count
        m_info.setColumnCount(projectedColumnCount);
        m_info.setRequestedColumns(columns);
    }

    private void setParametersAndCriteria() throws ConnectorException {
        //Build a linked list of parameters, and a linked list of equivilence
        // and set selection criteria.
        //Each parameter and criteria is itself represented by a linked list,
        //  containing names, element (metadata), and equivilence value, or all
        // set values

        MetadataID groupID = m_table.getMetadataID();
        List elementList = groupID.getChildIDs();

        ArrayList params = new ArrayList();
        ArrayList crits = new ArrayList();
        ArrayList responses = new ArrayList();
        ArrayList locations = new ArrayList();

        //Iterate through each field in the table
        Iterator elementListIterator = elementList.iterator();
        while (elementListIterator.hasNext()) {
            MetadataID elementID = (MetadataID) elementListIterator.next();
            Element element = (Element) m_metadata.getObject(elementID);
            CriteriaDesc criteria = CriteriaDesc.getCriteriaDescForColumn(
                    element, m_query);
            if (criteria != null) {
                mapCriteriaToColumn(criteria, params, crits, responses, locations);
            }
        }
        m_info.setParameters(params);
        m_info.setCriteria(crits);
        
        String location = null;
        for (Iterator iter = locations.iterator(); iter.hasNext(); ) {
            Object o = iter.next();
            CriteriaDesc crtierion = (CriteriaDesc)o;
            ArrayList values = crtierion.getValues();
            for (Iterator valuesIter = values.iterator(); valuesIter.hasNext(); ) {
                Object oValue = valuesIter.next();
                String value = (String)oValue;
                if (location != null) {
                    if (!(location.equals(value))) {
                        throw new ConnectorException(Messages
                                .getString("QueryAnalyzer.multiple.locations.supplied")); //$NON-NLS-1$
                    }
                }
                location = value;   
            }
        }
        m_info.setLocation(location);
    }

    private void mapCriteriaToColumn(CriteriaDesc criteria, ArrayList params,
            ArrayList crits, ArrayList responses, ArrayList locations) throws ConnectorException {
        int totalColumnCount = m_info.getColumnCount();
        //check each criteria to see which projected column it maps to
        String criteriaColName = criteria.getColumnName();
        boolean matchedField = false;
        OutputXPathDesc xpath = null;
        for (int j = 0; j < m_info.getRequestedColumns().size(); j++) {
            xpath = (OutputXPathDesc) m_info
                    .getRequestedColumns().get(j);
            String projColName = xpath.getColumnName();
            if (criteriaColName.equals(projColName)) {
                matchedField = true;
                criteria.setColumnNumber(j);
                break;
            }
        }
        if (criteria.isParameter() || criteria.isResponseId() && !criteria.isLocation()) {
            params.add(criteria);
            if (criteria.isResponseId()) {
                responses.add(criteria);
            }
        } else {

            // otherwise add a new column to the projected XPath list (don't
            // worry,
            // it will be removed later and not really projected.
            // match not found, add to project list
            if (!matchedField) {
                criteria.setColumnNumber(totalColumnCount);
                xpath = new OutputXPathDesc(criteria
                        .getElement());
                xpath.setColumnNumber(totalColumnCount++);
                m_info.getRequestedColumns().add(xpath);
            }
            if (xpath.isResponseId()) {
                responses.add(criteria);
            }
            else if (xpath.isLocation()) {
                locations.add(criteria);
            }
            else {
            	crits.add(criteria);
            }
        }
    }

    private void setOtherProperties() throws ConnectorException {
        m_info.setOtherProperties(m_table.getProperties());
    }

	public List getRequestPerms() {
		return RequestGenerator.getRequestPerms(m_info.getParameters());
	}

}
