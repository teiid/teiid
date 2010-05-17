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


package org.teiid.translator.xml;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.teiid.language.ColumnReference;
import org.teiid.language.DerivedColumn;
import org.teiid.language.Expression;
import org.teiid.language.Literal;
import org.teiid.language.NamedTable;
import org.teiid.language.Select;
import org.teiid.language.TableReference;
import org.teiid.metadata.Column;
import org.teiid.metadata.Table;
import org.teiid.translator.TranslatorException;

public class QueryAnalyzer {

    private Select command;
    private Table table;
    private ExecutionInfo executionInfo;

    public QueryAnalyzer(Select query) throws TranslatorException {
    	this.command = query;
        executionInfo = new ExecutionInfo();
        analyze();
    }

	public final void analyze() throws TranslatorException {
    	setGroupInfo();
        setRequestedColumns();
        setParametersAndCriteria();
        setProperties();
    }



    public ExecutionInfo getExecutionInfo() {
        return this.executionInfo;
    }

    private void setGroupInfo() throws TranslatorException {
        List<TableReference> fromItems = command.getFrom();
        //Can only be one because we do not support joins
        NamedTable group = (NamedTable) fromItems.get(0);
        this.table = group.getMetadataObject();
        this.executionInfo.setTableXPath(table.getNameInSource());
    }

	private void setRequestedColumns() throws TranslatorException {

        List<OutputXPathDesc> columns = new ArrayList<OutputXPathDesc>();
        //get the request items
        List<DerivedColumn> selectSymbols = this.command.getDerivedColumns();
        
        //setup column numbers
        int projectedColumnCount = 0;

        //add projected fields into XPath array and element array for later
        // lookup
        
       for(DerivedColumn selectSymbol : selectSymbols) {
            Expression expr = selectSymbol.getExpression();
            OutputXPathDesc xpath = null;

            //build the appropriate structure
                if (expr instanceof Literal) {
                    xpath = new OutputXPathDesc((Literal) expr);
                } else if (expr instanceof ColumnReference) {
                    Column element = ((ColumnReference)expr).getMetadataObject();
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
        this.executionInfo.setColumnCount(projectedColumnCount);
        this.executionInfo.setRequestedColumns(columns);
    }

    private void setParametersAndCriteria() throws TranslatorException {
        //Build a linked list of parameters, and a linked list of equivilence
        // and set selection criteria.
        //Each parameter and criteria is itself represented by a linked list,
        //  containing names, element (metadata), and equivilence value, or all
        // set values

        ArrayList<CriteriaDesc> params = new ArrayList<CriteriaDesc>();
        ArrayList<CriteriaDesc> crits = new ArrayList<CriteriaDesc>();
        ArrayList<CriteriaDesc> responses = new ArrayList<CriteriaDesc>();
        ArrayList<CriteriaDesc> locations = new ArrayList<CriteriaDesc>();

        //Iterate through each field in the table
        for (Column element : table.getColumns()) {
            CriteriaDesc criteria = CriteriaDesc.getCriteriaDescForColumn(
                    element, command);
            if (criteria != null) {
                mapCriteriaToColumn(criteria, params, crits, responses, locations);
            }
        }
        this.executionInfo.setParameters(params);
        this.executionInfo.setCriteria(crits);
        
        String location = null;
        for (Iterator<CriteriaDesc> iter = locations.iterator(); iter.hasNext(); ) {
            Object o = iter.next();
            CriteriaDesc crtierion = (CriteriaDesc)o;
            List values = crtierion.getValues();
            for (Iterator valuesIter = values.iterator(); valuesIter.hasNext(); ) {
                Object oValue = valuesIter.next();
                String value = (String)oValue;
                if (location != null) {
                    if (!(location.equals(value))) {
                        throw new TranslatorException(XMLPlugin
                                .getString("QueryAnalyzer.multiple.locations.supplied")); //$NON-NLS-1$
                    }
                }
                location = value;   
            }
        }
        this.executionInfo.setLocation(location);
    }

    private void mapCriteriaToColumn(CriteriaDesc criteria, ArrayList<CriteriaDesc> params,
            ArrayList<CriteriaDesc> crits, ArrayList<CriteriaDesc> responses, ArrayList<CriteriaDesc> locations) throws TranslatorException {
        int totalColumnCount = this.executionInfo.getColumnCount();
        //check each criteria to see which projected column it maps to
        String criteriaColName = criteria.getColumnName();
        boolean matchedField = false;
        OutputXPathDesc xpath = null;
        for (int j = 0; j < this.executionInfo.getRequestedColumns().size(); j++) {
            xpath = (OutputXPathDesc) this.executionInfo.getRequestedColumns().get(j);
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
                this.executionInfo.getRequestedColumns().add(xpath);
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

    private void setProperties() throws TranslatorException {
    	this.executionInfo.setOtherProperties(table.getProperties());
    }

	public List<CriteriaDesc[]> getRequestPerms() {
		return RequestGenerator.getRequests(this.executionInfo.getParameters());
	}

}
