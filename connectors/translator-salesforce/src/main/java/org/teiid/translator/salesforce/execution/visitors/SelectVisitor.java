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
package org.teiid.translator.salesforce.execution.visitors;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.teiid.language.AggregateFunction;
import org.teiid.language.ColumnReference;
import org.teiid.language.DerivedColumn;
import org.teiid.language.Expression;
import org.teiid.language.Limit;
import org.teiid.language.NamedTable;
import org.teiid.language.Select;
import org.teiid.metadata.AbstractMetadataRecord;
import org.teiid.metadata.Column;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.metadata.Table;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.salesforce.Constants;
import org.teiid.translator.salesforce.SalesForcePlugin;
import org.teiid.translator.salesforce.Util;


public class SelectVisitor extends CriteriaVisitor implements IQueryProvidingVisitor {

	private Map<Integer, Column> selectSymbolIndexToElement = new HashMap<Integer, Column>();
	private Map<String, Integer> selectSymbolNameToIndex = new HashMap<String, Integer>();
	private int selectSymbolCount;
	private int idIndex = -1; // index of the ID select symbol.
	protected List<DerivedColumn> selectSymbols;
	protected StringBuffer limitClause = new StringBuffer();
	private Boolean objectSupportsRetrieve;
	
	public SelectVisitor(RuntimeMetadata metadata) {
		super(metadata);
	}

	public void visit(Select query) {
		super.visit(query);
		if (query.isDistinct()) {
			exceptions.add(new TranslatorException(SalesForcePlugin.Util.getString("SelectVisitor.distinct.not.supported"))); //$NON-NLS-1$
		}
		selectSymbols = query.getDerivedColumns();
		selectSymbolCount = selectSymbols.size();
		Iterator<DerivedColumn> symbolIter = selectSymbols.iterator();
		int index = 0;
		while (symbolIter.hasNext()) {
			DerivedColumn symbol = symbolIter.next();
			// get the name in source
			Expression expression = symbol.getExpression();
			if (expression instanceof ColumnReference) {
				Column element = ((ColumnReference) expression).getMetadataObject();
				selectSymbolIndexToElement.put(index, element);
				String qualifiedName = element.getParent().getNameInSource() + ':' + element.getNameInSource();
				selectSymbolNameToIndex .put(qualifiedName, index);
				String nameInSource = element.getNameInSource();
				if (null == nameInSource || nameInSource.length() == 0) {
					exceptions.add(new TranslatorException("name in source is null or empty for column "+ symbol.toString())); //$NON-NLS-1$
					continue;
				}
				if (nameInSource.equalsIgnoreCase("id")) { //$NON-NLS-1$
					idIndex = index;
				}
			}
			++index;
		}
	}
	
	@Override
	public void visit(NamedTable obj) {
		try {
			table = obj.getMetadataObject();
	        String supportsQuery = table.getProperties().get(Constants.SUPPORTS_QUERY);
	        objectSupportsRetrieve = Boolean.valueOf(table.getProperties().get(Constants.SUPPORTS_RETRIEVE));
	        if (!Boolean.valueOf(supportsQuery)) {
	            throw new TranslatorException(table.getNameInSource() + " " + SalesForcePlugin.Util.getString("CriteriaVisitor.query.not.supported")); //$NON-NLS-1$ //$NON-NLS-2$
	        }
			loadColumnMetadata(obj);
		} catch (TranslatorException ce) {
			exceptions.add(ce);
		}
	}
	
	@Override
	public void visit(Limit obj) {
		super.visit(obj);
		limitClause.append(LIMIT).append(SPACE).append(obj.getRowLimit());
	}
	
	/*
	 * The SOQL SELECT command uses the following syntax: SELECT fieldList FROM
	 * objectType [WHERE The Condition Expression (WHERE Clause)] [ORDER BY]
	 * LIMIT ?
	 */

	public String getQuery() throws TranslatorException {
		if (!exceptions.isEmpty()) {
			throw exceptions.get(0);
		}
		StringBuffer result = new StringBuffer();
		result.append(SELECT).append(SPACE);
		addSelectSymbols(result);
		result.append(SPACE);
		result.append(FROM).append(SPACE);
		result.append(table.getNameInSource()).append(SPACE);
		addCriteriaString(result);
		//result.append(orderByClause).append(SPACE);
		result.append(limitClause);
		Util.validateQueryLength(result);
		return result.toString();
	}

	private void addSelectSymbols(StringBuffer result) throws TranslatorException {
		boolean firstTime = true;
		for (DerivedColumn symbol : selectSymbols) {
			if (!firstTime) {
				result.append(", "); //$NON-NLS-1$
			} else {
				firstTime = false; 
			}
			Expression expression = symbol.getExpression();
			if (expression instanceof ColumnReference) {
				Column element = ((ColumnReference) expression).getMetadataObject();
				AbstractMetadataRecord parent = element.getParent();
				Table table;
				if(parent instanceof Table) {
					table = (Table)parent;
				} else {
					parent = parent.getParent();
					if(parent instanceof Table) {
						table = (Table)parent;
					} else {
						throw new TranslatorException("Could not resolve Table for column " + element.getName()); //$NON-NLS-1$
					}
				}
				result.append(table.getNameInSource());
				result.append('.');
				result.append(element.getNameInSource());
			} else if (expression instanceof AggregateFunction) {
				result.append("count()"); //$NON-NLS-1$
			}
		}
	}


	public int getSelectSymbolCount() {
		return selectSymbolCount;
	}

	public Column getSelectSymbolMetadata(int index) {
		return selectSymbolIndexToElement.get(index);
	}
	
	public Column getSelectSymbolMetadata(String name) {
		Column result = null;
		Integer index = selectSymbolNameToIndex.get(name);
		if(null != index) {  
			result = selectSymbolIndexToElement.get(index);
		} 
		return result; 
	}
	
	public Integer getSelectSymbolIndex(String name) {
		return selectSymbolNameToIndex.get(name);
	}
	
	/**
	 * Returns the index of the ID column.
	 * @return the index of the ID column, -1 if there is no ID column.
	 */
	public int getIdIndex() {
		return idIndex;
	}


	public Boolean getQueryAll() {
		return queryAll;
	}


	public String getRetrieveFieldList() throws TranslatorException {
		assertRetrieveValidated();
		StringBuffer result = new StringBuffer();
		addSelectSymbols(result);
		return result.toString();
	}


	public List<String> getIdInCriteria() {
		assertRetrieveValidated();
		List<Expression> expressions = this.idInCriteria.getRightExpressions();
		List<String> result = new ArrayList<String>(expressions.size());
		for(int i = 0; i < expressions.size(); i++) {
			result.add(getValue(expressions.get(i), true));
		}      
		return result;
	}

	private void assertRetrieveValidated() throws AssertionError {
		if(!hasOnlyIDCriteria()) {
			throw new AssertionError("Must call hasOnlyIdInCriteria() before this method"); //$NON-NLS-1$
		}
	}

	public boolean hasOnlyIdInCriteria() {
		return hasOnlyIDCriteria() && idInCriteria != null;
	}
	
	public boolean canRetrieve() {
		return objectSupportsRetrieve && hasOnlyIDCriteria();
	}

}
