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
package com.metamatrix.connector.salesforce.execution.visitors;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.language.IAggregate;
import org.teiid.connector.language.IElement;
import org.teiid.connector.language.IExpression;
import org.teiid.connector.language.IFrom;
import org.teiid.connector.language.IGroup;
import org.teiid.connector.language.IQuery;
import org.teiid.connector.language.ISelect;
import org.teiid.connector.language.ISelectSymbol;
import org.teiid.connector.metadata.runtime.Element;
import org.teiid.connector.metadata.runtime.RuntimeMetadata;

import com.metamatrix.connector.salesforce.Messages;
import com.metamatrix.connector.salesforce.Util;

public class SelectVisitor extends CriteriaVisitor implements IQueryProvidingVisitor {

	private Map<Integer, Element> selectSymbolIndexToElement = new HashMap<Integer, Element>();
	private Map<String, Integer> selectSymbolNameToIndex = new HashMap<String, Integer>();
	private int selectSymbolCount;
	private int idIndex = -1; // index of the ID select symbol.
	private StringBuffer selectSymbols = new StringBuffer();
	private StringBuffer limitClause = new StringBuffer();
	//private StringBuffer orderByClause = new StringBuffer();

	public SelectVisitor(RuntimeMetadata metadata) {
		super(metadata);
	}

	
	public void visit(IQuery query) {
		super.visit(query);
		if(null != query.getLimit()) {
			limitClause.append(LIMIT).append(SPACE).append(query.getLimit().getRowLimit());
		}
	}


	public void visit(ISelect select) {
		super.visit(select);
		if (select.isDistinct()) {
			exceptions.add(new ConnectorException(
					Messages.getString("SelectVisitor.distinct.not.supported")));
		}
		try {
			List<ISelectSymbol> symbols = select.getSelectSymbols();
			selectSymbolCount = symbols.size();
			Iterator<ISelectSymbol> symbolIter = symbols.iterator();
			boolean firstTime = true;
			int index = 0;
			while (symbolIter.hasNext()) {
				ISelectSymbol symbol = symbolIter.next();
				// get the name in source
				IExpression expression = symbol.getExpression();
				if (expression instanceof IElement) {
					Element element = ((IElement) expression).getMetadataObject();
					selectSymbolIndexToElement.put(index, element);
					selectSymbolNameToIndex .put(element.getNameInSource(), index);
					String nameInSource = element.getNameInSource();
					if (null == nameInSource || nameInSource.length() == 0) {
						exceptions.add(new ConnectorException(
								"name in source is null or empty for column "
										+ symbol.toString()));
						continue;
					}
					if (nameInSource.equalsIgnoreCase("id")) {
						idIndex = index;
					}
					if (!firstTime) {
						selectSymbols.append(", ");
					} else {
						firstTime = false;
					}
					selectSymbols.append(nameInSource);
				} else if (expression instanceof IAggregate) {
					selectSymbols.append("count()"); //$NON-NLS-1$
				}
				++index;
			}
		} catch (ConnectorException ce) {
			exceptions.add(ce);
		}
	}

	@Override
	public void visit(IFrom from) {
		super.visit(from);
		try {
			IGroup group = (IGroup) from.getItems().get(0);
			loadColumnMetadata(group);
		} catch (ConnectorException ce) {
			exceptions.add(ce);
		}
	}

/*
	@Override
	public void visit(IOrderBy orderBy) {
		super.visit(orderBy);
		List items = orderBy.getItems();
		if(items.size() > 1) {
			exceptions.add(new ConnectorException("Salesforce cannot support more than one item in the ORDER BY clause"));
		}
		StringBuffer result = new StringBuffer();
		result.append(ORDER_BY).append(SPACE);
		result.append(items.get(0));
		orderByClause = result;
	}
*/
	/*
	 * The SOQL SELECT command uses the following syntax: SELECT fieldList FROM
	 * objectType [WHERE The Condition Expression (WHERE Clause)] [ORDER BY]
	 * LIMIT ?
	 */

	public String getQuery() throws ConnectorException {
		if (!exceptions.isEmpty()) {
			throw ((ConnectorException) exceptions.get(0));
		}
		StringBuffer result = new StringBuffer();
		result.append(SELECT).append(SPACE);
		result.append(selectSymbols).append(SPACE);
		result.append(FROM).append(SPACE);
		result.append(table.getNameInSource()).append(SPACE);
		addCriteriaString(result);
		//result.append(orderByClause).append(SPACE);
		result.append(limitClause);
		Util.validateQueryLength(result);
		return result.toString();
	}

	public int getSelectSymbolCount() {
		return selectSymbolCount;
	}

	public Element getSelectSymbolMetadata(int index) {
		return selectSymbolIndexToElement.get(index);
	}
	
	public Element getSelectSymbolMetadata(String name) {
		Element result = null;
		Integer index = selectSymbolNameToIndex.get(name);
		if(null != index) {  
			result = selectSymbolIndexToElement.get(index);
		} 
		return result; 
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

}
