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

package org.teiid.query.processor.relational;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import net.sf.saxon.om.Item;
import net.sf.saxon.om.SequenceIterator;
import net.sf.saxon.sxpath.XPathDynamicContext;
import net.sf.saxon.sxpath.XPathExpression;
import net.sf.saxon.trans.XPathException;
import net.sf.saxon.value.Value;

import org.teiid.api.exception.query.ExpressionEvaluationException;
import org.teiid.common.buffer.BlockedException;
import org.teiid.common.buffer.TupleBatch;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.XMLType;
import org.teiid.query.execution.QueryExecPlugin;
import org.teiid.query.sql.lang.XMLTable;
import org.teiid.query.sql.lang.XMLTable.XMLColumn;

/**
 * Handles xml table processing.
 */
public class XMLTableNode extends SubqueryAwareRelationalNode {

	private XMLTable table;
	private List<XMLColumn> projectedColumns;
	
	private SequenceIterator result;
	private int rowCount = 0;
	private Item item;
	
	public XMLTableNode(int nodeID) {
		super(nodeID);
	}
	
	@Override
	public void closeDirect() {
		super.closeDirect();
		reset();
	}
	
	@Override
	public void reset() {
		super.reset();
		if (this.result != null) {
			result.close();
			result = null;
		}
		item = null;
		rowCount = 0;
	}
	
	public void setTable(XMLTable table) {
		this.table = table;
	}
	
	public void setProjectedColumns(List<XMLColumn> projectedColumns) {
		this.projectedColumns = projectedColumns;
	}
	
	@Override
	public XMLTableNode clone() {
		XMLTableNode clone = new XMLTableNode(getID());
		this.copy(this, clone);
		clone.setTable(table);
		clone.setProjectedColumns(projectedColumns);
		return clone;
	}

	@Override
	protected TupleBatch nextBatchDirect() throws BlockedException,
			TeiidComponentException, TeiidProcessingException {
		
		if (result == null) {
			setReferenceValues(this.table);
			result = getEvaluator(Collections.emptyMap()).evaluateXQuery(this.table.getXQueryExpression(), this.table.getPassing(), null);
		}
		
		while (!isBatchFull() && !isLastBatch()) {
			processRow();
		}
		return pullBatch();
	}

	private void processRow() throws ExpressionEvaluationException, BlockedException,
			TeiidComponentException, TeiidProcessingException {
		if (item == null) {
			try {
				item = result.next();
			} catch (XPathException e) {
				throw new TeiidProcessingException(e, QueryExecPlugin.Util.getString("XMLTableNode.error", e.getMessage())); //$NON-NLS-1$
			}
			rowCount++;
			if (item == null) {
				terminateBatches();
				return;
			}
		}
		List<Object> tuple = new ArrayList<Object>(projectedColumns.size());
		for (XMLColumn proColumn : projectedColumns) {
			if (proColumn.isOrdinal()) {
				tuple.add(rowCount);
			} else {
				try {
					XPathExpression path = proColumn.getPathExpression();
					XPathDynamicContext dynamicContext = path.createDynamicContext(item);
					SequenceIterator pathIter = path.iterate(dynamicContext);
					Item colItem = pathIter.next();
					if (colItem == null) {
						if (proColumn.getDefaultExpression() != null) {
							tuple.add(getEvaluator(Collections.emptyMap()).evaluate(proColumn.getDefaultExpression(), null));
						} else {
							tuple.add(null);
						}
						continue;
					}
					if (proColumn.getSymbol().getType() == DataTypeManager.DefaultDataClasses.XML) {
						XMLType value = table.getXQueryExpression().createXMLType(pathIter.getAnother(), this.getBufferManager(), false);
						tuple.add(value);
						continue;
					}
					if (pathIter.next() != null) {
						throw new TeiidProcessingException(QueryExecPlugin.Util.getString("XMLTableName.multi_value", proColumn.getName())); //$NON-NLS-1$
					}
					Object value = Value.convertToJava(colItem);
					if (value instanceof Item) {
						value = ((Item)value).getStringValue();
					}
					value = DataTypeManager.convertToRuntimeType(value);
					value = DataTypeManager.transformValue(value, proColumn.getSymbol().getType());
					tuple.add(value);
				} catch (XPathException e) {
					throw new TeiidProcessingException(e, QueryExecPlugin.Util.getString("XMLTableNode.path_error", proColumn.getName())); //$NON-NLS-1$
				}
			}
		}
		item = null;
		addBatchRow(tuple);
	}
		
}