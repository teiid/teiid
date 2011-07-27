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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.sf.saxon.om.Item;
import net.sf.saxon.om.NodeInfo;
import net.sf.saxon.om.SequenceIterator;
import net.sf.saxon.sxpath.XPathDynamicContext;
import net.sf.saxon.sxpath.XPathExpression;
import net.sf.saxon.trans.XPathException;
import net.sf.saxon.type.BuiltInAtomicType;
import net.sf.saxon.type.ConversionResult;
import net.sf.saxon.value.AtomicValue;
import net.sf.saxon.value.StringValue;
import net.sf.saxon.value.Value;

import org.teiid.api.exception.query.ExpressionEvaluationException;
import org.teiid.common.buffer.BlockedException;
import org.teiid.common.buffer.TupleBatch;
import org.teiid.common.buffer.TupleBuffer;
import org.teiid.common.buffer.BufferManager.TupleSourceType;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.XMLType;
import org.teiid.query.QueryPlugin;
import org.teiid.query.function.FunctionDescriptor;
import org.teiid.query.sql.lang.XMLTable;
import org.teiid.query.sql.lang.XMLTable.XMLColumn;
import org.teiid.query.xquery.saxon.SaxonXQueryExpression.Result;
import org.teiid.query.xquery.saxon.SaxonXQueryExpression.RowProcessor;

/**
 * Handles xml table processing.
 * 
 * When streaming the results will be fully built and stored in a buffer
 * before being returned
 */
public class XMLTableNode extends SubqueryAwareRelationalNode implements RowProcessor {

	private static Map<Class<?>, BuiltInAtomicType> typeMapping = new HashMap<Class<?>, BuiltInAtomicType>();
	
	static {
		typeMapping.put(DataTypeManager.DefaultDataClasses.TIMESTAMP, BuiltInAtomicType.DATE_TIME);
		typeMapping.put(DataTypeManager.DefaultDataClasses.TIME, BuiltInAtomicType.TIME);
		typeMapping.put(DataTypeManager.DefaultDataClasses.DATE, BuiltInAtomicType.DATE);
		typeMapping.put(DataTypeManager.DefaultDataClasses.FLOAT, BuiltInAtomicType.FLOAT);
		typeMapping.put(DataTypeManager.DefaultDataClasses.DOUBLE, BuiltInAtomicType.DOUBLE);
	}
	
	private XMLTable table;
	private List<XMLColumn> projectedColumns;
	
	private Result result;
	private int rowCount = 0;
	private Item item;
	
	private TupleBuffer buffer;
	private int outputRow = 1;
	private boolean usingOutput;
	
	public XMLTableNode(int nodeID) {
		super(nodeID);
	}
	
	@Override
	public void closeDirect() {
		super.closeDirect();
		if(this.buffer != null) {
    		this.buffer.remove();
        	this.buffer = null;
        }
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
		outputRow = 1;
		usingOutput = false;
		this.buffer = null;
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
		
		evaluate();
		
		if (this.table.getXQueryExpression().isStreaming()) {
			TupleBatch batch = this.buffer.getBatch(outputRow);
			outputRow = batch.getEndRow() + 1;
			return batch;
		}
		
		while (!isBatchFull() && !isLastBatch()) {
			if (item == null) {
				try {
					item = result.iter.next();
				} catch (XPathException e) {
					throw new TeiidProcessingException(e, QueryPlugin.Util.getString("XMLTableNode.error", e.getMessage())); //$NON-NLS-1$
				}
				rowCount++;
				if (item == null) {
					terminateBatches();
					break;
				}
			}
			addBatchRow(processRow());
		}
		return pullBatch();
	}

	private void evaluate() throws TeiidComponentException,
			ExpressionEvaluationException, BlockedException,
			TeiidProcessingException {
		if (result == null) {
			if (this.buffer == null && this.table.getXQueryExpression().isStreaming()) {
				this.buffer = this.getBufferManager().createTupleBuffer(getOutputElements(), getConnectionID(), TupleSourceType.PROCESSOR); 
			}
			setReferenceValues(this.table);
			try {
				result = getEvaluator(Collections.emptyMap()).evaluateXQuery(this.table.getXQueryExpression(), this.table.getPassing(), null, this);
				if (this.buffer != null) {
					this.buffer.close();
					if (!usingOutput) {
						this.buffer.setForwardOnly(true);
					}
				}
			} catch (TeiidRuntimeException e) {
				if (e.getCause() instanceof TeiidComponentException) {
					throw (TeiidComponentException)e.getCause();
				}
				if (e.getCause() instanceof TeiidProcessingException) {
					throw (TeiidProcessingException)e.getCause();
				}
				throw e;
			}
		}
	}

	private List<?> processRow() throws ExpressionEvaluationException, BlockedException,
			TeiidComponentException, TeiidProcessingException {
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
						throw new TeiidProcessingException(QueryPlugin.Util.getString("XMLTableName.multi_value", proColumn.getName())); //$NON-NLS-1$
					}
					Object value = colItem;
					if (value instanceof AtomicValue) {
						value = Value.convertToJava(colItem);
					} else if (value instanceof Item) {
						Item i = (Item)value;
						BuiltInAtomicType bat = typeMapping.get(proColumn.getSymbol().getType());
						if (bat != null) {
							ConversionResult cr = StringValue.convertStringToBuiltInType(i.getStringValueCS(), bat, null);
							value = cr.asAtomic();
							value = Value.convertToJava((AtomicValue)value);
							if (value instanceof Item) {
								value = ((Item)value).getStringValue();
							}
						} else {
							value = i.getStringValue();
						}
					}
					value = FunctionDescriptor.importValue(value, proColumn.getSymbol().getType());
					tuple.add(value);
				} catch (XPathException e) {
					throw new TeiidProcessingException(e, QueryPlugin.Util.getString("XMLTableNode.path_error", proColumn.getName())); //$NON-NLS-1$
				}
			}
		}
		item = null;
		return tuple;
	}
	
	@Override
	public boolean hasFinalBuffer() {
		return this.table.getXQueryExpression().isStreaming();
	}
	
	@Override
	public TupleBuffer getFinalBuffer() throws BlockedException,
			TeiidComponentException, TeiidProcessingException {
		evaluate();
		usingOutput = true;
    	TupleBuffer finalBuffer = this.buffer;
    	this.buffer = null;
		close();
		return finalBuffer;
	}
	
	@Override
	public void processRow(NodeInfo row) {
		this.item = row;
		rowCount++;
		try {
			this.buffer.addTuple(processRow());
		} catch (TeiidException e) {
			throw new TeiidRuntimeException(e);
		}
	}
		
}