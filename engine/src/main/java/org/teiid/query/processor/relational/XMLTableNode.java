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

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
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
import net.sf.saxon.value.CalendarValue;
import net.sf.saxon.value.StringValue;
import net.sf.saxon.value.Value;

import org.teiid.api.exception.query.ExpressionEvaluationException;
import org.teiid.common.buffer.BlockedException;
import org.teiid.common.buffer.BufferManager.TupleSourceType;
import org.teiid.common.buffer.TupleBatch;
import org.teiid.common.buffer.TupleBuffer;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.XMLType;
import org.teiid.query.QueryPlugin;
import org.teiid.query.eval.Evaluator;
import org.teiid.query.function.FunctionDescriptor;
import org.teiid.query.function.source.XMLSystemFunctions;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.lang.XMLTable;
import org.teiid.query.sql.lang.XMLTable.XMLColumn;
import org.teiid.query.xquery.saxon.SaxonXQueryExpression.Result;
import org.teiid.query.xquery.saxon.SaxonXQueryExpression.RowProcessor;
import org.teiid.query.xquery.saxon.XQueryEvaluator;

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
		typeMapping.put(DataTypeManager.DefaultDataClasses.BLOB, BuiltInAtomicType.HEX_BINARY);
		typeMapping.put(DataTypeManager.DefaultDataClasses.VARBINARY, BuiltInAtomicType.HEX_BINARY);
	}
	
	private static TeiidRuntimeException EARLY_TERMINATION = new TeiidRuntimeException();
	
	private XMLTable table;
	private List<XMLColumn> projectedColumns;
	
	private Result result;
	private int rowCount = 0;
	private Item item;
	
	private TupleBuffer buffer;
	
	private enum State {
		BUILDING,
		AVAILABLE,
		DONE
	};
	
	private State state = State.BUILDING;
	private volatile TeiidRuntimeException asynchException;
	private int outputRow = 1;
	private boolean usingOutput;
	
	private int rowLimit = -1;
	
	public XMLTableNode(int nodeID) {
		super(nodeID);
	}
	
	@Override
	public synchronized void closeDirect() {
		super.closeDirect();
		if(this.buffer != null) {
			if (!usingOutput) {
				this.buffer.remove();
			}
        	this.buffer = null;
        }
		reset();
	}
	
	@Override
	public synchronized void reset() {
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
		this.state = State.BUILDING;
		this.asynchException = null;
		this.rowLimit = -1;
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
		this.copyTo(clone);
		clone.setTable(table);
		clone.setProjectedColumns(projectedColumns);
		return clone;
	}

	@Override
	protected synchronized TupleBatch nextBatchDirect() throws BlockedException,
			TeiidComponentException, TeiidProcessingException {
		
		evaluate(false);
		
		if (this.table.getXQueryExpression().isStreaming()) {
			while (state == State.BUILDING) {
				try {
					this.wait();
				} catch (InterruptedException e) {
					 throw new TeiidRuntimeException(QueryPlugin.Event.TEIID30169, e);
				}
			}
			unwrapException(asynchException);
			TupleBatch batch = this.buffer.getBatch(outputRow);
			outputRow = batch.getEndRow() + 1;
			if (state != State.DONE && !batch.getTerminationFlag()) {
				state = hasNextBatch()?State.AVAILABLE:State.BUILDING;
			}
			return batch;
		}
		
		while (!isBatchFull() && !isLastBatch()) {
			if (item == null) {
				try {
					item = result.iter.next();
				} catch (XPathException e) {
					 throw new TeiidProcessingException(QueryPlugin.Event.TEIID30170, e, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30170, e.getMessage()));
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

	private void evaluate(final boolean useFinalBuffer) throws TeiidComponentException,
			ExpressionEvaluationException, BlockedException,
			TeiidProcessingException {
		if (result != null || this.buffer != null) {
			return;
		}
		setReferenceValues(this.table);
		final HashMap<String, Object> parameters = new HashMap<String, Object>();
		Evaluator eval = getEvaluator(Collections.emptyMap());
		final Object contextItem = eval.evaluateParameters(this.table.getPassing(), null, parameters);

		if (this.table.getXQueryExpression().isStreaming()) {
			if (this.buffer == null) {
				this.buffer = this.getBufferManager().createTupleBuffer(getOutputElements(), getConnectionID(), TupleSourceType.PROCESSOR);
				if (!useFinalBuffer) {
					this.buffer.setForwardOnly(true);
				}
			}
			Runnable r = new Runnable() {
				@Override
				public void run() {
					try {
						XQueryEvaluator.evaluateXQuery(table.getXQueryExpression(), contextItem, parameters, XMLTableNode.this, getContext());
					} catch (TeiidRuntimeException e) {
						if (e != EARLY_TERMINATION) {
							asynchException = e;
						}
					} catch (Throwable e) {
						asynchException = new TeiidRuntimeException(e);
					} finally {
						synchronized (XMLTableNode.this) {
							if (buffer != null && asynchException == null) {
								try {
									buffer.close();
								} catch (TeiidComponentException e) {
									asynchException = new TeiidRuntimeException(e);
								}
							}
							state = State.DONE;
							XMLTableNode.this.notifyAll();
						}
					}
				}
			};
			this.getContext().getExecutor().execute(r);
			return;
		}
		try {
			result = XQueryEvaluator.evaluateXQuery(this.table.getXQueryExpression(), contextItem, parameters, null, this.getContext());
		} catch (TeiidRuntimeException e) {
			unwrapException(e);
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
						 throw new TeiidProcessingException(QueryPlugin.Event.TEIID30171, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30171, proColumn.getName()));
					}
					Object value = colItem;
					if (value instanceof AtomicValue) {
						value = getValue((AtomicValue)colItem);
					} else if (value instanceof Item) {
						Item i = (Item)value;
						if (XMLSystemFunctions.isNull(i)) {
							tuple.add(null);
							continue;
						}
						BuiltInAtomicType bat = typeMapping.get(proColumn.getSymbol().getType());
						if (bat != null) {
							ConversionResult cr = StringValue.convertStringToBuiltInType(i.getStringValueCS(), bat, null);
							value = cr.asAtomic();
							value = getValue((AtomicValue)value);
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
					 throw new TeiidProcessingException(QueryPlugin.Event.TEIID30172, e, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30172, proColumn.getName()));
				}
			}
		}
		item = null;
		return tuple;
	}

	private Object getValue(AtomicValue value) throws XPathException {
		if (value instanceof CalendarValue) {
			CalendarValue cv = (CalendarValue)value;
			if (!cv.hasTimezone()) {
				int tzMin = getContext().getServerTimeZone().getRawOffset()/60000;
				cv.setTimezoneInMinutes(tzMin);
				Calendar cal = cv.getCalendar();
				return new Timestamp(cal.getTime().getTime());
			}
		}
		return Value.convertToJava(value);
	}
	
	@Override
	public synchronized void processRow(NodeInfo row) {
		if (isClosed()) {
			throw EARLY_TERMINATION;
		}
		assert this.state != State.DONE;
		this.item = row;
		rowCount++;
		try {
			this.buffer.addTuple(processRow());
			if (this.buffer.getRowCount() == rowLimit) {
				throw EARLY_TERMINATION;
			}
			if (state == State.BUILDING && hasNextBatch()) {
				this.state = State.AVAILABLE;
				this.notifyAll();
			}
		} catch (TeiidException e) {
			 throw new TeiidRuntimeException(e);
		}
	}

	private boolean hasNextBatch() {
		return this.outputRow + this.buffer.getBatchSize() <= rowCount + 1;
	}
	
	@Override
	protected Collection<? extends LanguageObject> getObjects() {
		return this.table.getPassing();
	}

}