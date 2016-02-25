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

import java.lang.reflect.Array;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import net.sf.saxon.Configuration;
import net.sf.saxon.om.Item;
import net.sf.saxon.om.NodeInfo;
import net.sf.saxon.om.SequenceIterator;
import net.sf.saxon.om.SequenceTool;
import net.sf.saxon.sxpath.XPathDynamicContext;
import net.sf.saxon.sxpath.XPathExpression;
import net.sf.saxon.trans.XPathException;
import net.sf.saxon.tree.iter.EmptyIterator;
import net.sf.saxon.type.BuiltInAtomicType;
import net.sf.saxon.type.ConversionResult;
import net.sf.saxon.type.Converter;
import net.sf.saxon.type.ValidationException;
import net.sf.saxon.value.AtomicValue;
import net.sf.saxon.value.CalendarValue;
import net.sf.saxon.value.StringValue;

import org.teiid.api.exception.query.ExpressionEvaluationException;
import org.teiid.client.plan.PlanNode;
import org.teiid.common.buffer.BlockedException;
import org.teiid.common.buffer.BufferManager.TupleSourceType;
import org.teiid.common.buffer.TupleBatch;
import org.teiid.common.buffer.TupleBuffer;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.types.ArrayImpl;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.TransformationException;
import org.teiid.core.types.XMLType;
import org.teiid.query.QueryPlugin;
import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.eval.Evaluator;
import org.teiid.query.function.FunctionDescriptor;
import org.teiid.query.function.source.XMLSystemFunctions;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.lang.XMLTable;
import org.teiid.query.sql.lang.XMLTable.XMLColumn;
import org.teiid.query.util.CommandContext;
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
	private long rowCount = 0;
	private Item item;
	
	private TupleBuffer buffer;
	
	private enum State {
		BUILDING,
		AVAILABLE,
		DONE
	};
	
	private State state = State.BUILDING;
	private volatile TeiidRuntimeException asynchException;
	private long outputRow = 1;
	private boolean usingOutput;
	
	private int rowLimit = -1;
	
	private boolean streaming;
	
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
	public void open() throws TeiidComponentException, TeiidProcessingException {
		super.open();
		if (getParent() instanceof LimitNode) {
			LimitNode parent = (LimitNode)getParent();
			if (parent.getLimit() > 0) {
				rowLimit = parent.getLimit() + parent.getOffset();
			}
		}
		streaming = this.table.getXQueryExpression().isStreaming();
	}

	@Override
	protected synchronized TupleBatch nextBatchDirect() throws BlockedException,
			TeiidComponentException, TeiidProcessingException {
		
		evaluate(false);
		
		if (streaming) {
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
			if (rowCount == rowLimit) {
				terminateBatches();
				break;
			}
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
		eval.evaluateParameters(this.table.getPassing(), null, parameters);
		final Object contextItem;
		if (parameters.containsKey(null)) {
			contextItem = parameters.remove(null);
			//null context item mean no rows
			if (contextItem == null) {
				result = new Result();
				result.iter = EmptyIterator.emptyIterator();
				streaming = false;
				return;
			}
		} else {
			contextItem = null;
		}

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
				if (rowCount > Integer.MAX_VALUE) {
		    		throw new TeiidRuntimeException(new TeiidProcessingException(QueryPlugin.Event.TEIID31174, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31174)));
		    	}
				tuple.add((int)rowCount);
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
						XMLType value = table.getXQueryExpression().createXMLType(pathIter.getAnother(), this.getBufferManager(), false, getContext());
						tuple.add(value);
						continue;
					}
					Item next = pathIter.next();
					if (next != null) {
						if (proColumn.getSymbol().getType().isArray()) {
							ArrayList<Object> vals = new ArrayList<Object>();
							vals.add(getValue(proColumn.getSymbol().getType().getComponentType(), colItem, this.table.getXQueryExpression().getConfig(), getContext()));
							vals.add(getValue(proColumn.getSymbol().getType().getComponentType(), next, this.table.getXQueryExpression().getConfig(), getContext()));
							while ((next = pathIter.next()) != null) {
								vals.add(getValue(proColumn.getSymbol().getType().getComponentType(), next, this.table.getXQueryExpression().getConfig(), getContext()));
							}
							Object value = new ArrayImpl(vals.toArray((Object[]) Array.newInstance(proColumn.getSymbol().getType().getComponentType(), vals.size())));
							tuple.add(value);
							continue;
						}
						throw new TeiidProcessingException(QueryPlugin.Event.TEIID30171, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30171, proColumn.getName()));
					}
					Object value = getValue(proColumn.getSymbol().getType(), colItem, this.table.getXQueryExpression().getConfig(), getContext());
					tuple.add(value);
				} catch (XPathException e) {
					throw new TeiidProcessingException(QueryPlugin.Event.TEIID30172, e, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30172, proColumn.getName()));
				}
			}
		}
		item = null;
		return tuple;
	}

	public static Object getValue(Class<?> type,
			Item colItem, Configuration config, CommandContext context) throws XPathException,
			ValidationException, TransformationException {
		Object value = colItem;
		if (value instanceof AtomicValue) {
			value = getValue((AtomicValue)colItem, context);
		} else if (value instanceof Item) {
			Item i = (Item)value;
			if (XMLSystemFunctions.isNull(i)) {
				return null;
			}
			BuiltInAtomicType bat = typeMapping.get(type);
			if (bat != null) {
				AtomicValue av = new StringValue(i.getStringValueCS());
				ConversionResult cr = Converter.convert(av, bat, config.getConversionRules());
				value = cr.asAtomic();
				value = getValue((AtomicValue)value, context);
				if (value instanceof Item) {
					value = ((Item)value).getStringValue();
				}
			} else {
				value = i.getStringValue();
			}
		}
		return FunctionDescriptor.importValue(value, type);
	}

	static private Object getValue(AtomicValue value, CommandContext context) throws XPathException {
		if (value instanceof CalendarValue) {
			CalendarValue cv = (CalendarValue)value;
			if (!cv.hasTimezone()) {
				TimeZone tz = context.getServerTimeZone();
				int tzMin = tz.getRawOffset()/60000;
				if (tz.getDSTSavings() > 0) {
					tzMin = tz.getOffset(cv.getCalendar().getTimeInMillis())/60000;
				}
				cv.setTimezoneInMinutes(tzMin);
				Calendar cal = cv.getCalendar();
				return new Timestamp(cal.getTime().getTime());
			}
		}
		return SequenceTool.convertToJava(value);
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
	
	@Override
	public PlanNode getDescriptionProperties() {
		PlanNode props = super.getDescriptionProperties();
        AnalysisRecord.addLanaguageObjects(props, AnalysisRecord.PROP_TABLE_FUNCTION, Arrays.asList(this.table));
        return props;
	}

}