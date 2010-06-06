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

import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import javax.xml.transform.OutputKeys;
import javax.xml.transform.TransformerException;

import net.sf.saxon.Configuration;
import net.sf.saxon.om.Item;
import net.sf.saxon.om.NodeInfo;
import net.sf.saxon.om.SequenceIterator;
import net.sf.saxon.query.QueryResult;
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
import org.teiid.core.types.SQLXMLImpl;
import org.teiid.core.types.Streamable;
import org.teiid.core.types.TransformationException;
import org.teiid.core.types.XMLTranslator;
import org.teiid.core.types.XMLType;
import org.teiid.core.types.XMLType.Type;
import org.teiid.query.processor.xml.XMLUtil;
import org.teiid.query.sql.lang.XMLTable;
import org.teiid.query.sql.lang.XMLTable.XMLColumn;
import org.teiid.query.sql.symbol.DerivedColumn;

/**
 * Handles xml table processing.
 */
public class XMLTableNode extends SubqueryAwareRelationalNode {

	private static final class QueryResultTranslator extends XMLTranslator {
		private final SequenceIterator pathIter;
		private final Configuration config;

		private QueryResultTranslator(SequenceIterator pathIter, Configuration config) {
			this.pathIter = pathIter;
			this.config = config;
		}

		@Override
		public void translate(Writer writer) throws TransformerException,
				IOException {
			Properties props = new Properties();
		    props.setProperty(OutputKeys.METHOD, "xml"); //$NON-NLS-1$
		    //props.setProperty(OutputKeys.INDENT, "yes"); //$NON-NLS-1$
		    props.setProperty(OutputKeys.OMIT_XML_DECLARATION, "yes"); //$NON-NLS-1$
			QueryResult.serializeSequence(pathIter, config, writer, props);
		}
	}

	private XMLTable table;
	private List<XMLColumn> projectedColumns;
	
	private SequenceIterator result;
	private int rowCount = 0;
	
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
			HashMap<String, Object> parameters = new HashMap<String, Object>();
			Object contextItem = null;
			for (DerivedColumn passing : this.table.getPassing()) {
				Object value = getEvaluator(Collections.emptyMap()).evaluate(passing.getExpression(), null);
				if (passing.getAlias() == null) {
					contextItem = value;
				} else {
					parameters.put(passing.getAlias(), value);
				}
			}
			result = this.table.getXQueryExpression().evaluateXQuery(contextItem, parameters);
		}
		
		while (!isBatchFull() && !isLastBatch()) {
			try {
				processRow();
			} catch (XPathException e) {
				e.printStackTrace();
			}
		}
		return pullBatch();
	}

	private void processRow() throws XPathException,
			ExpressionEvaluationException, BlockedException,
			TeiidComponentException, TeiidProcessingException,
			TransformationException {
		Item item = result.next();
		rowCount++;
		if (item == null) {
			terminateBatches();
			return;
		}
		List<Object> tuple = new ArrayList<Object>(projectedColumns.size());
		for (XMLColumn proColumn : projectedColumns) {
			if (proColumn.isOrdinal()) {
				tuple.add(rowCount);
			} else {
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
					Item next = pathIter.next();
					XMLType.Type type = Type.FRAGMENT;
					if (next != null) {
						if (next instanceof NodeInfo || colItem instanceof NodeInfo) {
							type = Type.SIBLINGS;
						} else {
							type = Type.TEXT;
						}
					}
					pathIter = pathIter.getAnother();
					SQLXMLImpl xml = XMLUtil.saveToBufferManager(getBufferManager(), new QueryResultTranslator(pathIter, this.table.getXQueryExpression().getConfig()), Streamable.STREAMING_BATCH_SIZE_IN_BYTES);
					XMLType value = new XMLType(xml);
					value.setType(type);
					tuple.add(value);
					continue;
				}
				if (pathIter.next() != null) {
					throw new TeiidProcessingException("Unexpected multi-valued result was returned for XML Column " + proColumn.getName() + ".  All path expressions should return at most a single result.");
				}
				Object value = Value.convertToJava(colItem);
				if (value instanceof Item) {
					value = ((Item)value).getStringValue();
				}
				value = DataTypeManager.convertToRuntimeType(value);
				value = DataTypeManager.transformValue(value, proColumn.getSymbol().getType());
				tuple.add(value);
			}
		}
		addBatchRow(tuple);
	}
	
}