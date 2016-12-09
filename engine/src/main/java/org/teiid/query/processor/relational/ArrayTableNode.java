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

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import org.teiid.client.plan.PlanNode;
import org.teiid.common.buffer.BlockedException;
import org.teiid.common.buffer.BufferManager;
import org.teiid.common.buffer.TupleBatch;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.TransformationException;
import org.teiid.query.QueryPlugin;
import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.function.FunctionMethods;
import org.teiid.query.processor.ProcessorDataManager;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.lang.ArrayTable;
import org.teiid.query.sql.lang.TableFunctionReference.ProjectedColumn;
import org.teiid.query.util.CommandContext;

/**
 * Handles array table processing.
 */
public class ArrayTableNode extends SubqueryAwareRelationalNode {

	private ArrayTable table;
	
	//initialized state
    private int[] projectionIndexes;
	
	public ArrayTableNode(int nodeID) {
		super(nodeID);
	}
	
	@Override
	public void initialize(CommandContext context, BufferManager bufferManager,
			ProcessorDataManager dataMgr) {
		super.initialize(context, bufferManager, dataMgr);
		if (projectionIndexes != null) {
			return;
		}
        Map elementMap = createLookupMap(table.getProjectedSymbols());
        this.projectionIndexes = getProjectionIndexes(elementMap, getElements());
	}
	
	@Override
	public void closeDirect() {
		super.closeDirect();
		reset();
	}
	
	public void setTable(ArrayTable table) {
		this.table = table;
	}

	@Override
	public ArrayTableNode clone() {
		ArrayTableNode clone = new ArrayTableNode(getID());
		this.copyTo(clone);
		clone.setTable(table);
		return clone;
	}

	@Override
	protected TupleBatch nextBatchDirect() throws BlockedException,
			TeiidComponentException, TeiidProcessingException {
		Object array = getEvaluator(Collections.emptyMap()).evaluate(table.getArrayValue(), null);

		if (array != null) {
			ArrayList<Object> tuple = new ArrayList<Object>(projectionIndexes.length);
			for (int output : projectionIndexes) {
				ProjectedColumn col = table.getColumns().get(output);
				try {
					Object val = FunctionMethods.array_get(array, output + 1);
					tuple.add(DataTypeManager.transformValue(val, table.getColumns().get(output).getSymbol().getType()));
				} catch (TransformationException e) {
					 throw new TeiidProcessingException(QueryPlugin.Event.TEIID30190, e, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30190, col.getName()));
				} catch (SQLException e) {
					throw new TeiidProcessingException(QueryPlugin.Event.TEIID30188, e);
				}
			}
			addBatchRow(tuple);
		}
		terminateBatches();
		return pullBatch();
	}
	
	@Override
	public Collection<? extends LanguageObject> getObjects() {
		return Arrays.asList(this.table.getArrayValue());
	}
	
	@Override
	public PlanNode getDescriptionProperties() {
		PlanNode props = super.getDescriptionProperties();
        AnalysisRecord.addLanaguageObjects(props, AnalysisRecord.PROP_TABLE_FUNCTION, Arrays.asList(this.table));
        return props;
	}

}
