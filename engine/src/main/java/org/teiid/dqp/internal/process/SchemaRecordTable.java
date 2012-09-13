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

package org.teiid.dqp.internal.process;

import java.util.List;
import java.util.NavigableMap;

import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.metadata.AbstractMetadataRecord;
import org.teiid.metadata.Procedure;
import org.teiid.metadata.Schema;
import org.teiid.metadata.Table;
import org.teiid.query.metadata.CompositeMetadataStore;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.tempdata.BaseIndexInfo;

class SchemaRecordTable extends RecordTable<Schema> {
	
	public SchemaRecordTable(int pkColumnIndex, List<ElementSymbol> columns) {
		super(new int[] {0}, columns.subList(pkColumnIndex, pkColumnIndex + 1));
	}
	
	protected boolean isValid(Schema s, VDBMetaData vdb, List<Object> rowBuffer, Criteria condition) throws TeiidProcessingException, TeiidComponentException {
		if (s == null || !vdb.isVisible(s.getName())) {
			return false;
		}
		return super.isValid(s, vdb, rowBuffer, condition);
	}
	
	@Override
	public SimpleIterator<Schema> processQuery(
			VDBMetaData vdb, CompositeMetadataStore metadataStore,
			BaseIndexInfo<?> ii) {
		return processQuery(vdb, metadataStore.getSchemas(), ii);
	}

}

abstract class SchemaChildRecordTable<T extends AbstractMetadataRecord> extends RecordTable<T> {
	
	private SchemaRecordTable schemaTable;
	
	public SchemaChildRecordTable(int schemaPkColumnIndex, int tablePkColumnIndex, List<ElementSymbol> columns) {
		super(new int[] {0}, columns.subList(tablePkColumnIndex, tablePkColumnIndex + 1));
		this.schemaTable = new SchemaRecordTable(schemaPkColumnIndex, columns);
	}
	
	@Override
	public SimpleIterator<T> processQuery(
			final VDBMetaData vdb, final CompositeMetadataStore metadataStore,
			final BaseIndexInfo<?> ii) {
		final SimpleIterator<Schema> schemas = schemaTable.processQuery(vdb, metadataStore.getSchemas(), ii);
		return new ExpandingSimpleIterator<Schema, T>(schemas) {
			@Override
			protected SimpleIterator<T> getChildIterator(
					Schema parent) {
				return processQuery(vdb, getChildren(parent), ii.next);
			}
		};
	}
	
	@Override
	public BaseIndexInfo<RecordTable<?>> planQuery(Query query,
			Criteria condition) {
		BaseIndexInfo<RecordTable<?>> ii = schemaTable.planQuery(query, query.getCriteria());
		ii.next = super.planQuery(query, ii.getNonCoveredCriteria());
		return ii;
	}
	
	@Override
	protected void fillRow(T s, List<Object> rowBuffer) {
		rowBuffer.add(s.getName());
	}
	
	protected abstract NavigableMap<String, T> getChildren(Schema s);
	
}

class ProcedureSystemTable extends SchemaChildRecordTable<Procedure> {
	public ProcedureSystemTable(int schemaPkColumnIndex,
			int tablePkColumnIndex, List<ElementSymbol> columns) {
		super(schemaPkColumnIndex, tablePkColumnIndex, columns);
	}

	@Override
	protected NavigableMap<String, Procedure> getChildren(Schema s) {
		return s.getProcedures();
	}
}

class TableSystemTable extends SchemaChildRecordTable<Table> {
	public TableSystemTable(int schemaPkColumnIndex,
			int tablePkColumnIndex, List<ElementSymbol> columns) {
		super(schemaPkColumnIndex, tablePkColumnIndex, columns);
	}

	@Override
	protected NavigableMap<String, Table> getChildren(Schema s) {
		return s.getTables();
	}
}