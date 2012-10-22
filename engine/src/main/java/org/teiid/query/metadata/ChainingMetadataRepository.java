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

package org.teiid.query.metadata;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.teiid.metadata.AbstractMetadataRecord;
import org.teiid.metadata.Column;
import org.teiid.metadata.ColumnStats;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.MetadataRepository;
import org.teiid.metadata.Procedure;
import org.teiid.metadata.Table;
import org.teiid.metadata.Table.TriggerEvent;
import org.teiid.metadata.TableStats;
import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.TranslatorException;

public class ChainingMetadataRepository extends MetadataRepository<Object, Object> {

	final ArrayList<MetadataRepository<Object, Object>> repositories;

	@SuppressWarnings("unchecked")
	public ChainingMetadataRepository(List<? extends MetadataRepository> repos) {
		this.repositories = new ArrayList<MetadataRepository<Object,Object>>((Collection<? extends MetadataRepository<Object, Object>>)repos);
	}
	
	@Override
	public void loadMetadata(MetadataFactory factory,
			ExecutionFactory<Object, Object> executionFactory,
			Object connectionFactory) throws TranslatorException {
		for (MetadataRepository<Object, Object> repo : repositories) {
			repo.loadMetadata(factory, executionFactory, connectionFactory);
		}
	}

	@Override
	public void setColumnStats(String vdbName, int vdbVersion, Column column,
			ColumnStats columnStats) {
		for (MetadataRepository<Object, Object> repo : repositories) {
			repo.setColumnStats(vdbName, vdbVersion, column, columnStats);
		}
	}

	@Override
	public void setInsteadOfTriggerDefinition(String vdbName, int vdbVersion,
			Table table, TriggerEvent triggerOperation, String triggerDefinition) {
		for (MetadataRepository<Object, Object> repo : repositories) {
			repo.setInsteadOfTriggerDefinition(vdbName, vdbVersion, table, triggerOperation, triggerDefinition);
		}		
	}

	@Override
	public void setInsteadOfTriggerEnabled(String vdbName, int vdbVersion,
			Table table, TriggerEvent triggerOperation, boolean enabled) {
		for (MetadataRepository<Object, Object> repo : repositories) {
			repo.setInsteadOfTriggerEnabled(vdbName, vdbVersion, table, triggerOperation, enabled);
		}
	}

	@Override
	public void setProcedureDefinition(String vdbName, int vdbVersion,
			Procedure procedure, String procedureDefinition) {
		for (MetadataRepository<Object, Object> repo : repositories) {
			repo.setProcedureDefinition(vdbName, vdbVersion, procedure, procedureDefinition);
		}
	}

	@Override
	public void setProperty(String vdbName, int vdbVersion,
			AbstractMetadataRecord record, String name, String value) {
		for (MetadataRepository<Object, Object> repo : repositories) {
			repo.setProperty(vdbName, vdbVersion, record, name, value);
		}
	}

	@Override
	public void setTableStats(String vdbName, int vdbVersion, Table table,
			TableStats tableStats) {
		for (MetadataRepository<Object, Object> repo : repositories) {
			repo.setTableStats(vdbName, vdbVersion, table, tableStats);
		}	
	}

	@Override
	public void setViewDefinition(String vdbName, int vdbVersion, Table table,
			String viewDefinition) {
		for (MetadataRepository<Object, Object> repo : repositories) {
			repo.setViewDefinition(vdbName, vdbVersion, table, viewDefinition);
		}
	}
	
}
