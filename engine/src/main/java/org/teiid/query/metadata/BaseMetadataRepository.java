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

import org.teiid.metadata.AbstractMetadataRecord;
import org.teiid.metadata.Column;
import org.teiid.metadata.ColumnStats;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.MetadataRepository;
import org.teiid.metadata.Procedure;
import org.teiid.metadata.Table;
import org.teiid.metadata.TableStats;
import org.teiid.metadata.Table.TriggerEvent;
import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.TranslatorException;

public class BaseMetadataRepository<F, C> implements MetadataRepository<F, C> {
	protected MetadataRepository nextRepository;
	
	@Override
	public void loadMetadata(MetadataFactory factory, ExecutionFactory<F, C> executionFactory, F connectionFactory) throws TranslatorException {
		if (this.nextRepository != null) {
			this.nextRepository.loadMetadata(factory, executionFactory, connectionFactory);
		}		
	}	
	
	@Override
	public void setViewDefinition(String vdbName, int vdbVersion, Table table,String viewDefinition) {
		if (this.nextRepository != null) {
			this.nextRepository.setViewDefinition(vdbName, vdbVersion, table, viewDefinition);
		}
	}

	@Override
	public void setInsteadOfTriggerDefinition(String vdbName, int vdbVersion,
			Table table, TriggerEvent triggerOperation, String triggerDefinition) {
		if (this.nextRepository != null) {
			this.nextRepository.setInsteadOfTriggerDefinition(vdbName, vdbVersion, table, triggerOperation, triggerDefinition);
		}		
	}

	@Override
	public void setInsteadOfTriggerEnabled(String vdbName, int vdbVersion,
			Table table, TriggerEvent triggerOperation, boolean enabled) {
		if (this.nextRepository != null) {
			this.nextRepository.setInsteadOfTriggerEnabled(vdbName, vdbVersion, table, triggerOperation, enabled);
		}		
	}

	@Override
	public void setProcedureDefinition(String vdbName, int vdbVersion,
			Procedure procedure, String procedureDefinition) {
		if (this.nextRepository != null) {
			this.nextRepository.setProcedureDefinition(vdbName, vdbVersion, procedure, procedureDefinition);
		}		
	}

	@Override
	public void setTableStats(String vdbName, int vdbVersion, Table table,
			TableStats tableStats) {
		if (this.nextRepository != null) {
			this.nextRepository.setTableStats(vdbName, vdbVersion, table, tableStats);
		}		
	}

	@Override
	public void setColumnStats(String vdbName, int vdbVersion, Column column,
			ColumnStats columnStats) {
		if (this.nextRepository != null) {
			this.nextRepository.setColumnStats(vdbName, vdbVersion, column, columnStats);
		}		
	}

	@Override
	public void setProperty(String vdbName, int vdbVersion,
			AbstractMetadataRecord record, String name, String value) {
		if (this.nextRepository != null) {
			this.nextRepository.setProperty(vdbName, vdbVersion, record, name, value);
		}		
	}

    public void setNext(MetadataRepository next) {
        this.nextRepository = next;
    }
}
