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

package org.teiid.runtime;

import org.teiid.core.types.DataTypeManager;
import org.teiid.dqp.internal.process.multisource.MultiSourceMetadataWrapper;
import org.teiid.metadata.BaseColumn.NullType;
import org.teiid.metadata.Column;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.MetadataRepository;
import org.teiid.metadata.Procedure;
import org.teiid.metadata.ProcedureParameter;
import org.teiid.metadata.ProcedureParameter.Type;
import org.teiid.metadata.Schema;
import org.teiid.metadata.Table;
import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.TranslatorException;

public class MultiSourceMetadataRepository extends MetadataRepository<Object, Object> {
	
	private String multiSourceColumnName;
	
	public MultiSourceMetadataRepository(String multiSourceColumnName) {
		this.multiSourceColumnName = multiSourceColumnName;
	}
	
	@Override
	public void loadMetadata(MetadataFactory factory,
			ExecutionFactory<Object, Object> executionFactory, Object connectionFactory)
			throws TranslatorException {
		Schema s = factory.getSchema();
		for (Table t : s.getTables().values()) {
			if (!t.isPhysical()) {
				continue;
			}
			Column c = t.getColumnByName(multiSourceColumnName);
			if (c == null) {
				c = factory.addColumn(multiSourceColumnName, DataTypeManager.DefaultDataTypes.STRING, t);
				MultiSourceMetadataWrapper.setMultiSourceElementMetadata(c);
			}
		}
		outer: for (Procedure p : s.getProcedures().values()) {
			if (p.isVirtual()) {
				continue;
			}
			for (ProcedureParameter pp : p.getParameters()) {
				if (multiSourceColumnName.equalsIgnoreCase(pp.getName())) {
					continue outer;
				}
			}
			ProcedureParameter pp = factory.addProcedureParameter(multiSourceColumnName, DataTypeManager.DefaultDataTypes.STRING, Type.In, p);
			pp.setNullType(NullType.Nullable);
		}
	}

}
