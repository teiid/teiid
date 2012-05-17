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
package org.teiid.metadata.index;

import java.io.IOException;

import org.teiid.metadata.MetadataFactory;
import org.teiid.query.metadata.BaseMetadataRepository;
import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.TranslatorException;

public class IndexMetadataRepository extends BaseMetadataRepository {
	
	private IndexMetadataStore idxStore;
	
	public IndexMetadataRepository(IndexMetadataStore index) {
		this.idxStore = index;
	}

	@Override
	public void loadMetadata(MetadataFactory factory, ExecutionFactory executionFactory, Object connectionFactory)
			throws TranslatorException {
		try {
			this.idxStore.load(factory.getName(), factory.getDataTypes() == null?null:factory.getDataTypes().values());
			if (this.idxStore.getSchema(factory.getName()) == null) {
				throw new TranslatorException(RuntimeMetadataPlugin.Util.gs(RuntimeMetadataPlugin.Event.TEIID80004, factory.getName()));
			}
			factory.setSchema(this.idxStore.getSchema(factory.getName()));
		} catch (IOException e) {
			throw new TranslatorException(e);
		}
		
		super.loadMetadata(factory, executionFactory, connectionFactory);
	}
}
