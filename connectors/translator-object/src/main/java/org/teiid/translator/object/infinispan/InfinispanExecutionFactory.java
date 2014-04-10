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
package org.teiid.translator.object.infinispan;

import java.util.List;

import org.teiid.language.Select;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.Translator;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.object.ObjectConnection;
import org.teiid.translator.object.ObjectExecutionFactory;

/**
 * InfinispanExecutionFactory is the translator that will access an Infinispan local cache.
 * <p>
 * 
 * @author vhalbert
 *
 */
@Translator(name = "infinispan-cache", description = "The Infinispan Cache Translator")
public class InfinispanExecutionFactory extends ObjectExecutionFactory {

	public InfinispanExecutionFactory() {
		super();
	}
	

	@Override
	public boolean supportsLikeCriteria() {
		// at this point, i've been unable to get the Like to work.
		return false;
	}
	
	@Override
	public List<Object> search(Select command, String cacheName,
			ObjectConnection connection, ExecutionContext executionContext)
			throws TranslatorException {

			if (connection.getCacheContainer().supportsLuceneSearching()) {
				Class<?> type = connection.getType(cacheName);
				return LuceneSearch.performSearch(command, type, cacheName,
						connection.getCacheContainer());
			}

			return super.search(command, cacheName, connection, executionContext);
	}
	
}
