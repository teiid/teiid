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

import org.teiid.language.Command;
import org.teiid.language.Delete;
import org.teiid.language.Select;
import org.teiid.language.Update;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.Translator;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TranslatorProperty;
import org.teiid.translator.UpdateExecution;
import org.teiid.translator.object.ObjectConnection;
import org.teiid.translator.object.ObjectExecutionFactory;

/**
 * InfinispanExecutionFactory is the translator that will access an Infinispan local cache.
 * <p>
 * The optional setting is:
 * <li>{@link #supportsLuceneSearching dynamic Searching} - will default to <code>false</code>, supporting only Key searching.
 * Set to <code>true</code> will use the Hibernate/Lucene searching to locate objects in the cache</li> This is because you must 
 * have your objects in your cache annotated before Hibernate/Lucene searching will work.
 * </li>
 * 
 * @author vhalbert
 *
 */
@Translator(name = "infinispan-cache", description = "The Infinispan Cache Translator")
public class InfinispanExecutionFactory extends ObjectExecutionFactory {
	private boolean supportsLuceneSearching = false;

	public InfinispanExecutionFactory() {
		super();
	}
	
	public boolean isFullTextSearchingSupported() {
		return this.supportsLuceneSearching;
	}

	/**
	 * Indicates if Hibernate Search and Apache Lucene are used to index and
	 * search objects
	 * @return boolean
	 * 
	 * @since 6.1.0
	 */
	@TranslatorProperty(display = "Support Using Lucene Searching", description = "True, assumes objects have Hibernate Search annotations", advanced = true)
	public boolean supportsLuceneSearching() {
		return this.supportsLuceneSearching;
	}

	public void setSupportsLuceneSearching(boolean supportsLuceneSearching) {
		this.supportsLuceneSearching = supportsLuceneSearching;
	}

	@Override
	public boolean supportsOrCriteria() {
		return isFullTextSearchingSupported();
	}
	
	@Override
	public boolean supportsCompareCriteriaOrdered() {
		return isFullTextSearchingSupported();
	}
	
	@Override
	public boolean supportsNotCriteria() {
		return isFullTextSearchingSupported();
	}

	@Override
	public boolean supportsLikeCriteria() {
		// at this point, i've been unable to get the Like to work.
		return false;
	}
	
   @Override
	public UpdateExecution createUpdateExecution(Command command,
			ExecutionContext executionContext, RuntimeMetadata metadata,
			ObjectConnection connection) {
    	return new InfinispanUpdateExecution(command, connection, executionContext, this);
	}	
	
	@Override
	public List<Object> search(Select command, String cacheName,
			ObjectConnection connection, ExecutionContext executionContext)
			throws TranslatorException {

			if (supportsLuceneSearching()) {
				
				return LuceneSearch.performSearch(command, cacheName, connection);
			}

			return super.search(command, cacheName, connection, executionContext);
	}
	
	public List<Object> search(Delete command, String cacheName, ObjectConnection conn, ExecutionContext executionContext)
				throws TranslatorException {   
		return LuceneSearch.performSearch(command, cacheName, conn);
	}
	
	public List<Object> search(Update command, String cacheName, ObjectConnection conn, ExecutionContext executionContext)
			throws TranslatorException {   
		return LuceneSearch.performSearch(command, cacheName, conn);
	}	
	
	public Object performKeySearch(String cacheName, String columnName, Object value, ObjectConnection conn, ExecutionContext executionContext) throws TranslatorException {
		return LuceneSearch.performKeySearch(cacheName, columnName, value, conn);
	}		
	
}
