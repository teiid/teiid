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

import org.teiid.language.Command;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.Translator;
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
	private boolean supportsDSLSearching = false;
	

	public InfinispanExecutionFactory() {
		super();
	}
	
	public boolean isFullTextSearchingSupported() {
		return this.supportsLuceneSearching || this.supportsDSLSearching;
	}

	/**
	 * Indicates if Hibernate Search and Apache Lucene are used to index and
	 * search objects
	 * @return boolean
	 * 
	 * @since 6.1.0
	 */
	@TranslatorProperty(display = "Support Using Lucene Searching", description = "True, assumes objects have Hibernate Search annotations and will use Hiberante Lucene searching", advanced = true)
	public boolean supportsLuceneSearching() {
		return this.supportsLuceneSearching;
	}

	public void setSupportsLuceneSearching(boolean supportsLuceneSearching) {
		this.supportsLuceneSearching = supportsLuceneSearching;
		setSearchType(new LuceneSearch());
	}
	
	/**
	 * Indicates if Infinispan DSL Querying is used for searching	 * @return boolean
	 * @return boolean
	 * 
	 * @since 6.1.0
	 */
	@TranslatorProperty(display = "Support Using DSL Searching", description = "True means Infinispan DSL Querying is used for searching ", advanced = true)
	public boolean supportsDSLSearching() {
		return this.supportsDSLSearching;
	}

	public void setSupportsDSLSearching(boolean supportsDSLSearching) {
		this.supportsDSLSearching = supportsDSLSearching;
		setSearchType(new DSLSearch());
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
		// at this point, i've been unable to get this to work with Lucene searching
		return this.supportsDSLSearching;
	}
	
   @Override
	public UpdateExecution createUpdateExecution(Command command,
			ExecutionContext executionContext, RuntimeMetadata metadata,
			ObjectConnection connection) {
    	return new InfinispanUpdateExecution(command, connection, executionContext, this);
	}	
	

	
}
