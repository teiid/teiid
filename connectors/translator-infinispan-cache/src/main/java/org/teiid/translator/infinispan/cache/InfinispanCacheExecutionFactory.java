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
package org.teiid.translator.infinispan.cache;

import org.teiid.language.Command;
import org.teiid.language.QueryExpression;
import org.teiid.language.Select;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.Translator;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TranslatorProperty;
import org.teiid.translator.UpdateExecution;
import org.teiid.translator.object.ObjectConnection;
import org.teiid.translator.object.ObjectExecution;
import org.teiid.translator.object.ObjectExecutionFactory;
import org.teiid.translator.object.ObjectUpdateExecution;
import org.teiid.translator.object.ObjectVisitor;
import org.teiid.translator.object.simpleMap.SearchByKey;
import org.teiid.translator.object.simpleMap.SimpleKeyVisitor;

/**
 * InfinispanExecutionFactory is the "infinispan-cache" translator that is used to access an Infinispan cache.
 * <p>
 * The optional setting is:
 * <li>{@link #supportsDSLSearching DSL Searching} - will default to <code>false</code>, supporting only Key searching.
 * Set to <code>true</code> will use the Infinispan DSL query language to search the cache for objects</li> 
 * </li>
 * 
 * @author vhalbert
 *
 */
@Translator(name = "infinispan-cache", description = "The Infinispan Cache Translator")
public class InfinispanCacheExecutionFactory extends ObjectExecutionFactory {

	public static final int MAX_SET_SIZE = 10000;

	private boolean supportsLuceneSearching = false;
	private boolean supportsDSLSearching = false;
	
	private boolean supportsCompareCriteriaOrdered = false;
	private boolean supportNotCriteria = false;
	private boolean supportsIsNullCriteria = false;

	public InfinispanCacheExecutionFactory() {
		super();
		setSourceRequiredForMetadata(true);
		setMaxInCriteriaSize(MAX_SET_SIZE);
		setMaxDependentInPredicates(1);
		
		setSupportsSelectDistinct(false);
		setSupportsInnerJoins(true);
		setSupportsFullOuterJoins(false);
		setSupportsOuterJoins(true);
		
		setSupportedJoinCriteria(SupportedJoinCriteria.EQUI);
		
		// default search is by key type
		setSearchType(new SearchByKey());

	}
	
	public boolean isFullQuerySupported() {
		return this.supportsDSLSearching || this.supportsLuceneSearching ;
	}
	
	@Override
	public ResultSetExecution createResultSetExecution(QueryExpression command,
			ExecutionContext executionContext, RuntimeMetadata metadata,
			ObjectConnection connection) throws TranslatorException {
		return new ObjectExecution((Select) command, metadata, this, connection, executionContext) {
			@Override
			protected ObjectVisitor createVisitor() {
				// us the base object visitor when perform DSL or Lucence searching
				// because the visitor doesn't need to overhead of capturing the values used when doing key searches
				if (isFullQuerySupported()) {
					return super.createVisitor();
				}
		    	return new SimpleKeyVisitor();

		    }
		};
	}
	
    @Override
	public UpdateExecution createUpdateExecution(Command command,
			ExecutionContext executionContext, RuntimeMetadata metadata,
			ObjectConnection connection) throws TranslatorException {
    	return new ObjectUpdateExecution(command, connection, executionContext, this) {
			@Override
			protected ObjectVisitor createVisitor() {
				// us the base object visitor when perform DSL or Lucence searching
				// because the visitor doesn't need to overhead of capturing the values used when doing key searches
				if (isFullQuerySupported()) {
					return super.createVisitor();
				}
		    	return new SimpleKeyVisitor();

		    }  		
    	};
	}		

	/**
	 * Indicates if Hibernate Search and Apache Lucene were used to index and
	 * search objects
	 * @return boolean
	 * 
	 * @since 6.1.0
	 * @deprecated @see {@link #supportsDSLSearching()}
	 */
	@Deprecated
	@TranslatorProperty(display = "Support Using Lucene Searching", description = "True, assumes objects have Hibernate Search annotations and will use Hiberante Lucene searching", advanced = true)
	public boolean supportsLuceneSearching() {
		return this.supportsLuceneSearching;
	}

	public void setSupportsLuceneSearching(boolean supportsLuceneSearching) {
		this.supportsLuceneSearching = supportsLuceneSearching;
		if (this.supportsLuceneSearching) {
			setSearchType(new LuceneSearch());
		}
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
		if (this.supportsDSLSearching) {
			setSearchType(new DSLSearch());

		}
		this.setSupportsOrderBy(supportsDSLSearching);
	}	

	@Override
	public boolean supportsOrCriteria() {
		return isFullQuerySupported();
	}
	
	/**
	 * see https://issues.jboss.org/browse/TEIID-3573
	 * Discusses issue with trying to support IS NULL and IS NOT NULL;
	 */
	@TranslatorProperty(display="SupportsIsNullCriteria", description="If true, translator can support IsNull criteria ",advanced=true)
	@Override
    public boolean supportsIsNullCriteria() {
		return supportsIsNullCriteria;
	}
	
	public void setSupportsIsNullCriteria(boolean supports) {
		supportsIsNullCriteria = supports;
	}	
	
	
	@TranslatorProperty(display="CompareCriteriaOrdered", description="If true, translator can support comparison criteria with the operator '=>' or '<=' ",advanced=true)
	@Override
	public boolean supportsCompareCriteriaOrdered() {
		return supportsCompareCriteriaOrdered;
	}
	
	
	public void setSupportsCompareCriteriaOrdered(boolean supports) {
		supportsCompareCriteriaOrdered = supports;
	}	
	
	/**
	 *  see https://issues.jboss.org/browse/TEIID-3573
	 * Discusses issue with trying to support NOT;
	 */
	@TranslatorProperty(display="NotCriteria", description="If true, translator can support the NOT operators' ",advanced=true)	
	@Override
	public boolean supportsNotCriteria() {
		return this.supportNotCriteria;
	}
	
	public void setSupportsNotCriteria(boolean supportNot) {
		this.supportNotCriteria = supportNot;
	}

	@Override
	public boolean supportsLikeCriteria() {
		// at this point, i've been unable to get this to work with Lucene searching
		return this.supportsDSLSearching;
	}
	
	@Override
	public boolean supportsLikeCriteriaEscapeCharacter() {
		return this.supportsDSLSearching;
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.translator.ExecutionFactory#supportsOrderByUnrelated()
	 */
	@Override
	public boolean supportsOrderByUnrelated() {
		return this.isFullQuerySupported();
	}	
	
}
