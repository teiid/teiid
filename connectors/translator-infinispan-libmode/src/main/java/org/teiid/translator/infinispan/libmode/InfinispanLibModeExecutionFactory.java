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
package org.teiid.translator.infinispan.libmode;

import org.teiid.language.Command;
import org.teiid.language.QueryExpression;
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
import org.teiid.translator.object.simpleMap.SimpleKeyVisitor;
import org.teiid.util.Version;

/**
 * InfinispanExecutionFactory is the "infinispan-lib-mode" translator that is used to access an Infinispan cache running in library mode.
 * <p>
 * The optional setting is:
 * <li>{@link #supportsDSLSearching DSL Searching} - will default to <code>true</code>, to support only Key searching set to false.
 * Set to <code>true</code> will use the Infinispan DSL query language to search the cache for objects</li> 
 * </li>
 * 
 * @author vhalbert
 *
 */
@Translator(name = "ispn-lib-mode", description = "Translator used for accessing Infinispan cache running in Library Mode ")
public class InfinispanLibModeExecutionFactory extends ObjectExecutionFactory {
	public static final Version SIX_6 = Version.getVersion("6.6"); //$NON-NLS-1$

	// max available without having to try to override 
	// BooleanQuery.setMaxClauseCount(), and
	// infinispan doesn't currently support that option.
	// https://issues.jboss.org/browse/ISPN-6728
	public static final int MAX_SET_SIZE = 1024;

	private boolean supportsDSLSearching = true;
	
	private boolean supportsCompareCriteriaOrdered = false;
	private boolean supportNotCriteria = false;
	private boolean supportsIsNullCriteria = false;
	

	public InfinispanLibModeExecutionFactory() {
		super();
		setSourceRequiredForMetadata(true);
		setMaxInCriteriaSize(MAX_SET_SIZE);
		setMaxDependentInPredicates(5);
		
		setSupportsSelectDistinct(false);
		setSupportsInnerJoins(true);
		setSupportsFullOuterJoins(false);
		setSupportsOuterJoins(true);
		
		setSupportsDSLSearching(true);
		
		setSupportedJoinCriteria(SupportedJoinCriteria.EQUI);

	} 
	
	@Override
	public int getMaxFromGroups() {
		return 1;
	}
	
	public boolean isFullQuerySupported() {
		return this.supportsDSLSearching ;
	}
	
	@Override
	public ResultSetExecution createResultSetExecution(QueryExpression command,
			ExecutionContext executionContext, RuntimeMetadata metadata,
			ObjectConnection connection) throws TranslatorException {
		return new ObjectExecution(command, this, connection, executionContext) {
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
			ObjectConnection connection) {
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

	/*
	 * Indicates if Infinispan DSL Querying is used for searching	 
	 * @return boolean
	 * 
	 * @since 6.1.0
	 */
	@TranslatorProperty(display = "Support Using DSL Searching [default=true]", description = "True means Infinispan DSL Querying is used for searching [default=true] ", advanced = true)
	public boolean supportsDSLSearching() {
		return this.supportsDSLSearching;
	}
	
	public void setSupportsDSLSearching(boolean supportsDSLSearching) {
		this.supportsDSLSearching = supportsDSLSearching;
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
	
	@Override
	public void initCapabilities(ObjectConnection connection)
			throws TranslatorException {
		super.initCapabilities(connection);
		if (connection == null) {
			return;
		}

		Version version = connection.getVersion();
		// any version prior to JDG 6.6 the supportCompareCritiaOrdered needs to be set to false;
		if (version != null && version.compareTo(SIX_6) >= 0) {
			this.supportsCompareCriteriaOrdered = true;
		}
	}

	
}
