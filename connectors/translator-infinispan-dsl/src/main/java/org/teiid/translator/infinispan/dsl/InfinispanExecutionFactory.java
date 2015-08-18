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

package org.teiid.translator.infinispan.dsl;

import java.util.List;

import javax.resource.cci.ConnectionFactory;

import org.teiid.language.Command;
import org.teiid.language.Delete;
import org.teiid.language.QueryExpression;
import org.teiid.language.Select;
import org.teiid.language.Update;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.MetadataProcessor;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.Translator;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.UpdateExecution;
import org.teiid.translator.TranslatorProperty;
import org.teiid.translator.infinispan.dsl.metadata.ProtobufMetadataProcessor;



/**
 * InfinispanExecutionFactory is the translator that will be use to translate  a remote Infinispan cache and issue queries
 * using DSL to query the cache.  
 * 
 * @author vhalbert
 * 
 * @since 8.7
 *
 */
@Translator(name = "infinispan-cache-dsl", description = "The Infinispan Translator Using DSL to Query Cache")
public class InfinispanExecutionFactory extends
		ExecutionFactory<ConnectionFactory, InfinispanConnection> {

	public static final int MAX_SET_SIZE = 10000;
	
	private boolean supportsCompareCriteriaOrdered = false;
	
	public InfinispanExecutionFactory() {
		super();
		setSourceRequiredForMetadata(true);
		setMaxInCriteriaSize(MAX_SET_SIZE);
		setMaxDependentInPredicates(1);

		setSupportsOrderBy(true);
		setSupportsSelectDistinct(false);
		setSupportsInnerJoins(true);
		setSupportsFullOuterJoins(false);
		setSupportsOuterJoins(true);
		
	
		setSupportedJoinCriteria(SupportedJoinCriteria.KEY);
	}

	@Override
	public int getMaxFromGroups() {
		return 2;
	}

	@Override
	public ResultSetExecution createResultSetExecution(QueryExpression command,
			ExecutionContext executionContext, RuntimeMetadata metadata,
			InfinispanConnection connection) throws TranslatorException {
		return new InfinispanExecution((Select) command, metadata, this, connection, executionContext);
	}
	
    @Override
	public UpdateExecution createUpdateExecution(Command command,
			ExecutionContext executionContext, RuntimeMetadata metadata,
			InfinispanConnection connection) throws TranslatorException {
    	// if no primary key defined,then updates are not enabled
    	if (connection.getPkField() == null) {
			throw new TranslatorException(InfinispanPlugin.Util.gs(InfinispanPlugin.Event.TEIID25007, new Object[] {connection.getCacheName()}));
    	}
    	return new InfinispanUpdateExecution(command, connection, executionContext, this);
	}
    

	@Override
    public boolean supportsAliasedTable() {
        return true;
    }

	@Override
    public boolean supportsInCriteria() {
		return Boolean.TRUE.booleanValue();
	}

	/**
	 * @see @link https://issues.jboss.org/browse/TEIID-3573
	 * Discusses issue with trying to support IS NULL and IS NOT NULL;
	 */
	@Override
    public boolean supportsIsNullCriteria() {
		return Boolean.FALSE.booleanValue();
	}
	
	@Override
	public boolean supportsOrCriteria() {
		return Boolean.TRUE.booleanValue();
	}

	@Override
    public boolean supportsCompareCriteriaEquals() {
		return Boolean.TRUE.booleanValue();
	}

	@TranslatorProperty(display="CompareCriteriaOrdered", description="If true, translator can support comparison criteria with the operator '=>' or '<=' ",advanced=true)
	@Override
	public boolean supportsCompareCriteriaOrdered() {
		return supportsCompareCriteriaOrdered;
	}
	
	public boolean setSupportsCompareCriteriaOrdered(boolean supports) {
		return supportsCompareCriteriaOrdered = supports;
	}
	
	@Override
	public boolean supportsLikeCriteria() {
		return Boolean.TRUE.booleanValue();
	}	

	@Override
	public boolean supportsLikeCriteriaEscapeCharacter() {
		return Boolean.TRUE.booleanValue();
	}	
	
	/**
	 * @see @link https://issues.jboss.org/browse/TEIID-3573
	 * Discusses issue with trying to support NOT;
	 */
	@Override
	public boolean supportsNotCriteria() {
		return Boolean.FALSE.booleanValue();
	}

	public List<Object> search(Select command, String cacheName,
			InfinispanConnection connection, ExecutionContext executionContext)
			throws TranslatorException {
		return DSLSearch.performSearch(command, cacheName, connection);
	}
	
	public List<Object> search(Delete command, String cacheName, InfinispanConnection conn, ExecutionContext executionContext)
				throws TranslatorException {   
		return DSLSearch.performSearch(command, cacheName, conn);
	}
	
	public List<Object> search(Update command, String cacheName, InfinispanConnection conn, ExecutionContext executionContext)
			throws TranslatorException {   
		return DSLSearch.performSearch(command, cacheName, conn);
	}	
	
	public Object performKeySearch(String cacheName, String columnName, Object value, InfinispanConnection conn, ExecutionContext executionContext) throws TranslatorException {
		return DSLSearch.performKeySearch(cacheName, columnName, value, conn);
	}	

	@Override
    public MetadataProcessor<InfinispanConnection> getMetadataProcessor(){
	    return new ProtobufMetadataProcessor();
	}
}
