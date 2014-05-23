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

import org.teiid.language.QueryExpression;
import org.teiid.language.Select;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.MetadataProcessor;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.Translator;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TranslatorProperty;
import org.teiid.translator.infinispan.dsl.metadata.ProtobufMetadataProcessor;



/**
 * InfinispanExecutionFactory is the translator that will access a remote Infinispan cache and issue queries
 * using DSL to query the cache.  
 * 
 * @author vhalbert
 * 
 * @since 8.8
 *
 */
@Translator(name = "infinispan-cache-dsl", description = "The Infinispan Translator Using DSL to Query Cache")
public class InfinispanExecutionFactory extends
		ExecutionFactory<ConnectionFactory, InfinispanConnection> {

	public static final int MAX_SET_SIZE = 10000;
	
	private String messageDescriptor;

	public InfinispanExecutionFactory() {
		setSourceRequiredForMetadata(false);
		setMaxInCriteriaSize(MAX_SET_SIZE);
		setMaxDependentInPredicates(1);

		setSupportsOrderBy(false);
		setSupportsSelectDistinct(false);
		setSupportsInnerJoins(false);
		setSupportsFullOuterJoins(false);
		setSupportsOuterJoins(false);
	}

	@Override
	public ResultSetExecution createResultSetExecution(QueryExpression command,
			ExecutionContext executionContext, RuntimeMetadata metadata,
			InfinispanConnection connection) throws TranslatorException {
		return new InfinispanExecution((Select) command, metadata, this, connection, executionContext);
	}
	
	/**
	 * Returns the name of the message descriptor to be found in the ProfoBuf that describes
	 * the object to be serialized.
	 * 
	 * @return String MessageDescriptor
	 * 
	 * @since 6.1.0
	 */
	@TranslatorProperty(display = "Protobuf Message Descriptor", description = "The name of the message descriptor to get from the Protobuf")
	public String getMessageDescriptor() {
		return this.messageDescriptor;
	}

	public void setMessageDescriptor(String messageDescriptor) {
		this.messageDescriptor = messageDescriptor;
	}

	@Override
    public boolean supportsCompareCriteriaEquals() {
		return true;
	}

	@Override
    public boolean supportsInCriteria() {
		return true;
	}

	@Override
	public boolean supportsOnlyLiteralComparison() {
		return true;
	}
	
	@Override
	public boolean supportsOrCriteria() {
		return Boolean.TRUE.booleanValue();
	}
	
	@Override
	public boolean supportsCompareCriteriaOrdered() {
		return Boolean.TRUE.booleanValue();
	}
	
	@Override
	public boolean supportsLikeCriteria() {
		// at this point, i've been unable to get the Like to work.
		return Boolean.TRUE.booleanValue();
	}	
	
	public List<Object> search(Select command, String cacheName,
			InfinispanConnection connection, ExecutionContext executionContext)
			throws TranslatorException {

			return DSLSearch.performSearch(command, connection.getType(cacheName), cacheName, connection);
	}

	@Override
    public MetadataProcessor<InfinispanConnection> getMetadataProcessor(){
	    return new ProtobufMetadataProcessor();
	}
}
