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

package org.teiid.translator.object;

import java.util.List;

import javax.resource.cci.ConnectionFactory;

import org.teiid.language.Command;
import org.teiid.language.QueryExpression;
import org.teiid.language.Select;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.MetadataProcessor;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.UpdateExecution;
import org.teiid.translator.object.metadata.JavaBeanMetadataProcessor;


/**
 * The ObjectExecutionFactory is a base implementation for connecting to an
 * Object cache.  It provides the core features and behavior common to all implementations.
 * 
 * @author vhalbert
 * 
 */
public abstract class ObjectExecutionFactory extends
		ExecutionFactory<ConnectionFactory, ObjectConnection> {

	public static final int MAX_SET_SIZE = 10000;
	private SearchType searchType=null;
	
	public ObjectExecutionFactory() {
		setSourceRequiredForMetadata(false);
		setMaxInCriteriaSize(MAX_SET_SIZE);
		setMaxDependentInPredicates(1);

		setSupportsOrderBy(false);
		setSupportsSelectDistinct(false);
		setSupportsInnerJoins(false);
		setSupportsFullOuterJoins(false);
		setSupportsOuterJoins(false);
		
	}
	
	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.translator.ExecutionFactory#start()
	 */
	@Override
	public void start() throws TranslatorException {
		if (searchType == null) throw new TranslatorException("Programming Error: Search Type was not set");
		super.start();
	}

	@Override
	public ResultSetExecution createResultSetExecution(QueryExpression command,
			ExecutionContext executionContext, RuntimeMetadata metadata,
			ObjectConnection connection) throws TranslatorException {
		return new ObjectExecution((Select) command, metadata, this, connection, executionContext);
	}
	
    @Override
	public UpdateExecution createUpdateExecution(Command command,
			ExecutionContext executionContext, RuntimeMetadata metadata,
			ObjectConnection connection) throws TranslatorException {
    	return new ObjectUpdateExecution(command, connection, executionContext, this);
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
	
	public void setSearchType(SearchType type) {
		this.searchType = type;
	}
	
	public SearchType getSearchType() {
		return this.searchType;
	}
	
	@Override
    public MetadataProcessor<ObjectConnection> getMetadataProcessor(){
	    return new JavaBeanMetadataProcessor();
	}
	
	public List<Object> search(ObjectVisitor visitor, ObjectConnection connection, ExecutionContext executionContext)
			throws TranslatorException {
		return searchType.performSearch(visitor,connection);
	}
	
	/**
	 * The searchByKey is used by update operations that need to obtain a specific object, but don't need
	 * to create a Select command in order to find a single object.
	 * @param columnName
	 * @param value
	 * @param connection
	 * @param executionContext
	 * @return Object
	 * @throws TranslatorException
	 */
	public Object performKeySearch(String columnName, Object value, ObjectConnection connection, ExecutionContext executionContext) throws TranslatorException {
		return searchType.performKeySearch(columnName, value, connection);
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.translator.ExecutionFactory#supportsRowLimit()
	 */
	@Override
	public boolean supportsRowLimit() {
		return true;
	}



}
