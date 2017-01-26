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

import org.teiid.language.Argument;
import org.teiid.language.Command;
import org.teiid.language.QueryExpression;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.MetadataProcessor;
import org.teiid.translator.ProcedureExecution;
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
	private boolean searchabilityBasedOnAnnotations = false;
	
	public ObjectExecutionFactory() {
		setSourceRequiredForMetadata(true);
		setMaxInCriteriaSize(MAX_SET_SIZE);
		setMaxDependentInPredicates(1);

		setSupportsOrderBy(false);
		setSupportsSelectDistinct(false);
		setSupportsInnerJoins(false);
		setSupportsFullOuterJoins(false);
		setSupportsOuterJoins(false);
			
	}
	
	@Override
	public boolean isSourceRequiredForCapabilities() {
		return true;
	}
	
	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.translator.ExecutionFactory
	 */
	@Override
	public void start() throws TranslatorException {
		super.start();
	}

	@Override
	public ResultSetExecution createResultSetExecution(QueryExpression command,
			ExecutionContext executionContext, RuntimeMetadata metadata,
			ObjectConnection connection) throws TranslatorException {
		return new ObjectExecution(command, this, connection, executionContext);
	}
	
    @Override
	public UpdateExecution createUpdateExecution(Command command,
			ExecutionContext executionContext, RuntimeMetadata metadata,
			ObjectConnection connection)  {
    	return new ObjectUpdateExecution(command, connection, executionContext, this);
	}
    
	@Override
	public ProcedureExecution createDirectExecution(List<Argument> arguments, Command command, ExecutionContext executionContext, RuntimeMetadata metadata, ObjectConnection connection)  {
		return new ObjectDirectExecution(arguments, command, connection, executionContext, this);
	}   
	
	public boolean supportsSearchabilityUsingAnnotations() {
		return searchabilityBasedOnAnnotations;
	}	
	
	public void setSupportsSearchabilityUsingAnnotations(boolean useAnnotations) {
		this.searchabilityBasedOnAnnotations = useAnnotations;
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
    public MetadataProcessor<ObjectConnection> getMetadataProcessor(){
	    return new JavaBeanMetadataProcessor(supportsSearchabilityUsingAnnotations());
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
	
	@Override
	public void initCapabilities(ObjectConnection connection) throws TranslatorException {
		if (connection == null) {
			return;
		}

		this.setSupportsSearchabilityUsingAnnotations(connection.configuredUsingAnnotations());

	}
	
	@Override
	public boolean supportsUpsert() {
	    return true;
	}
}
