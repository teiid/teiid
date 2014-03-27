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

import org.teiid.language.QueryExpression;
import org.teiid.language.Select;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.*;
import org.teiid.translator.object.metadata.JavaBeanMetadataProcessor;


/**
 * The ObjectExecutionFactory is a base implementation for connecting to an
 * Object cache.  It provides the core features and behavior common to all implementations.
 * 
 * @author vhalbert
 * 
 */
@Translator(name = "map-cache", description = "Searches a Map for Objects")
public class ObjectExecutionFactory extends
		ExecutionFactory<ConnectionFactory, ObjectConnection> {

	public static final int MAX_SET_SIZE = 10000;

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

	@Override
	public ResultSetExecution createResultSetExecution(QueryExpression command,
			ExecutionContext executionContext, RuntimeMetadata metadata,
			ObjectConnection connection) throws TranslatorException {
		return new ObjectExecution((Select) command, metadata, this, connection, executionContext);
	}

	@Override
	public boolean supportsInnerJoins() {
		return false;
	}

	@Override
	public boolean supportsOuterJoins() {
		return false;
	}

	@Override
	public boolean supportsFullOuterJoins() {
		return false;
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
	public  List<Object> search(Select command, String cacheName, ObjectConnection connection,ExecutionContext executionContext) throws TranslatorException {
		  SearchByKey sbk = new SearchByKey();
		  Class<?> type = connection.getType(cacheName);
		  return sbk.search(command, type, cacheName, connection.getCacheContainer());		
	}

	@Override
    public MetadataProcessor<ObjectConnection> getMetadataProcessor(){
	    return new JavaBeanMetadataProcessor();
	}
}
