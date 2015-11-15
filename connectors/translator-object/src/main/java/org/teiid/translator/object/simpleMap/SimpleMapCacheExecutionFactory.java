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

package org.teiid.translator.object.simpleMap;

import org.teiid.language.Command;
import org.teiid.language.QueryExpression;
import org.teiid.language.Select;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.Translator;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.UpdateExecution;
import org.teiid.translator.object.ObjectConnection;
import org.teiid.translator.object.ObjectExecution;
import org.teiid.translator.object.ObjectExecutionFactory;
import org.teiid.translator.object.ObjectUpdateExecution;
import org.teiid.translator.object.ObjectVisitor;


/**
 * The ObjectExecutionFactory is a base implementation for connecting to an
 * Object cache.  It provides the core features and behavior common to all implementations.
 * 
 * @author vhalbert
 * 
 */
@Translator(name = "map-cache", description = "Translator for managing a cache of Objects")
public class SimpleMapCacheExecutionFactory extends ObjectExecutionFactory {


	public static final int MAX_SET_SIZE = 10000;
	
	public SimpleMapCacheExecutionFactory() {
		setSourceRequiredForMetadata(false);
		setMaxInCriteriaSize(MAX_SET_SIZE);
		setMaxDependentInPredicates(1);

		setSupportsOrderBy(false);
		setSupportsSelectDistinct(false);
		setSupportsInnerJoins(false);
		setSupportsFullOuterJoins(false);
		setSupportsOuterJoins(false);
		
		setSearchType(new SearchByKey());
		
	}
	
	@Override
	public ResultSetExecution createResultSetExecution(QueryExpression command,
			ExecutionContext executionContext, RuntimeMetadata metadata,
			ObjectConnection connection) throws TranslatorException {
		return new ObjectExecution((Select) command, metadata, this, connection, executionContext) {
			@Override
			protected ObjectVisitor createVisitor() {
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
		        return new SimpleKeyVisitor();
		    }  		
    	};
	}	
    
}
