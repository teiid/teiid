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

package org.teiid.translator.coherence;

import java.util.Collections;
import java.util.List;

import javax.resource.cci.ConnectionFactory;

import org.teiid.core.util.ReflectionHelper;
import org.teiid.language.Command;
import org.teiid.language.QueryExpression;
import org.teiid.language.Select;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.resource.adapter.coherence.CoherenceConnection;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.Translator;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.UpdateExecution;


@Translator(name="coherence", description="A Coherence translator")
public class CoherenceExecutionFactory extends ExecutionFactory<ConnectionFactory, CoherenceConnection> {

	public static final int MAX_SET_SIZE = 100;
	   
    private static SourceCacheAdapter cacheTranslator =  null;
    
    private MetadataFactory metadataFactory = null;
	
	
	public CoherenceExecutionFactory() {
		super();
		this.setMaxInCriteriaSize(MAX_SET_SIZE);
		this.setMaxDependentInPredicates(1);
		this.setSourceRequired(false);
		this.setSupportsOrderBy(false);
		this.setSupportsSelectDistinct(false);
		this.setSupportsInnerJoins(true);
		this.setSupportsFullOuterJoins(false);
		this.setSupportsOuterJoins(false);

	}
	
    @Override
    public void start() throws TranslatorException {
    	super.start();    	
    }

    @Override
    public ResultSetExecution createResultSetExecution(QueryExpression command, ExecutionContext executionContext, RuntimeMetadata metadata, CoherenceConnection connection)
    		throws TranslatorException {
     	createCacheTranslator(connection);

    	return new CoherenceExecution((Select)command, metadata, connection, cacheTranslator);
    }    
    
	@Override
	public UpdateExecution createUpdateExecution(Command command, ExecutionContext executionContext, RuntimeMetadata metadata, CoherenceConnection connection) throws TranslatorException {
		createCacheTranslator(connection);

		return new CoherenceUpdateExecution(command, connection, metadata, executionContext, cacheTranslator);
	}   

    
    public List getSupportedFunctions() {
        return Collections.EMPTY_LIST;
    }

    
    @Override
    public void getMetadata(MetadataFactory metadataFactory,
    		CoherenceConnection conn) throws TranslatorException {
    	this.metadataFactory = metadataFactory;
    }
    
    public SourceCacheAdapter getCacheTranslator() {
    	return this.cacheTranslator;
    }
    
    
	private synchronized void createCacheTranslator(CoherenceConnection conn) throws TranslatorException {
		if (cacheTranslator != null) {
			return;
		}
		if (conn.getCacheTranslatorClassName() == null) {
			throw new TranslatorException(
					CoherencePlugin.Util
							.getString("CoherenceVisitor.cacheTranslatorClassName_not_set")); //$NON-NLS-1$
		}

		try {
			String classname = conn.getCacheTranslatorClassName();
			cacheTranslator = (SourceCacheAdapter) ReflectionHelper
					.create(classname,
							null, null);
			
	    	cacheTranslator.setMetadataFactory(metadataFactory);

		} catch (Exception e1) {
			throw new TranslatorException(e1);
		}

	}
  
  
}
