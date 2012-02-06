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


@Translator(name="coherence", description="A Coherence translator")
public class CoherenceExecutionFactory extends ExecutionFactory<ConnectionFactory, CoherenceConnection> {


	public static final int MAX_SET_SIZE = 100;
	
    
    private SourceCacheAdapter cacheTranslator =  null;
	
	
	public CoherenceExecutionFactory() {
		super();
		setMaxInCriteriaSize(MAX_SET_SIZE);
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

    
    public List getSupportedFunctions() {
        return Collections.EMPTY_LIST;
    }

    public boolean supportsCompareCriteriaEquals() {
        return true;
    }

    public boolean supportsInCriteria() {
        return true;
    }
    
    @Override
    public boolean isSourceRequired() {
    	return false;
    }
    
    @Override
    public void getMetadata(MetadataFactory metadataFactory,
    		CoherenceConnection conn) throws TranslatorException {
    	
    	cacheTranslator.setMetadataFactory(metadataFactory);
    }
    
    
	private void createCacheTranslator(CoherenceConnection conn) throws TranslatorException {
		if (conn.getCacheTranslatorClassName() == null) {
			throw new TranslatorException(
					CoherencePlugin.Util
							.getString("CoherenceVisitor.cacheTranslatorClassName_not_set")); //$NON-NLS-1$
		}

		try {
			String classname = conn.getCacheTranslatorClassName();
			this.cacheTranslator = (SourceCacheAdapter) ReflectionHelper
					.create(classname,
							null, null);
		} catch (Exception e1) {
			throw new TranslatorException(e1);
		}

	}
  
  
}
