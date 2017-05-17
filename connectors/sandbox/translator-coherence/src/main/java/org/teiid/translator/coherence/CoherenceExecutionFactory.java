/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
    
    public boolean supportsCompareCriteriaEquals() {
    	return true;
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
