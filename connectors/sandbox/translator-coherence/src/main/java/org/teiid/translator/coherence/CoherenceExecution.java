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
import java.util.Iterator;
import java.util.List;

import org.teiid.language.Select;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.resource.adapter.coherence.CoherenceConnection;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.coherence.visitor.CoherenceVisitor;


/**
 * Execution of the SELECT Command
 */
public class CoherenceExecution implements ResultSetExecution {


    private Select query;
    private CoherenceConnection connection;
    
    private Iterator resultsIt = null;
    
    private CoherenceVisitor visitor = null;

    private SourceCacheAdapter sourceCacheTranslator = null;
    
    
    public CoherenceExecution(Select query, RuntimeMetadata metadata, CoherenceConnection connection, SourceCacheAdapter cacheTranslator) {
        this.query = query;
        this.connection = connection;
        this.visitor = new CoherenceVisitor(metadata);
        this.sourceCacheTranslator = cacheTranslator;
    }
    
    @Override
    public void execute() throws TranslatorException {
        // Log our command
        LogManager.logTrace(LogConstants.CTX_CONNECTOR, "Coherence executing command: " + query); //$NON-NLS-1$

    	visitor.visitNode(query);
    	
        if(visitor.getException() != null) { 
            throw visitor.getException();
        }
        
        // Execute url to get results
        List results = executeQuery();
        this.resultsIt = results.iterator();
    }     
    
	protected List executeQuery()
				throws TranslatorException {

		try {
			List<Object> objects = this.connection.get(visitor.getFilter());
					//"Id in (" + parm + ")", this.connection.getCacheName());
			
			if (objects == null)
				return Collections.EMPTY_LIST;
			
			return sourceCacheTranslator.translateObjects(objects, this.visitor);

		} catch (TranslatorException te) {
			throw te;
		} catch (Throwable re) {
			re.printStackTrace();
		}

		return Collections.EMPTY_LIST;
	}
    
    
    
    
    @Override
    public List<?> next() throws TranslatorException, DataNotAvailableException {
    	// create and return one row at a time for your resultset.
    	if (resultsIt.hasNext()) {
    		return (List) resultsIt.next();
    	}
        
        return null;
    }
    

    @Override
    public void close() {
        // TODO:cleanup your execution based resources here
    }

    @Override
    public void cancel() throws TranslatorException {
    	//TODO: initiate the "abort" of execution 
    }

  

}
