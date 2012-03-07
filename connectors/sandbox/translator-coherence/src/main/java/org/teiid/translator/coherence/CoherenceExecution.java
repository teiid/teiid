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
