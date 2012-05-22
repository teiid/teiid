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

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.teiid.language.Command;
import org.teiid.language.Select;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.TranslatorException;


/**
 * Execution of the SELECT Command
 */
public class ObjectExecution implements ResultSetExecution {

    private Select query;
    private ObjectSourceProxy proxy;
    private ObjectMethodManager methodManager;
    private ObjectExecutionFactory config;
    private RuntimeMetadata metadata;
    
	private Iterator<List<Object>> resultsIt = null;  
    
    public ObjectExecution(Command query, RuntimeMetadata metadata, ObjectSourceProxy proxy, ObjectMethodManager methodManager, ObjectExecutionFactory factory) {
    	this.query = (Select) query;
        this.proxy = proxy;
        this.methodManager = methodManager;
        this.config = factory;
        this.metadata = metadata;
    }
    
	@Override
    public void execute() throws TranslatorException {

        LogManager.logTrace(LogConstants.CTX_CONNECTOR, "Object executing command: " + query.toString()); //$NON-NLS-1$
        ObjectVisitor visitor = getObjectVisitor();
        
        visitor.visitNode(query);

        List<Object> objects = executeQuery(visitor);

        List<List<Object>> results = null;
		if (objects != null && objects.size() > 0) {
		    LogManager.logDetail(LogConstants.CTX_CONNECTOR, "ObjectExecution number of objects from proxy is : " + objects.size()); //$NON-NLS-1$
		    
			results = ObjectTranslator.translateObjects(objects, visitor, methodManager);
			 
			LogManager.logDetail(LogConstants.CTX_CONNECTOR, "ObjectExecution number of rows from translation : " + results.size()); //$NON-NLS-1$

		} else {
		    LogManager.logDetail(LogConstants.CTX_CONNECTOR, "ObjectExecution number of objects from proxy is : 0"); //$NON-NLS-1$

			results = Collections.emptyList();
		}
		
        this.resultsIt = results.iterator();
    }     
    
	protected List<Object> executeQuery(ObjectVisitor op)
				throws TranslatorException {

	    LogManager.logDetail(LogConstants.CTX_CONNECTOR, "ObjectExecution calling proxy : " + this.proxy.getClass().getName()); //$NON-NLS-1$

		return this.proxy.get(query, config.getCacheName(), op);

	}
	
	protected ObjectVisitor getObjectVisitor() {
		return new ObjectVisitor(this.config,this.metadata);
	}
	
    @Override
    public List<Object> next() throws TranslatorException, DataNotAvailableException {
    	// create and return one row at a time for your resultset.
    	if (resultsIt.hasNext()) {
      		return (List<Object>) resultsIt.next();
    	}
        return null;
    }
    

    @Override
    public void close() {
        if (this.proxy != null) this.proxy.close();
        this.proxy = null;
        this.methodManager = null;
        this.query = null;
        this.metadata = null;
    }

    @Override
    public void cancel() throws TranslatorException {
    	//TODO: initiate the "abort" of execution 
    }

  

}
