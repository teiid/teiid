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

package com.metamatrix.query.validator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixException;
import com.metamatrix.api.exception.query.QueryMetadataException;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.sql.LanguageObject;
import com.metamatrix.query.sql.LanguageVisitor;
import com.metamatrix.query.sql.lang.Command;
import com.metamatrix.query.sql.lang.Query;
import com.metamatrix.query.sql.symbol.ElementSymbol;

public class AbstractValidationVisitor extends LanguageVisitor {
    
    // Exception handling
    private MetaMatrixComponentException exception;
    private LanguageObject exceptionObject;
        
    // Validation error handling
    private ValidatorReport report;
    
    private QueryMetadataInterface metadata;
    
    protected Command currentCommand;
    
    public AbstractValidationVisitor() {
        this.report = new ValidatorReport();
    }
        
    public void setMetadata(QueryMetadataInterface metadata) {
        this.metadata = metadata;
    }
    
    protected QueryMetadataInterface getMetadata() {
        return this.metadata;
    } 
    
    /**
     * Reset so visitor can be used on a different language object.  This does 
     * not wipe the report.
     */
    public void reset() {
        this.currentCommand = null;
    }
    
    // ######################### Store results info #########################

    protected void handleValidationError(String message) {
        this.report.addItem(new ValidatorFailure(message));
    }

    protected void handleValidationError(String message, LanguageObject invalidObj) {
        this.report.addItem(new ValidatorFailure(message, invalidObj));
    }

    protected void handleValidationError(String message, Collection invalidObjs) {
        this.report.addItem(new ValidatorFailure(message, invalidObjs));
    }

    protected void handleException(MetaMatrixException e) { 
        handleException(e, null);
    }

    protected void handleException(MetaMatrixException e, LanguageObject obj) { 
        // Store exception information
        this.exceptionObject = obj;
        if(e instanceof MetaMatrixComponentException) {
            this.exception = (MetaMatrixComponentException) e;
        } else {
            this.exception = new MetaMatrixComponentException(e);
        }    
        
        // Abort the validation process
        setAbort(true);
    }

    // ######################### Report results info #########################

    public MetaMatrixComponentException getException() { 
        return this.exception;
    }
    
    public LanguageObject getExceptionObject() { 
        return this.exceptionObject;
    }
    
    public ValidatorReport getReport() { 
        return this.report;
    }
    
    // ######################### Helper methods for validation #########################
    /**
	 * Check to verify if the query would return XML results.
     * @param query the query to check
	 */
	protected boolean isXMLCommand(Command command) {
		if (command instanceof Query) {
		    return ((Query)command).getIsXML();
        }
        return false;
	}   
	
    protected Collection validateElementsSupport(Collection elements, int supportsFlag) {
	    // Collect any identifiers not supporting flag
	    List dontSupport = null;  
        ElementSymbol symbol = null;              

        try {
	        Iterator elemIter = elements.iterator();
            while(elemIter.hasNext()) {
		    symbol = (ElementSymbol) elemIter.next();
               if(! getMetadata().elementSupports(symbol.getMetadataID(), supportsFlag)) {
                    if(dontSupport == null) { 
                        dontSupport = new ArrayList();
                    } 
                    dontSupport.add(symbol);    
                }            
		    }
        } catch(QueryMetadataException e) {
            handleException(e, symbol);
        } catch(MetaMatrixComponentException e) { 
            handleException(e, symbol);
        }    

        return dontSupport;
    }

}

