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

package org.teiid.query.validator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Stack;

import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidException;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.symbol.ElementSymbol;


public class AbstractValidationVisitor extends LanguageVisitor {

    // Exception handling
    private TeiidComponentException exception;
    private LanguageObject exceptionObject;

    // Validation error handling
    protected ValidatorReport report;

    private QueryMetadataInterface metadata;

    protected Command currentCommand;
    protected Stack<LanguageObject> stack = new Stack<LanguageObject>();

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
        this.stack.clear();
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

    protected void handleException(TeiidException e) {
        handleException(e, null);
    }

    protected void handleException(TeiidException e, LanguageObject obj) {
        // Store exception information
        this.exceptionObject = obj;
        if(e instanceof TeiidComponentException) {
            this.exception = (TeiidComponentException) e;
        } else {
            this.exception = new TeiidComponentException(e);
        }

        // Abort the validation process
        setAbort(true);
    }

    // ######################### Report results info #########################

    public TeiidComponentException getException() {
        return this.exception;
    }

    public LanguageObject getExceptionObject() {
        return this.exceptionObject;
    }

    public ValidatorReport getReport() {
        return this.report;
    }

    // ######################### Helper methods for validation #########################

    protected Collection<ElementSymbol> validateElementsSupport(Collection<ElementSymbol> elements, int supportsFlag) {
        // Collect any identifiers not supporting flag
        List<ElementSymbol> dontSupport = null;
        ElementSymbol symbol = null;

        try {
            Iterator<ElementSymbol> elemIter = elements.iterator();
            while(elemIter.hasNext()) {
            symbol = elemIter.next();
               if(! getMetadata().elementSupports(symbol.getMetadataID(), supportsFlag)) {
                    if(dontSupport == null) {
                        dontSupport = new ArrayList<ElementSymbol>();
                    }
                    dontSupport.add(symbol);
                }
            }
        } catch(QueryMetadataException e) {
            handleException(e, symbol);
        } catch(TeiidComponentException e) {
            handleException(e, symbol);
        }

        return dontSupport;
    }

}

