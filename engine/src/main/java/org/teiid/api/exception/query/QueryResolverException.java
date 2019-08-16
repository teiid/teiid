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

package org.teiid.api.exception.query;

import java.util.ArrayList;
import java.util.List;

import org.teiid.core.BundleUtil;

/**
 * This exception represents the case where the query submitted could not resolved
 * when it is checked against the metadata
 */
public class QueryResolverException extends QueryProcessingException {

    private static final long serialVersionUID = 752912934870580744L;
    private transient List<UnresolvedSymbolDescription> problems;

    /**
     * No-arg constructor required by Externalizable semantics.
     */
    public QueryResolverException() {
        super();
    }

    /**
     * Construct an instance with the message specified.
     *
     * @param message A message describing the exception
     */
    public QueryResolverException( String message ) {
        super( message );
    }

    public QueryResolverException(Throwable e) {
        super(e);
    }

    /**
     * Construct an instance from a message and an exception to chain to this one.
     *
     * @param message A message describing the exception
     * @param e An exception to nest within this one
     */
    public QueryResolverException( Throwable e, String message ) {
        super( e, message );
    }

    public QueryResolverException(BundleUtil.Event event, Throwable e) {
        super( event, e);
    }

    public QueryResolverException(BundleUtil.Event event, Throwable e, String msg) {
        super(event, e, msg);
    }

    public QueryResolverException(BundleUtil.Event event, String msg) {
        super(event, msg);
    }

    /**
     * Set the list of unresolved symbols during QueryResolution
     * @param unresolvedSymbols List of &lt;UnresolvedSymbolDescription&gt; objects
     */
    public void setUnresolvedSymbols(List<UnresolvedSymbolDescription> unresolvedSymbols) {
        this.problems = unresolvedSymbols;
    }

    /**
     * Add an UnresolvedSymbolDescription to the list of unresolved symbols
     * @param symbolDesc Single description
     */
    public void addUnresolvedSymbol(UnresolvedSymbolDescription symbolDesc) {
        if(this.problems == null) {
            this.problems = new ArrayList<UnresolvedSymbolDescription>();
        }
        this.problems.add(symbolDesc);
    }

    /**
     * Set the list of unresolved symbols during QueryResolution
     * @return List of {@link UnresolvedSymbolDescription} objects
     */
    public List<UnresolvedSymbolDescription> getUnresolvedSymbols() {
        return this.problems;
    }
}
