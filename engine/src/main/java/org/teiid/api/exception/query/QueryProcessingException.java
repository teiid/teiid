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

import org.teiid.core.BundleUtil;
import org.teiid.core.TeiidProcessingException;

/**
 * This exception is thrown when an error occurs while retrieving metadata
 * from a query component metadata facade.
 */
public class QueryProcessingException extends TeiidProcessingException {

    private static final long serialVersionUID = -1976946369356781737L;

    /**
     * No-arg constructor required by Externalizable semantics.
     */
    public QueryProcessingException() {
        super();
    }

    /**
     * Construct an instance with the message specified.
     *
     * @param message A message describing the exception
     */
    public QueryProcessingException( String message ) {
        super( message );
    }

    public QueryProcessingException(Throwable e) {
        super(e);
    }

    /**
     * Construct an instance from a message and an exception to chain to this one.
     *
     * @param message A message describing the exception
     * @param e An exception to nest within this one
     */
    public QueryProcessingException(Throwable e, String message ) {
        super( e, message );
    }

    public QueryProcessingException(BundleUtil.Event event, Throwable e) {
        super( event, e);
    }

    public QueryProcessingException(BundleUtil.Event event, Throwable e, String msg) {
        super(event, e, msg);
    }

    public QueryProcessingException(BundleUtil.Event event, String msg) {
        super(event, msg);
    }

}
