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

package org.teiid.client.security;

import org.teiid.core.BundleUtil;
import org.teiid.core.TeiidProcessingException;

public class TeiidSecurityException extends TeiidProcessingException {

    /**
     * No-Arg Constructor
     */
    public TeiidSecurityException(  ) {
        super( );
    }

    /**
     * Construct an instance with a linked exception, and an error code and
     * message, specified.
     *
     * @param t       An exception to chain to this exception
     * @param message The error message
     * @param code    The error code
     */
    public TeiidSecurityException(BundleUtil.Event code, Throwable t, String message ) {
        super(code, t, message);
    }
    /**
     * Construct an instance with an error code and message specified.
     *
     * @param message The error message
     * @param code    The error code
     */
    public TeiidSecurityException(BundleUtil.Event code, String message ) {
        super( code, message );
    }
    /**
     * Constructs an instance of the exception with the specified detail message
     * and a single exception. A detail message is a String that describes this
     * particular exception.
     * @param message the detail message
     * @param e the exception that is encapsulated by this exception
     */
    public TeiidSecurityException( Throwable e, String message ) {
        super(e, message);
    }
    /**
     * Constructs an instance of the exception with no detail message but with a
     * single exception.
     * @param e the exception that is encapsulated by this exception
     */
    public TeiidSecurityException(Throwable e) {
        super(e);
    }
    /**
     * Constructs an instance of the exception with the specified detail message. A detail
     * message is a String that describes this particular exception.
     * @param message the detail message
     */
    public TeiidSecurityException(String message) {
        super(message);
    }
}

