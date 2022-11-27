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

package org.teiid.core;


/**
 * This exception is a superclass for exceptions that are thrown during
 * processing as a result of user input.  This exception is the result
 * of handling a user request, not the result of an internal error.
 */
public class TeiidProcessingException extends TeiidException {

    private static final long serialVersionUID = -4013536109023540872L;

    /**
     * No-arg Constructor
     */
    public TeiidProcessingException() {
        super();
    }

    /**
     * Construct an instance with the message specified.
     *
     * @param message A message describing the exception
     */
    public TeiidProcessingException( String message ) {
        super( message );
    }

    /**
     * Construct an instance with a linked exception specified.
     *
     * @param e An exception to chain to this exception
     */
    public TeiidProcessingException( Throwable e ) {
        super( e );
    }

    /**
     * Construct an instance with the message and error code specified.
     *
     * @param message A message describing the exception
     * @param code The error code
     */
    public TeiidProcessingException(BundleUtil.Event code, Throwable t, String message ) {
        super(code, t, message );
    }

    public TeiidProcessingException(BundleUtil.Event code, final String message) {
        super(code, message);
    }

    public TeiidProcessingException(BundleUtil.Event code, Throwable t) {
        super(code, t);
    }

    /**
     * Construct an instance from a message and an exception to chain to this one.
     *
     * @param message A message describing the exception
     * @param e An exception to nest within this one
     */
    public TeiidProcessingException( Throwable e, String message ) {
        super( e, message );
    }

} // END CLASS

