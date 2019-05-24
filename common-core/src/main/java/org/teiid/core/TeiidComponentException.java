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
 * Exception which occurs if an error occurs within the server that is not
 * business-related.  For instance, if a service or bean is not available
 * or communication fails.
 */
public class TeiidComponentException extends TeiidException {

    private static final long serialVersionUID = 5853804556425201591L;

    public TeiidComponentException(  ) {
        super(  );
    }
    /**
     * Construct an instance with the message specified.
     *
     * @param message A message describing the exception
     */
    public TeiidComponentException( String message ) {
        super( message );
    }

    /**
     * Construct an instance with a linked exception specified.
     *
     * @param e An exception to chain to this exception
     */
    public TeiidComponentException( Throwable e ) {
        super( e );
    }

    public TeiidComponentException(BundleUtil.Event code, final String message) {
        super(code, message);
    }

    public TeiidComponentException(BundleUtil.Event code, Throwable e, final String message) {
        super(code, e, message);
    }

    public TeiidComponentException(BundleUtil.Event code, Throwable e) {
        super(code, e);
    }

    public TeiidComponentException( Throwable e, String message ) {
        super( e, message );
    }

} // END CLASS

