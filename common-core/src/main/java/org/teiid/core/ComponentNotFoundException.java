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
 * Exception which occurs if a system component could not be found by another
 * component.
 */
public class ComponentNotFoundException extends TeiidComponentException {

    private static final long serialVersionUID = 8484545412724259223L;

    /**
     * No-Arg Constructor
     */
    public ComponentNotFoundException(  ) {
        super( );
    }
    /**
     * Construct an instance with the message specified.
     *
     * @param message A message describing the exception
     */
    public ComponentNotFoundException( String message ) {
        super( message );
    }

    public ComponentNotFoundException(BundleUtil.Event code, final String message) {
        super(code, message);
    }

    /**
     * Construct an instance from a message and an exception to chain to this one.
     *
     * @param e An exception to nest within this one
     * @param message A message describing the exception
     */
    public ComponentNotFoundException( Throwable e, String message ) {
        super( e, message );
    }

    /**
     * Construct an instance from a message and a code and an exception to
     * chain to this one.
     *
     * @param e An exception to nest within this one
     * @param message A message describing the exception
     * @param code A code denoting the exception
     */
    public ComponentNotFoundException(BundleUtil.Event code, Throwable e, String message ) {
        super(code, e, message);
    }

} // END CLASS

