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

package org.teiid.deployers;

import org.teiid.core.BundleUtil;
import org.teiid.core.TeiidProcessingException;
/**
 * The base exception from which all Runtime Metadata Exceptions extend.
 */
public class VirtualDatabaseException extends TeiidProcessingException {

    private static final long serialVersionUID = -6654557123904497650L;
    public static final String NO_MODELS = "1"; //$NON-NLS-1$
    public static final String MODEL_NON_DEPLOYABLE_STATE = "2";  //$NON-NLS-1$
    public static final String VDB_NON_DEPLOYABLE_STATE = "3";  //$NON-NLS-1$

    /**
     * No-arg costructor required by Externalizable semantics
     */
    public VirtualDatabaseException() {
        super();
    }

    /**
     * Construct an instance with the message specified.
     *
     * @param message A message describing the exception
     */
    public VirtualDatabaseException( String message ) {
        super( message );
    }

    /**
     * Construct an instance with the message and error code specified.
     *
     * @param message A message describing the exception
     * @param code The error code
     */
    public VirtualDatabaseException(BundleUtil.Event code, String message ) {
        super(code, message );
    }

    /**
     * Construct an instance from an exception to chain to this one.
     *
     * @param e An exception to nest within this one
     */
    public VirtualDatabaseException(Exception e) {
        super(e);
    }
    /**
     * Construct an instance from a message and an exception to chain to this one.
     *
     * @param e An exception to nest within this one
     */
    public VirtualDatabaseException( Exception e, String message ) {
        super( e, message );
    }

    /**
     * Construct an instance from a message and a code and an exception to
     * chain to this one.
     *
     * @param e An exception to nest within this one
     * @param message A message describing the exception
     * @param event A code denoting the exception
     */
    public VirtualDatabaseException(BundleUtil.Event event, Exception e, String message ) {
        super(event, e, message );
    }
}

