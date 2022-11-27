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


/**
 * This exception is thrown when an attempt to log in to obtain a session has failed.
 * Possible reasons include but are not limited to:
 * <p>
 * <ul>
 * <li>The limit on the number of sessions for the user has been reached, and a new session for the user could not be established;
 * <li>An account for the user does not exist, has been frozen or has been removed; and</li>
 * <li>The credentials that were supplied did not authenticate the user.</li>
 * </ul>
 */
public class LogonException extends TeiidSecurityException {

    private static final long serialVersionUID = -4407245748107257061L;

    /**
     * No-Arg Constructor
     */
    public LogonException(  ) {
        super( );
    }

    /**
     * Construct an instance with the message and error code specified.
     *
     * @param message A message describing the exception
     * @param event The error event code
     */
    public LogonException( BundleUtil.Event event, String message ) {
        super(event, message);
    }

    public LogonException( BundleUtil.Event event, Throwable t, String message ) {
        super(event, t, message );
    }

    public LogonException(Throwable t) {
        super(t);
    }
}

