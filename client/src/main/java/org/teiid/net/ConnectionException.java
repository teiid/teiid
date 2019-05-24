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

package org.teiid.net;

import org.teiid.core.BundleUtil;
import org.teiid.core.TeiidException;

/**
 * This exception indicates that an error has occurred during connection.  There
 * are many possible reasons for this, but the most likely is a problem with
 * connection parameters.
 */
public class ConnectionException extends TeiidException {
    private static final long serialVersionUID = -5647655775983865084L;

    /**
     * No-Arg Constructor
     */
    public ConnectionException(  ) {
        super( );
    }

    /**
     * @param message
     */
    public ConnectionException(String message) {
        super(message);
    }

    /**
     * @param e
     */
    public ConnectionException(Throwable e) {
        super(e);
    }

    /**
     * @param e
     * @param message
     */
    public ConnectionException(Throwable e, String message) {
        super(e, message);
    }

    public ConnectionException(BundleUtil.Event event, Throwable e, String message) {
        super(event, e, message);
    }
}
