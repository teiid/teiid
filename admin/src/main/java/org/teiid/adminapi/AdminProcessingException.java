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

package org.teiid.adminapi;

import org.teiid.core.BundleUtil;


/**
 * An <code>AdminProcessingException</code> indicates that an error occured during processing as a result
 * of user input.  This exception is the result of handling an invalid user
 * request, not the result of an internal error.
 *
 * <p>This exception class is capable of containing multiple exceptions.  See
 * {@link AdminException} for details.
 */
public final class AdminProcessingException extends AdminException {

    private static final long serialVersionUID = -878521636838205857L;

    /**
     * No-arg ctor.
     *
     * @since 4.3
     */
    public AdminProcessingException() {
        super();
    }

    /**
     * Construct with a message.
     * @param msg the error message.
     * @since 4.3
     */
    public AdminProcessingException(String msg) {
        super(msg);
    }

    public AdminProcessingException(Throwable cause) {
        super(cause);
    }

    /**
     * Construct with an optional error code and a message.
     * @param code an optional error code
     * @param msg the error message.
     * @since 4.3
     */
    public AdminProcessingException(BundleUtil.Event code, String msg) {
        super(code, msg);
    }

    public AdminProcessingException(String msg, Throwable cause) {
        super(msg, cause);
    }

    public AdminProcessingException(BundleUtil.Event code, Throwable cause, String msg) {
        super(code, cause, msg);
    }

    public AdminProcessingException(BundleUtil.Event code, Throwable cause) {
        super(code, cause);
    }
}
