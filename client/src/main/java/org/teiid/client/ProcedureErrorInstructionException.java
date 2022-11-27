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

package org.teiid.client;

import org.teiid.core.BundleUtil;
import org.teiid.core.TeiidProcessingException;


/**
 * Used to notify the user that the virtual procedure raised an error.
 * @since 4.3
 */
public class ProcedureErrorInstructionException extends TeiidProcessingException {

    private static final long serialVersionUID = 895480748445855790L;

    /**
     *
     * @since 4.3
     */
    public ProcedureErrorInstructionException() {
        super();
    }

    /**
     * @param message
     * @since 4.3
     */
    public ProcedureErrorInstructionException(String message) {
        super(message);
    }

    public ProcedureErrorInstructionException(BundleUtil.Event event, Exception parent) {
        super(event, parent);
    }
}
