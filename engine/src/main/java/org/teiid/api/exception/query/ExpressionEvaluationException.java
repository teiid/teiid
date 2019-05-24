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
 * This exception is thrown when an error occurs while evaluating a SQL expression.
 */
public class ExpressionEvaluationException extends TeiidProcessingException {

    private static final long serialVersionUID = 4955469005442543688L;

    /**
     * No-arg constructor required by Externalizable semantics.
     */
    public ExpressionEvaluationException() {
        super();
    }

    /**
     * Construct an instance with the message specified.
     *
     * @param message A message describing the exception
     */
    public ExpressionEvaluationException( String message ) {
        super( message );
    }

    public ExpressionEvaluationException(Throwable e) {
        super(e);
    }

    /**
     * Construct an instance from a message and an exception to chain to this one.
     *
     * @param message A message describing the exception
     * @param e An exception to nest within this one
     */
    public ExpressionEvaluationException( Throwable e, String message ) {
        super( e, message );
    }

    public ExpressionEvaluationException(BundleUtil.Event event, Throwable e) {
        super( event, e);
    }

    public ExpressionEvaluationException(BundleUtil.Event event, Throwable e, String msg) {
        super(event, e, msg);
    }

    public ExpressionEvaluationException(BundleUtil.Event event, String msg) {
        super(event, msg);
    }
}
