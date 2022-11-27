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

package org.teiid.query.function.metadata;

import org.teiid.metadata.FunctionMethod;
import org.teiid.query.validator.ValidatorFailure;

/**
 * This is a specialized report item for reporting invalid function methods during
 * function metadata validation.  It is overrides ReportItem and adds an additional
 * attribute with the method reference for the invalid method.
 */
public class InvalidFunctionItem extends ValidatorFailure {

    private static final long serialVersionUID = 5679334286895174700L;

    /**
     * Report item type
     */
    public static final String INVALID_FUNCTION = "InvalidFunction"; //$NON-NLS-1$

    private FunctionMethod method;

    /**
     * Constructor for InvalidFunctionItem.
     */
    public InvalidFunctionItem() {
        super(INVALID_FUNCTION);
    }

    /**
     * Construct with invalid function object and exception.
     * @param method Invalid function method object
     * @param message Message describing invalid function
     */
    public InvalidFunctionItem(FunctionMethod method, String message) {
        this();
        setMessage(message);
        setMethod(method);
    }

    /**
     * Gets the method.
     * @return Returns a FunctionMethod
     */
    public FunctionMethod getMethod() {
        return method;
    }

    /**
     * Sets the method.
     * @param method The method to set
     */
    public void setMethod(FunctionMethod method) {
        this.method = method;
    }

}
