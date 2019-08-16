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

import org.teiid.core.TeiidException;

/**
 * <p> This class is used to store the details of an atomic query warning.
 * It stores model name on which the atomic query is based, name of the
 * connector binding for the data source against which the atomic query
 * is executed, and the actual exception thrown when the atomic
 * query is executed.
 */

public class SourceWarning extends TeiidException {

    public static final StackTraceElement[] EMPTY_STACK_TRACE = new StackTraceElement[0];
    private String modelName = "UNKNOWN"; // variable stores the name of the model for the atomic query //$NON-NLS-1$
    private String connectorBindingName = "UNKNOWN"; // variable stores name of the connector binding //$NON-NLS-1$
    private boolean partialResults;

    /**
     * <p>Constructor that stores atomic query failure details.
     * @param model Name of the model for the atomic query
     * @param connectorBinding Name of the connector binding name for the atomic query
     * @param ex Exception thrown when atomic query fails
     */
    public SourceWarning(String model, String connectorBinding, Throwable ex, boolean partialResults) {
        super(ex);
        if(model != null) {
            this.modelName = model;
        }
        if(connectorBinding != null) {
            this.connectorBindingName = connectorBinding;
        }
        this.partialResults = partialResults;
        setStackTrace(EMPTY_STACK_TRACE);
    }

    /**
     * <p>Get's the model name for the atomic query.
     * @return The name of the model
     */
    public String getModelName() {
        return modelName;
    }

    /**
     * <p>Get's the connector binding name for the atomic query.
     * @return The Connector Binding Name
     */
    public String getConnectorBindingName() {
        return connectorBindingName;
    }

    public boolean isPartialResultsError() {
        return partialResults;
    }

    /**
     * <p>Gets a message detailing the source against which the atomic query failed.
     * @return Message containing details of the source for which there is a failure.
     */
    public String toString() {
        StringBuffer warningBuf = new StringBuffer();
        if (partialResults) {
            warningBuf.append("Error "); //$NON-NLS-1$
        } else {
            warningBuf.append("Warning "); //$NON-NLS-1$
        }
        warningBuf.append("querying the connector with binding name "); //$NON-NLS-1$
        warningBuf.append(connectorBindingName);
        warningBuf.append(" for the model "); //$NON-NLS-1$
        warningBuf.append(modelName);
        warningBuf.append(" : "); //$NON-NLS-1$
        warningBuf.append(this.getCause());
        return warningBuf.toString();
    }

} // END CLASS
