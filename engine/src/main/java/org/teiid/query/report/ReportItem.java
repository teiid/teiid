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

package org.teiid.query.report;

import java.io.Serializable;

/**
 * Represents a single item on a report
 */
public class ReportItem implements Serializable {

    private String type;
    private String message;

    public ReportItem(String type) {
        this.type = type;
    }

    public String getType() {
         return this.type;
    }

    /**
     * Gets the message.
     * @return Returns the message
     */
    public String getMessage() {
        return message;
    }

    /**
     * Sets the message.
     * @param message The message to set
     */
    public void setMessage(String message) {
        this.message = message;
    }

    public String toString() {
        return getType() + ": " + getMessage();     //$NON-NLS-1$
    }

}
