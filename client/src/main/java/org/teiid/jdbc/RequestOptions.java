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

package org.teiid.jdbc;


public class RequestOptions {

    /**
     * true indicates that the query should be re-executed upon completion, such that
     * the current Executions and CommandContext are reused.
     * Continuous queries must be forward-only and return a result set.
     */
    private boolean continuous;

    public boolean isContinuous() {
        return continuous;
    }

    public RequestOptions continuous(boolean isContinuous) {
        this.continuous = isContinuous;
        return this;
    }

    public void setContinuous(boolean continuous) {
        this.continuous = continuous;
    }

}
