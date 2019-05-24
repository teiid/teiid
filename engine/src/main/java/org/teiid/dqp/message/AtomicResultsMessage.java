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

package org.teiid.dqp.message;

import java.util.List;

import org.teiid.translator.CacheDirective.Scope;


public class AtomicResultsMessage {

    private List<?>[] results;

    // Final row index in complete result set, if known
    private long finalRow = -1;

    // by default we support implicit close.
    private boolean supportsImplicitClose = true;

    private List<Exception> warnings;

    private Scope scope;

    // to honor the externalizable contract
    public AtomicResultsMessage() {
    }

    public AtomicResultsMessage(List<?>[] results) {
        this.results = results;
    }

    public boolean supportsImplicitClose() {
        return this.supportsImplicitClose;
    }

    public void setSupportsImplicitClose(boolean supportsImplicitClose) {
        this.supportsImplicitClose = supportsImplicitClose;
    }

    public long getFinalRow() {
        return finalRow;
    }

    public void setFinalRow(long i) {
        finalRow = i;
    }

    public List[] getResults() {
        return results;
    }

    public void setWarnings(List<Exception> warnings) {
        this.warnings = warnings;
    }

    public List<Exception> getWarnings() {
        return warnings;
    }

    public void setScope(Scope scope) {
        this.scope = scope;
    }

    public Scope getScope() {
        return scope;
    }
}
