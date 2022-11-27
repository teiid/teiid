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

package org.teiid.language;

import org.teiid.language.visitor.LanguageObjectVisitor;

/**
 * Represents a procedure call that returns a table.
 */
public class NamedProcedureCall extends BaseLanguageObject implements TableReference {

    private String correlationName;
    private Call call;
    private boolean lateral;

    public NamedProcedureCall(Call call, String name) {
        this.call = call;
        this.correlationName = name;
    }

    public String getCorrelationName() {
        return this.correlationName;
    }

    public void setCorrelationName(String name) {
        this.correlationName = name;
    }

    public Call getCall() {
        return this.call;
    }

    public void setCall(Call call) {
        this.call = call;
    }

    public void acceptVisitor(LanguageObjectVisitor visitor) {
        visitor.visit(this);
    }

    public boolean isLateral() {
        return lateral;
    }

    public void setLateral(boolean lateral) {
        this.lateral = lateral;
    }

}
