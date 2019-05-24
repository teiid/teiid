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

package org.teiid.adminapi.impl;

import java.util.Date;

import org.teiid.adminapi.Transaction;


public class TransactionMetadata extends AdminObjectImpl implements Transaction {

    private static final long serialVersionUID = -8588785315218789068L;
    private String associatedSession;
    private String scope;
    private String id;
    private long createdTime;

    @Override
    public String getAssociatedSession() {
        return associatedSession;
    }

    public void setAssociatedSession(String associatedSession) {
        this.associatedSession = associatedSession;
    }

    @Override
    public String getScope() {
        return scope;
    }

    public void setScope(String scope) {
        this.scope = scope;
    }

    @Override
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    @Override
    public long getCreatedTime() {
        return createdTime;
    }

    public void setCreatedTime(long time) {
        this.createdTime = time;
    }

    @Override
    public String toString() {
        StringBuffer result = new StringBuffer();
        result.append("Associated Session:").append(associatedSession); //$NON-NLS-1$
        result.append("Scope:").append(scope); //$NON-NLS-1$
        result.append("Id:").append(id); //$NON-NLS-1$
        result.append("CreatedTime:").append(new Date(createdTime)); //$NON-NLS-1$
        return result.toString();
    }

}
