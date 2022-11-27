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
package org.teiid.translator.mongodb;

import java.util.ArrayList;

import com.mongodb.QueryBuilder;

class ColumnDetail {
    private ArrayList<String> projectedNames = new ArrayList<String>();
    String documentFieldName;
    Object expression;
    boolean partOfGroupBy;
    boolean partOfProject;

    public QueryBuilder getQueryBuilder() {
        QueryBuilder query = QueryBuilder.start(this.projectedNames.get(0));
        if (this.documentFieldName != null) {
            query = QueryBuilder.start(this.documentFieldName);
        }
        return query;
    }

    public QueryBuilder getPullQueryBuilder() {
        if (this.documentFieldName != null) {
            return QueryBuilder.start(this.documentFieldName.substring(this.documentFieldName.lastIndexOf('.')+1));
        }
        return QueryBuilder.start(this.projectedNames.get(0));
    }

    public void addProjectedName(String name) {
        this.projectedNames.add(0, name);
    }

    public String getProjectedName(){
        return this.projectedNames.get(0);
    }

    public boolean hasProjectedName(String name) {
        for(String s: this.projectedNames) {
            if (s.equalsIgnoreCase(name)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Projected Name             = ").append(this.projectedNames).append("\n");  //$NON-NLS-1$//$NON-NLS-2$
        sb.append("Document Field Name  = ").append(this.documentFieldName).append("\n"); //$NON-NLS-1$//$NON-NLS-2$
        sb.append("Expression = ").append(this.expression).append("\n"); //$NON-NLS-1$//$NON-NLS-2$
        return sb.toString();
    }
}