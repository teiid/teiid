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

import java.util.Iterator;
import java.util.List;

import org.teiid.language.visitor.LanguageObjectVisitor;

/**
 * Represents a DELETE command.
 */
public class Delete extends BaseLanguageObject implements BulkCommand {

    private NamedTable table;
    private Condition where;
    private Iterator<? extends List<?>> parameterValues;

    public Delete(NamedTable group, Condition criteria) {
        this.table = group;
        this.where = criteria;
    }

    /**
     * Get group that is being deleted from.
     * @return Insert group
     */
    public NamedTable getTable() {
        return table;
    }

    /**
     * Get criteria that is being used with the delete - may be null
     * @return Criteria, may be null
     */
    public Condition getWhere() {
        return where;
    }

    public void acceptVisitor(LanguageObjectVisitor visitor) {
        visitor.visit(this);
    }

    /**
     * Set group that is being deleted from.
     * @param group Insert group
     */
    public void setTable(NamedTable group) {
        this.table = group;
    }

    /**
     * Set criteria that is being used with the delete - may be null
     * @param criteria Criteria, may be null
     */
    public void setWhere(Condition criteria) {
        this.where = criteria;
    }

    @Override
    public Iterator<? extends List<?>> getParameterValues() {
        return parameterValues;
    }

    public void setParameterValues(Iterator<? extends List<?>> parameterValues) {
        this.parameterValues = parameterValues;
    }

}
