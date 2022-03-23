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
package org.teiid.translator.dynamodb.execution;

import org.teiid.language.Condition;
import org.teiid.language.Delete;
import org.teiid.language.NamedTable;
import org.teiid.language.visitor.HierarchyVisitor;
import org.teiid.metadata.Table;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.dynamodb.DynamoDBPlugin;

import java.util.ArrayList;
import java.util.List;

public class DynamoDBDeleteVisitor extends HierarchyVisitor {
    private List<TranslatorException> translatorExceptions = new ArrayList<>();
    private Table table;
    private Condition where;

    public Table getTable() {
        return this.table;
    }

    public Condition getWhere() {
        return where;
    }

    public DynamoDBDeleteVisitor(Delete delete) {
        visitNode(delete);
    }

    public void checkExceptions() throws TranslatorException {
        if (!this.translatorExceptions.isEmpty()) {
            throw this.translatorExceptions.get(0);
        }
    }

    @Override
    public void visit(NamedTable obj) {
        super.visit(obj);
        this.table = obj.getMetadataObject();
    }

    @Override
    public void visit(Delete obj) {
        if (obj.getParameterValues() != null) {
            this.translatorExceptions.add(new TranslatorException(
                    DynamoDBPlugin.Event.TEIID32001, DynamoDBPlugin.Util.gs(DynamoDBPlugin.Event.TEIID32001)));
        }

        visitNode(obj.getTable());
        where = obj.getWhere();
    }
}
