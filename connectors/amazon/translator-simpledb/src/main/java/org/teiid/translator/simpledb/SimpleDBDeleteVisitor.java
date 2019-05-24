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

package org.teiid.translator.simpledb;

import java.util.ArrayList;

import org.teiid.language.Delete;
import org.teiid.language.NamedTable;
import org.teiid.language.visitor.HierarchyVisitor;
import org.teiid.metadata.Table;
import org.teiid.translator.TranslatorException;

public class SimpleDBDeleteVisitor extends HierarchyVisitor {

    private Table table;
    private String criteria;
    private ArrayList<TranslatorException> exceptions = new ArrayList<TranslatorException>();

    public SimpleDBDeleteVisitor(Delete delete) {
        visitNode(delete);
    }

    public Table getTable(){
        return this.table;
    }

    public String getCriteria() {
        return this.criteria;
    }

    public void checkExceptions() throws TranslatorException {
        if (!this.exceptions.isEmpty()) {
            throw this.exceptions.get(0);
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
            this.exceptions.add(new TranslatorException(SimpleDBPlugin.Event.TEIID24007, SimpleDBPlugin.Util.gs(SimpleDBPlugin.Event.TEIID24007)));
        }

        visitNode(obj.getTable());
        if (obj.getWhere() != null) {
            this.criteria = SimpleDBSQLVisitor.getSQLString(obj.getWhere());
        }
    }
}
