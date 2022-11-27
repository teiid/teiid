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
import java.util.HashMap;
import java.util.Map;

import org.teiid.language.Array;
import org.teiid.language.Literal;
import org.teiid.language.SetClause;
import org.teiid.language.Update;
import org.teiid.language.visitor.HierarchyVisitor;
import org.teiid.metadata.Column;
import org.teiid.metadata.Table;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.simpledb.api.SimpleDBDataTypeManager;

public class SimpleDBUpdateVisitor extends HierarchyVisitor{
    private Table table;
    private Map<String, Object> attributes = new HashMap<String, Object>();
    private String criteria;
    private ArrayList<TranslatorException> exceptions = new ArrayList<TranslatorException>();

    public SimpleDBUpdateVisitor(Update update) {
        visitNode(update);
    }

    public void checkExceptions() throws TranslatorException {
        if (!this.exceptions.isEmpty()) {
            throw this.exceptions.get(0);
        }
    }

    @Override
    public void visit(Update obj) {
        if (obj.getParameterValues() != null) {
            this.exceptions.add(new TranslatorException(SimpleDBPlugin.Event.TEIID24006, SimpleDBPlugin.Util.gs(SimpleDBPlugin.Event.TEIID24006)));
        }

        this.table = obj.getTable().getMetadataObject();
        for(SetClause setClause : obj.getChanges()){
            visitNode(setClause);
        }
        if (obj.getWhere() != null) {
            this.criteria = SimpleDBSQLVisitor.getSQLString(obj.getWhere());
        }
    }

    @Override
    public void visit(SetClause obj) {
        Column column = obj.getSymbol().getMetadataObject();
        if (obj.getValue() instanceof Literal){
            try {
                Literal l = (Literal) obj.getValue();
                this.attributes.put(SimpleDBMetadataProcessor.getName(column), SimpleDBDataTypeManager.convertToSimpleDBType(l.getValue(), column.getJavaType()));
            } catch (TranslatorException e) {
                this.exceptions.add(e);
            }
        }
        else if (obj.getValue() instanceof Array) {
            try {
                Array array  = (Array)obj.getValue();
                String[] result = SimpleDBInsertVisitor.getValuesArray(array);
                this.attributes.put(SimpleDBMetadataProcessor.getName(column), result);
            } catch (TranslatorException e) {
                this.exceptions.add(e);
            }
        }
        else {
            this.exceptions.add(new TranslatorException(SimpleDBPlugin.Event.TEIID24001, SimpleDBPlugin.Util.gs(SimpleDBPlugin.Event.TEIID24001)));
        }
    }

    public Table getTable() {
        return table;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public String getCriteria() {
        return this.criteria;
    }
}
