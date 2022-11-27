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
import java.util.Iterator;
import java.util.List;

import org.teiid.language.*;
import org.teiid.language.visitor.HierarchyVisitor;
import org.teiid.metadata.Column;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.simpledb.api.SimpleDBDataTypeManager;

public class SimpleDBInsertVisitor extends HierarchyVisitor {

    private Iterator<? extends List<?>> values;
    private List<Object> expressionValues = new ArrayList<Object>();
    private List<Column> columns = new ArrayList<Column>();
    private ArrayList<TranslatorException> exceptions = new ArrayList<TranslatorException>();
    private String tableName;

    public void checkExceptions() throws TranslatorException {
        if (!this.exceptions.isEmpty()) {
            throw this.exceptions.get(0);
        }
    }

    public List<Column> getColumns() {
        return this.columns;
    }

    public Iterator<? extends List<?>> values() {
        if (this.values != null) {
            return this.values;
        }
        List<List<?>> result = new ArrayList<List<?>>(1);
        result.add(this.expressionValues);
        return result.iterator();
    }

    public String getDomainName(){
        return this.tableName;
    }

    @Override
    public void visit(Insert obj) {
        visitNode(obj.getTable());
        visitNodes(obj.getColumns());
        if (!(obj.getValueSource() instanceof QueryExpression) && obj.getParameterValues() == null) {
            visitNode(obj.getValueSource());
        }
        else {
            // bulk insert values
            this.values = obj.getParameterValues();
        }
    }

    @Override
    public void visit(NamedTable obj) {
        this.tableName = SimpleDBMetadataProcessor.getName(obj.getMetadataObject());
    }

    @Override
    public void visit(ColumnReference obj) {
        this.columns.add(obj.getMetadataObject());
        super.visit(obj);
    }

    @Override
    public void visit(ExpressionValueSource obj) {
        try {
            List<Expression> values = obj.getValues();
            for (int i = 0; i < obj.getValues().size(); i++){
                if (values.get(i) instanceof Literal){
                    Literal lit = (Literal) values.get(i);
                    this.expressionValues.add(lit.getValue());
                }
                else if (values.get(i) instanceof Array){
                    Array array  = (Array)values.get(i);
                    String[] result = getValuesArray(array);
                    this.expressionValues.add(result);
                }
                else {
                    this.exceptions.add(new TranslatorException(SimpleDBPlugin.Event.TEIID24001, SimpleDBPlugin.Util.gs(SimpleDBPlugin.Event.TEIID24001)));
                }
            }
            super.visit(obj);
        } catch (TranslatorException e) {
            this.exceptions.add(e);
        }
    }

    static String[] getValuesArray(Array array) throws TranslatorException {
        String[] result = new String[array.getExpressions().size()];
        for (int j = 0; j < array.getExpressions().size(); j++) {
            Expression expr = array.getExpressions().get(j);
            if (expr instanceof Literal){
                Literal lit = (Literal) expr;
                result[j] = (String)SimpleDBDataTypeManager.convertToSimpleDBType(lit.getValue(), lit.getType());
            }
            else {
                new TranslatorException(SimpleDBPlugin.Event.TEIID24001, SimpleDBPlugin.Util.gs(SimpleDBPlugin.Event.TEIID24001));
            }
        }
        return result;
    }
}