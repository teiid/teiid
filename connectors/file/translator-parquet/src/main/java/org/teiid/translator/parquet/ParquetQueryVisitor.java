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
package org.teiid.translator.parquet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Stack;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.teiid.language.*;
import org.teiid.language.visitor.HierarchyVisitor;
import org.teiid.metadata.Column;
import org.teiid.metadata.Table;
import org.teiid.translator.TranslatorException;

public class ParquetQueryVisitor extends HierarchyVisitor {

    protected Stack<Object> onGoingExpression = new Stack<Object>();
    private List<String> projectedColumnNames = new ArrayList<String>();
    protected ArrayList<TranslatorException> exceptions = new ArrayList<TranslatorException>();

    public Multimap<String, Comparison> getColumnPredicates() {
        return columnPredicates;
    }

    private Multimap<String, Comparison> columnPredicates = ArrayListMultimap.create();

    private Table table;
    private String parquetPath;

    public List<String> getProjectedColumnNames() {
        return projectedColumnNames;
    }

    public ArrayList<TranslatorException> getExceptions() {
        return exceptions;
    }

    public Table getTable() {
        return table;
    }

    public String getParquetPath() {
        return parquetPath;
    }

    @Override
    public void visit(NamedTable obj) {
        this.table = obj.getMetadataObject();
        this.parquetPath = this.table.getProperty(ParquetMetadataProcessor.FILE);
    }

    @Override
    public void visit(ColumnReference obj) {
        this.onGoingExpression.add(obj.getMetadataObject());
    }

    @Override
    public void visit(DerivedColumn obj) {
        visitNode(obj.getExpression());

        createProjectedColumn();
    }

    private void createProjectedColumn() {
        Column column = (Column) this.onGoingExpression.pop();
        this.projectedColumnNames.add(column.getSourceName());
    }

    @Override
    public void visit(Comparison obj) {
        ColumnReference cr = (ColumnReference) obj.getLeftExpression();
        Column column = cr.getMetadataObject();
        String columnName = column.getSourceName();
        columnPredicates.put(columnName, obj);
    }

    @Override
    public void visit(Literal obj) {
        this.onGoingExpression.push(obj.getValue());
    }

}
