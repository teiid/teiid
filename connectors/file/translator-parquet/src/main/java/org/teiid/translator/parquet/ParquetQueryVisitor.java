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
import java.util.List;
import java.util.Stack;

import org.teiid.language.ColumnReference;
import org.teiid.language.DerivedColumn;
import org.teiid.language.NamedTable;
import org.teiid.language.visitor.HierarchyVisitor;
import org.teiid.metadata.Column;
import org.teiid.metadata.Table;
import org.teiid.translator.TranslatorException;

public class ParquetQueryVisitor extends HierarchyVisitor {

    protected Stack<Object> onGoingExpression = new Stack<Object>();
    private List<String> projectedColumnNames = new ArrayList<String>();
    protected ArrayList<TranslatorException> exceptions = new ArrayList<TranslatorException>();

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

}
