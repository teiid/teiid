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
import java.util.concurrent.atomic.AtomicInteger;

import org.teiid.language.*;
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
    private String path = "";
    private AtomicInteger columnCount = new AtomicInteger();
    private String[] partitionedColumnArray;
    boolean flag = true;

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
        if(this.table.getProperty(ParquetMetadataProcessor.PARTITIONING_SCHEME) == null){
            // TODO: Row filtering
        }
        else {
            if (this.table.getProperty(ParquetMetadataProcessor.PARTITIONING_SCHEME).equals("directory")) {
                if(flag) {
                    this.partitionedColumnArray = getPartitionedColumns(this.table.getProperty(ParquetMetadataProcessor.PARTITIONED_COLUMNS));
                    flag = false;
                }
                directoryBasedPartitioning(obj);
            } else {
                // TODO: File Based Partitioning
            }
        }
    }

    @Override
    public void visit(Literal obj) {
        this.onGoingExpression.push(obj.getValue());
    }

    private void directoryBasedPartitioning(Comparison obj) {
        visitNode(obj.getLeftExpression());
        Column column = (Column)this.onGoingExpression.pop();
        String columnName = column.getSourceName();
        visitNode(obj.getRightExpression());
        String columnValue = (String) this.onGoingExpression.pop();
        while(this.columnCount.get() < partitionedColumnArray.length && !partitionedColumnArray[this.columnCount.getAndIncrement()].equals(columnName)){
            path += "/*";
        }
        path += "/" + columnValue;
        if(partitionedColumnArray.length == this.columnCount.get()){
            path += "/*";
            parquetPath += path;
        }
    }

    private String[] getPartitionedColumns(String partitionedColumns) {
        String[] partitionedColumnList;
        partitionedColumnList = partitionedColumns.split(",");
        return partitionedColumnList;
    }

}
