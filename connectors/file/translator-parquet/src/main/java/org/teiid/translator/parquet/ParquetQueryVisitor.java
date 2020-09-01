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
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import org.teiid.language.ColumnReference;
import org.teiid.language.Comparison;
import org.teiid.language.Condition;
import org.teiid.language.DerivedColumn;
import org.teiid.language.LanguageUtil;
import org.teiid.language.NamedTable;
import org.teiid.language.Select;
import org.teiid.language.visitor.CollectorVisitor;
import org.teiid.language.visitor.HierarchyVisitor;
import org.teiid.metadata.Column;
import org.teiid.metadata.Table;

public class ParquetQueryVisitor extends HierarchyVisitor {

    private List<String> projectedColumnNames = new ArrayList<String>();
    private LinkedHashSet<String> partitionedColumns = new LinkedHashSet<>();
    private Map<String, Comparison> partitionedComparisons = new HashMap<String, Comparison>();
    private List<Condition> nonPartionedConditions = new ArrayList<>();

    private Table table;
    private String parquetPath;

    public List<String> getProjectedColumnNames() {
        return projectedColumnNames;
    }

    public Table getTable() {
        return table;
    }

    public String getParquetPath() {
        return parquetPath;
    }

    public LinkedHashSet<String> getPartitionedColumns() {
        return partitionedColumns;
    }

    public List<Condition> getNonPartionedConditions() {
        return nonPartionedConditions;
    }

    public Map<String, Comparison> getPartitionedComparisons() {
        return partitionedComparisons;
    }

    @Override
    public void visit(NamedTable obj) {
        this.table = obj.getMetadataObject();
        this.parquetPath = this.table.getProperty(ParquetMetadataProcessor.FILE);
        String partitionedColumnNames = this.table.getProperty(ParquetMetadataProcessor.PARTITIONED_COLUMNS);
        if (partitionedColumnNames != null) {
            partitionedColumns.addAll(Arrays.asList(partitionedColumnNames.split(","))); //$NON-NLS-1$
        }
    }

    @Override
    public void visit(Select obj) {
        for (DerivedColumn column : obj.getDerivedColumns()) {
            this.projectedColumnNames
                    .add(((ColumnReference) column.getExpression())
                            .getMetadataObject().getSourceName());
        }

        //visit the from to initialize the partitioned columns
        visitNodes(obj.getFrom());

        for (Condition c : LanguageUtil.separateCriteriaByAnd(obj.getWhere())) {
            Collection<ColumnReference> cols = CollectorVisitor.collectElements(c);
            boolean partitioned = false;
            boolean nonPartitioned = false;
            for (ColumnReference ref : cols) {
                if (partitionedColumns.contains(ref.getMetadataObject().getSourceName())) {
                    partitioned = true;
                } else {
                    nonPartitioned = true;
                }
            }
            if (partitioned && nonPartitioned) {
                //not currently usable - spans both partitioning and non-partitioning columns
                continue;
            }
            if (partitioned) {
                if (!(c instanceof Comparison)) {
                    //not currently usable
                    continue;
                }
                Comparison comp = (Comparison)c;
                ColumnReference cr = (ColumnReference) comp.getLeftExpression();
                Column column = cr.getMetadataObject();
                String columnName = column.getSourceName();
                Comparison old = partitionedComparisons.put(columnName, comp);
                assert old == null;
            } else if (nonPartitioned) {
                nonPartionedConditions.add(c);
            }
        }
    }

}
