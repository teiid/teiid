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
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import org.apache.parquet.filter2.predicate.FilterApi;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.language.ColumnReference;
import org.teiid.language.Comparison;
import org.teiid.language.Comparison.Operator;
import org.teiid.language.Condition;
import org.teiid.language.DerivedColumn;
import org.teiid.language.LanguageUtil;
import org.teiid.language.NamedTable;
import org.teiid.language.Select;
import org.teiid.language.visitor.CollectorVisitor;
import org.teiid.language.visitor.HierarchyVisitor;
import org.teiid.metadata.Column;
import org.teiid.metadata.Table;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TypeFacility;

public class ParquetQueryVisitor extends HierarchyVisitor {

    private List<String> projectedColumnNames = new ArrayList<String>();
    private LinkedHashSet<String> allColumns = new LinkedHashSet<>();
    private LinkedHashMap<String, Column> partitionedColumns = new LinkedHashMap<>();

    //we separate predicates into three buckets
    //comparisons that can be applied to the file search path
    private Map<String, Comparison> partitionedComparisons = new HashMap<String, Comparison>();
    //predicates that can be evaluated solely against the file path once the file is retrieved
    private List<Condition> partitionedConditions = new ArrayList<>();
    //predicates that can only be applied against row values
    private List<Condition> nonPartionedConditions = new ArrayList<>();

    private Table table;
    private String parquetPath;

    private org.apache.parquet.filter2.predicate.Operators.Column<?> referenceColumn;

    public List<String> getProjectedColumnNames() {
        return projectedColumnNames;
    }

    public LinkedHashSet<String> getAllColumns() {
        return allColumns;
    }

    public Table getTable() {
        return table;
    }

    public String getParquetPath() {
        return parquetPath;
    }

    public LinkedHashMap<String, Column> getPartitionedColumns() {
        return partitionedColumns;
    }

    public List<Condition> getNonPartionedConditions() {
        return nonPartionedConditions;
    }

    public Map<String, Comparison> getPartitionedComparisons() {
        return partitionedComparisons;
    }

    public List<Condition> getPartitionedConditions() {
        return partitionedConditions;
    }

    public org.apache.parquet.filter2.predicate.Operators.Column<?> getReferenceColumn() {
        return referenceColumn;
    }

    @Override
    public void visit(NamedTable obj) {
        this.table = obj.getMetadataObject();
        this.parquetPath = this.table.getProperty(ParquetMetadataProcessor.LOCATION);
        String partitionedColumnNames = this.table.getProperty(ParquetMetadataProcessor.PARTITIONED_COLUMNS);
        if (partitionedColumnNames != null) {
            Arrays.stream(partitionedColumnNames.split(",")).forEach((s) -> {
                Column col = null;
                for (Column c : this.table.getColumns()) {
                    if (c.getSourceName().equals(s)) {
                        col = c;
                        break;
                    }
                }
                partitionedColumns.put(s, col);
            });
        }
    }

    @Override
    public void visit(Select obj) {
        for (DerivedColumn column : obj.getDerivedColumns()) {
            this.projectedColumnNames
                    .add(((ColumnReference) column.getExpression())
                            .getMetadataObject().getSourceName());
        }

        allColumns.addAll(this.projectedColumnNames);

        //visit the from to initialize the partitioned columns
        visitNodes(obj.getFrom());

        boolean findReferenceColumn = false;
        for (Condition c : LanguageUtil.separateCriteriaByAnd(obj.getWhere())) {
            Collection<ColumnReference> cols = CollectorVisitor.collectElements(c);
            boolean partitioned = false;
            boolean nonPartitioned = false;
            for (ColumnReference ref : cols) {
                String colName = ref.getMetadataObject().getSourceName();
                allColumns.add(colName);
                if (partitionedColumns.containsKey(colName)) {
                    partitioned = true;
                } else {
                    nonPartitioned = true;
                }
            }
            //progress through each conjunct to see how it can be used
            //if all else fails it is applied as a general filter
            if (partitioned && !nonPartitioned) {
                if (c instanceof Comparison) {
                    Comparison comp = (Comparison)c;
                    if (comp.getOperator() == Operator.EQ) {
                        ColumnReference cr = (ColumnReference) comp.getLeftExpression();
                        Column column = cr.getMetadataObject();
                        String columnName = column.getSourceName();
                        Comparison old = partitionedComparisons.put(columnName, comp);
                        assert old == null; //should only be a single equality
                        continue;
                    }
                }
                partitionedConditions.add(c);
                continue;
            }

            findReferenceColumn |= partitioned;
            nonPartionedConditions.add(c);
            //TODO: could move the logic for building the condition filters into the visitor
        }

        if (findReferenceColumn) {
            //find a reference column for partitioned comparisons in row filters,
            //if we don't supply a valid column, the filter is effectively ignored
            referenceColumn = findReferenceColumn(true);
            if (referenceColumn == null) {
                referenceColumn = findReferenceColumn(false);
                if (referenceColumn == null) {
                    //corner case, we can't apply the predicate
                    throw new TeiidRuntimeException("There needs to be at least one generally filterable column on the table to apply a nested partition predicate"); //$NON-NLS-1$
                }
            }
        } else {
            referenceColumn = FilterApi.longColumn("."); //$NON-NLS-1$
        }
    }

    private org.apache.parquet.filter2.predicate.Operators.Column<?> findReferenceColumn(boolean projected) {
        for (Column col : this.table.getColumns()) {
            if (partitionedColumns.containsKey(col.getSourceName())
                    || col.getJavaType().isArray()
                    || col.getJavaType() == TypeFacility.RUNTIME_TYPES.BOOLEAN
                    || (projected && !allColumns.contains(col.getSourceName()))) {
                continue;
            }
            try {
                return BaseParquetExecution.createFilterColumn(col.getSourceName(), col.getJavaType().getName());
            } catch (TranslatorException e) {
                continue;
            }
        }
        return null;
    }

}
