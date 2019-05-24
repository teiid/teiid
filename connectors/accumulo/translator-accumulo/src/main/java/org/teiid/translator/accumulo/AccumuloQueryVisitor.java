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
package org.teiid.translator.accumulo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Stack;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.teiid.language.*;
import org.teiid.language.visitor.HierarchyVisitor;
import org.teiid.language.visitor.SQLStringVisitor;
import org.teiid.metadata.AbstractMetadataRecord;
import org.teiid.metadata.Column;
import org.teiid.metadata.KeyRecord;
import org.teiid.metadata.Table;
import org.teiid.query.metadata.DDLStringVisitor;
import org.teiid.query.metadata.SystemMetadata;
import org.teiid.translator.TranslatorException;

public class AccumuloQueryVisitor extends HierarchyVisitor {

    protected Stack<Object> onGoingExpression  = new Stack<Object>();
    protected List<Range> ranges = new ArrayList<Range>();
    protected Table scanTable;
    protected ArrayList<TranslatorException> exceptions = new ArrayList<TranslatorException>();
    private HashMap<String, Column> keybasedColumnMap = new HashMap<String, Column>();
    private ArrayList<Column> selectColumns = new ArrayList<Column>();
    private ArrayList<IteratorSetting>  scanIterators = new ArrayList<IteratorSetting>();
    private String currentAlias;
    private int aliasIdx = 0;
    private int iteratorPriority = 2;
    private boolean doScanEvaluation = false;
    private AccumuloExecutionFactory ef;

    public AccumuloQueryVisitor(AccumuloExecutionFactory ef) {
        this.ef = ef;
    }

    public List<Range> getRanges(){
        return this.ranges;
    }

    public Table getScanTable() {
        return this.scanTable;
    }

    public Column lookupColumn(String key) {
        return this.keybasedColumnMap.get(key);
    }

    public List<Column> projectedColumns(){
        return this.selectColumns;
    }

    public List<IteratorSetting> scanIterators(){
        return this.scanIterators;
    }

    @Override
    public void visit(Select obj) {
        visitNodes(obj.getFrom());
        visitNodes(obj.getDerivedColumns());
        visitNode(obj.getWhere());
        visitNode(obj.getGroupBy());
        visitNode(obj.getHaving());
        visitNode(obj.getOrderBy());
        visitNode(obj.getLimit());

        if (this.doScanEvaluation) {
            HashMap<String, String> options = buildEvaluatorOptions(this.scanTable);
            SQLStringVisitor visitor = new SQLStringVisitor() {
                @Override
                public String getName(AbstractMetadataRecord object) {
                    return object.getName();
                }
            };
            visitor.append(obj.getWhere());

            options.put(EvaluatorIterator.QUERYSTRING, visitor.toString());
            IteratorSetting it = new IteratorSetting(1, EvaluatorIterator.class, options);
            this.scanIterators.add(it);
        }
    }

    @Override
    public void visit(DerivedColumn obj) {
        this.currentAlias = buildAlias(obj.getAlias());
        visitNode(obj.getExpression());

        Column column = (Column)this.onGoingExpression.pop();

        String CF = column.getProperty(AccumuloMetadataProcessor.CF, false);
        String CQ = column.getProperty(AccumuloMetadataProcessor.CQ, false);
        if (CQ != null) {
            this.keybasedColumnMap.put(CF+"/"+CQ, column); //$NON-NLS-1$
        }
        else {
            this.keybasedColumnMap.put(CF, column);
        }

        // no expressions in select are allowed.
        this.selectColumns.add(column);
    }

    private String buildAlias(String alias) {
        if (alias != null) {
            return alias;
        }
        return "_m"+this.aliasIdx; //$NON-NLS-1$
    }

    @Override
    public void visit(ColumnReference obj) {
        this.onGoingExpression.push(obj.getMetadataObject());
    }

    @Override
    public void visit(AndOr obj) {
        visitNode(obj.getLeftCondition());
        visitNode(obj.getRightCondition());

           this.ranges = Range.mergeOverlapping(this.ranges);
    }

    @Override
    public void visit(Comparison obj) {
        visitNode(obj.getLeftExpression());
        Column column = (Column)this.onGoingExpression.pop();

        visitNode(obj.getRightExpression());
        Object rightExpr = this.onGoingExpression.pop();
        Key rightKey = buildKey(rightExpr);
        if (isPartOfPrimaryKey(column)) {
            switch(obj.getOperator()) {
            case EQ:
                this.ranges.add(singleRowRange(rightKey));
                break;
            case NE:
                this.ranges.add(new Range(null, true, rightKey, false));
                this.ranges.add(new Range(rightKey.followingKey(PartialKey.ROW), null, false, true, false, true));
                break;
            /*
            case LT:
                this.ranges.add(new Range(null, true, rightKey, false));
                break;
            case LE:
                this.ranges.add(new Range(null, true, rightKey, false));
                this.ranges.add(singleRowRange(rightKey));
                break;
            case GT:
                this.ranges.add(new Range(rightKey.followingKey(PartialKey.ROW), null, false, true, false, true));
                break;
            case GE:
                this.ranges.add(new Range(rightKey, true, null, true));
                break;
            */
            }
            this.doScanEvaluation = true;
        }
        else {
            this.doScanEvaluation = true;
        }
    }

    static Key buildKey(Object value) {
        byte[] row = AccumuloDataTypeManager.serialize(value);
        Key rangeKey = new Key(row, AccumuloDataTypeManager.EMPTY_BYTES,
                AccumuloDataTypeManager.EMPTY_BYTES,
                AccumuloDataTypeManager.EMPTY_BYTES,
                Long.MAX_VALUE);
        return rangeKey;
    }

    @Override
    public void visit(In obj) {
        visitNode(obj.getLeftExpression());
        Column column = (Column)this.onGoingExpression.pop();

        visitNodes(obj.getRightExpressions());
        if (isPartOfPrimaryKey(column)) {
            Object prevExpr = null;
            // NOTE: we are popping in reverse order to IN stmt
            for (int i = 0; i < obj.getRightExpressions().size(); i++) {
                Object rightExpr = this.onGoingExpression.pop();
                Key rightKey = buildKey(rightExpr);
                Key prevKey = null;
                if (prevExpr != null) {
                    prevKey = buildKey(prevExpr);
                }
                Range range = singleRowRange(rightKey);
                if (obj.isNegated()) {
                    if (prevExpr == null) {
                        this.ranges.add(new Range(rightKey, false, null, true));
                        this.ranges.add(new Range(null, true, rightKey, false));
                    }
                    else {
                        this.ranges.remove(this.ranges.size()-1);
                        this.ranges.add(new Range(rightKey, false, prevKey, false));
                        this.ranges.add(new Range(null, true, rightKey, false));
                    }
                    prevExpr = rightExpr;
                }
                else {
                    this.ranges.add(range);
                }
            }
        }
        else {
            this.doScanEvaluation = true;
        }
    }

    static Range singleRowRange(Key key) {
        Range range = new Range(key, key.followingKey(PartialKey.ROW), true, false, false, false);
        return range;
    }

    public static boolean isPartOfPrimaryKey(Column column) {
        KeyRecord pk = ((Table)column.getParent()).getPrimaryKey();
        if (pk != null) {
            for (Column col:pk.getColumns()) {
                if (col.getName().equals(column.getName())) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public void visit(AggregateFunction obj) {
        if (!obj.getParameters().isEmpty()) {
            visitNodes(obj.getParameters());
        }

        if (obj.getName().equals(AggregateFunction.COUNT)) {
            HashMap<String, String> options = new HashMap<String, String>();
            options.put(CountStarIterator.ALIAS, this.currentAlias);
            IteratorSetting it = new IteratorSetting(this.iteratorPriority++, CountStarIterator.class, options);

            // expression expects a column
            Column c = new Column();
            c.setName(this.currentAlias);
            c.setDatatype(SystemMetadata.getInstance().getSystemStore().getDatatypes().get("integer"));//$NON-NLS-1$
            c.setProperty(AccumuloMetadataProcessor.CF, this.currentAlias);

            this.scanIterators.add(it);
            this.onGoingExpression.push(c) ;
        }
        else if (obj.getName().equals(AggregateFunction.AVG)) {
        }
        else if (obj.getName().equals(AggregateFunction.SUM)) {
        }
        else if (obj.getName().equals(AggregateFunction.MIN)) {
        }
        else if (obj.getName().equals(AggregateFunction.MAX)) {
        }
        else {
        }
    }

    @Override
    public void visit(IsNull obj) {
        visitNode(obj.getExpression());
        Column column = (Column)onGoingExpression.pop();
        // this will never be part of the rowid, as it can never be null, so scan
        this.doScanEvaluation = true;
    }

    @Override
    public void visit(Literal obj) {
        this.onGoingExpression.push(obj.getValue());
    }

    @Override
    public void visit(NamedTable obj) {
        this.scanTable = obj.getMetadataObject();
    }

    private static HashMap<String, String> buildEvaluatorOptions(Table table) {
        HashMap<String, String> options = new HashMap<String, String>();
        options.put(EvaluatorIterator.TABLE, table.getName());

        String ddl = DDLStringVisitor.getDDLString(table.getParent(), null, table.getName());
        options.put(EvaluatorIterator.DDL, ddl);
        return options;
    }
}
