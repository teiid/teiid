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
package org.teiid.translator.excel;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

import org.teiid.language.AndOr;
import org.teiid.language.ColumnReference;
import org.teiid.language.Comparison;
import org.teiid.language.Comparison.Operator;
import org.teiid.language.DerivedColumn;
import org.teiid.language.Expression;
import org.teiid.language.ExpressionValueSource;
import org.teiid.language.In;
import org.teiid.language.Insert;
import org.teiid.language.Literal;
import org.teiid.language.NamedTable;
import org.teiid.language.SetClause;
import org.teiid.language.visitor.HierarchyVisitor;
import org.teiid.metadata.Column;
import org.teiid.metadata.KeyRecord;
import org.teiid.metadata.Table;
import org.teiid.translator.TranslatorException;

public class ExcelQueryVisitor extends HierarchyVisitor {

    public static int ROW_ID_INDEX = -1;

    protected Stack<Object> onGoingExpression = new Stack<Object>();
    private List<Integer> projectedColumns = new ArrayList<Integer>();
    protected ArrayList<TranslatorException> exceptions = new ArrayList<TranslatorException>();

    private Table table;
    private String xlsPath;
    private String sheetName;
    private int firstDataRowNumber;

    static interface Filter {
        public boolean allows (int row);
    }

    static class InFilter implements Filter {
        Integer[] values;
        public InFilter(Integer[] allowed) {
            this.values = allowed;
        }

        @Override
        public boolean allows(int row) {
            for (int i = 0; i < values.length; i++) {
                if (values[i] == row) {
                    return true;
                }
            }
            return false;
        }
    }

    static class CompareFilter implements Filter {
        int start;
        Operator op;

        public CompareFilter (int start, Operator op) {
            this.start = start;
            this.op = op;
        }

        public boolean allows (int row) {
            switch(op) {
            case EQ:
                return row == start;
            case NE:
                return row != start;
            case LT:
                return row < start;
            case LE:
                return row <= start;
            case GT:
                return row > start;
            case GE:
                return row >= start;
            }
            return false;
        }
    }

    private ArrayList<ExcelQueryVisitor.Filter> filters = new ArrayList<ExcelQueryVisitor.Filter>();

    public List<Integer> getProjectedColumns() {
        return projectedColumns;
    }

    public int getFirstDataRowNumber() {
        return firstDataRowNumber;
    }

    public ArrayList<TranslatorException> getExceptions() {
        return exceptions;
    }

    public Table getTable() {
        return table;
    }

    public String getXlsPath() {
        return xlsPath;
    }

    public String getSheetName() {
        return sheetName;
    }

    @Override
    public void visit(ColumnReference obj) {
        this.onGoingExpression.push(obj.getMetadataObject());
    }

    @Override
    public void visit(DerivedColumn obj) {
        visitNode(obj.getExpression());

        createProjectedColumn(true);
    }

    private void createProjectedColumn(boolean allowRowId) {
        Column column = (Column) this.onGoingExpression.pop();
        String str = column.getProperty(ExcelMetadataProcessor.CELL_NUMBER, false);

        if (str == null) {
            this.exceptions.add(new TranslatorException(ExcelPlugin.Event.TEIID23007, ExcelPlugin.Util.gs(ExcelPlugin.Event.TEIID23007, column.getName())));
            return;
        }

        if (str.equalsIgnoreCase(ExcelMetadataProcessor.ROW_ID)) {
            if (!allowRowId) {
                this.exceptions.add(new TranslatorException(ExcelPlugin.Event.TEIID23009, ExcelPlugin.Util.gs(ExcelPlugin.Event.TEIID23009, column.getName())));
            }
            this.projectedColumns.add(ROW_ID_INDEX);
        }
        else {
            this.projectedColumns.add(Integer.valueOf(str));
        }
    }

    @Override
    public void visit(SetClause clause) {
        visit(clause.getSymbol());
        createProjectedColumn(false);
        if (!(clause.getValue() instanceof Literal)) {
            this.exceptions.add(new TranslatorException(ExcelPlugin.Event.TEIID23010, ExcelPlugin.Util.gs(ExcelPlugin.Event.TEIID23010, clause)));
        }
    }

    @Override
    public void visit(NamedTable obj) {
        this.table = obj.getMetadataObject();
        this.xlsPath = this.table.getProperty(ExcelMetadataProcessor.FILE, false);
        this.sheetName = this.table.getSourceName();
        String firstRow = this.table.getProperty(ExcelMetadataProcessor.FIRST_DATA_ROW_NUMBER, false);
        if (firstRow != null) {
            // -1 make it zero based index
            this.firstDataRowNumber = Integer.parseInt(firstRow)-1;
        }
    }

    @Override
    public void visit(AndOr obj) {
        visitNode(obj.getLeftCondition());
        visitNode(obj.getRightCondition());
    }

    @Override
    public void visit(Comparison obj) {
        visitNode(obj.getLeftExpression());
        Column column = (Column)this.onGoingExpression.pop();

        visitNode(obj.getRightExpression());
        Integer rightExpr = (Integer)this.onGoingExpression.pop();

        if (isPartOfPrimaryKey(column)) {
            switch(obj.getOperator()) {
            case EQ:
                this.filters.add(new CompareFilter(rightExpr-1, Operator.EQ));
                break;
            case NE:
                this.filters.add(new CompareFilter(rightExpr-1, Operator.NE));
                break;
            case LT:
                this.filters.add(new CompareFilter(rightExpr-1, Operator.LT));
                break;
            case LE:
                this.filters.add(new CompareFilter(rightExpr-1, Operator.LE));
                break;
            case GT:
                this.filters.add(new CompareFilter(rightExpr-1,  Operator.GT));
                break;
            case GE:
                this.filters.add(new CompareFilter(rightExpr-1,  Operator.GE));
                break;
            }
        }
        else {
            this.exceptions.add(new TranslatorException(ExcelPlugin.Event.TEIID23008, ExcelPlugin.Util.gs(ExcelPlugin.Event.TEIID23008, column.getName())));
        }
    }

    @Override
    public void visit(In obj) {
        visitNode(obj.getLeftExpression());
        Column column = (Column)this.onGoingExpression.pop();

        visitNodes(obj.getRightExpressions());

        if (isPartOfPrimaryKey(column)) {
            ArrayList<Integer> values = new ArrayList<Integer>();
            // NOTE: we are popping in reverse order to IN stmt
            for (int i = 0; i < obj.getRightExpressions().size(); i++) {
                values.add((Integer)this.onGoingExpression.pop());
            }
            this.filters.add(new InFilter(values.toArray(new Integer[values.size()])));
        }
        else {
            this.exceptions.add(new TranslatorException(ExcelPlugin.Event.TEIID23008, ExcelPlugin.Util.gs(ExcelPlugin.Event.TEIID23008, column.getName())));
        }
    }

    @Override
    public void visit(Literal obj) {
        this.onGoingExpression.push(obj.getValue());
    }

    public static boolean isPartOfPrimaryKey(Column column) {
        KeyRecord pk = ((Table)column.getParent()).getPrimaryKey();
        if (pk != null) {
            for (Column col:pk.getColumns()) {
                if (col.getName().equals(column.getName())) {
                    if (col.getProperty(ExcelMetadataProcessor.CELL_NUMBER, false).equalsIgnoreCase(ExcelMetadataProcessor.ROW_ID)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    public boolean allows(int row) {
        if (this.filters.isEmpty()) {
            return true;
        }

        for (Filter f:this.filters) {
            if (!f.allows(row)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void visit(Insert obj) {
        visit(obj.getTable());
        for (ColumnReference cr : obj.getColumns()) {
            visit(cr);
            createProjectedColumn(false);
        }
        if (!(obj.getValueSource() instanceof ExpressionValueSource)) {
            throw new AssertionError();
        }
        for (Expression ex : ((ExpressionValueSource)obj.getValueSource()).getValues()) {
            if (!(ex instanceof Literal)) {
                throw new AssertionError();
            }
        }
    }

}
