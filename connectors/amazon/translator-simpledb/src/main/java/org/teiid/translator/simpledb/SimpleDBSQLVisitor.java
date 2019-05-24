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

import static org.teiid.language.SQLConstants.Reserved.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.teiid.language.*;
import org.teiid.language.Comparison.Operator;
import org.teiid.language.SQLConstants.Tokens;
import org.teiid.language.visitor.CollectorVisitor;
import org.teiid.language.visitor.SQLStringVisitor;
import org.teiid.metadata.Column;
import org.teiid.translator.TranslatorException;

public class SimpleDBSQLVisitor extends SQLStringVisitor {

    private List<String> projectedColumns = new ArrayList<String>();
    private ArrayList<TranslatorException> exceptions = new ArrayList<TranslatorException>();
    private Column previousColumn;
    private boolean skipCompare;

    public void checkExceptions() throws TranslatorException {
        if (!this.exceptions.isEmpty()) {
            throw this.exceptions.get(0);
        }
    }

    @Override
    public void visit(Select obj) {
        buffer.append(SELECT).append(Tokens.SPACE);

        List<DerivedColumn> allowedColumns = new ArrayList<DerivedColumn>();

        boolean otherCols = false;
        for(int i = 0; i < obj.getDerivedColumns().size(); i++) {
            DerivedColumn dc = obj.getDerivedColumns().get(i);
            if (!(dc.getExpression() instanceof ColumnReference)) {
                otherCols = true;
                break;
            }
            ColumnReference column = (ColumnReference)dc.getExpression();
            if (!SimpleDBMetadataProcessor.isItemName(column.getMetadataObject())) {
                otherCols = true;
                break;
            }
        }

        boolean addedItemName = false;
        for(int i = 0; i < obj.getDerivedColumns().size(); i++) {
            DerivedColumn dc = obj.getDerivedColumns().get(i);
            if (dc.getExpression() instanceof ColumnReference) {
                ColumnReference column = (ColumnReference)dc.getExpression();
                if (SimpleDBMetadataProcessor.isItemName(column.getMetadataObject())) {
                    if (!addedItemName && !otherCols) {
                        allowedColumns.add(dc);
                        addedItemName = true;
                    }
                }
                else {
                    allowedColumns.add(dc);
                }
                projectedColumns.add(SimpleDBMetadataProcessor.getName(column.getMetadataObject()));
            } else if (dc.getExpression() instanceof AggregateFunction) {
                allowedColumns.add(dc);
                projectedColumns.add("Count"); //$NON-NLS-1$
            }
            else {
                this.exceptions.add(new TranslatorException(SimpleDBPlugin.Event.TEIID24004, SimpleDBPlugin.Util.gs(SimpleDBPlugin.Event.TEIID24004, dc)));
            }
        }
        append(allowedColumns);

        if (obj.getFrom() != null && !obj.getFrom().isEmpty()) {
            buffer.append(Tokens.SPACE).append(FROM).append(Tokens.SPACE);
            append(obj.getFrom());
        }
        if (obj.getWhere() != null) {
            buffer.append(Tokens.SPACE)
                  .append(WHERE)
                  .append(Tokens.SPACE);
            append(obj.getWhere());
        }
        if (obj.getOrderBy() != null) {
            validateOrderBy(obj);
            buffer.append(Tokens.SPACE);
            append(obj.getOrderBy());
        }
    }

    // SORT is only supported when a criteria is available
    private void validateOrderBy(Select obj) {
        Collection<ColumnReference> whereColumns = CollectorVisitor.collectElements(obj.getWhere());
        for (SortSpecification sort:obj.getOrderBy().getSortSpecifications()) {
            boolean matched = false;
            for (ColumnReference where:whereColumns) {
                ColumnReference col = (ColumnReference)sort.getExpression();
                if (col.getName().equals(where.getName())) {
                    matched = true;
                }
            }
            if (!matched) {
                this.exceptions.add(new TranslatorException(SimpleDBPlugin.Event.TEIID24005, SimpleDBPlugin.Util.gs(SimpleDBPlugin.Event.TEIID24005)));
                break;
            }
        }
    }

    @Override
    public void visit(Comparison obj) {
        append(obj.getLeftExpression());
        if (!this.skipCompare) {
            buffer.append(Tokens.SPACE);
            if (obj.getOperator().equals(Operator.NE)) {
                buffer.append("!="); //$NON-NLS-1$
            }
            else {
                buffer.append(obj.getOperator());
            }
            buffer.append(Tokens.SPACE);
            append(obj.getRightExpression());
        }
        this.skipCompare = false;
    }

    @Override
    public void visit(Array array) {
        List<Expression> exprs = array.getExpressions();
        append(exprs.get(0));
        for (int i = 1; i < exprs.size(); i++) {
            buffer.append(Tokens.SPACE).append(AndOr.Operator.OR).append(Tokens.SPACE);
            buffer.append(SimpleDBMetadataProcessor.getQuotedName(this.previousColumn));
            buffer.append(Tokens.SPACE).append(Tokens.EQ).append(Tokens.SPACE);
            append(exprs.get(i));
        }
    }

    @Override
    public void visit(Literal obj) {
        if (obj.getValue() == null) {
            buffer.append(NULL);
        } else {
            String val = obj.getValue().toString();
            buffer.append(Tokens.QUOTE)
                .append(escapeString(val, Tokens.QUOTE))
                .append(Tokens.QUOTE);
        }
    }

    @Override
    public void visit(Function obj) {
        String name = obj.getName();
        List<Expression> args = obj.getParameters();
        if(name.equalsIgnoreCase(SimpleDBExecutionFactory.INTERSECTION) || name.equalsIgnoreCase(SimpleDBExecutionFactory.SIMPLEDB+"."+SimpleDBExecutionFactory.INTERSECTION)) { //$NON-NLS-1$
            append(args.get(0));
            buffer.append(Tokens.SPACE).append(Tokens.EQ).append(Tokens.SPACE);
            for (int i = 1; i < args.size(); i++) {
                append(args.get(i));
                if (i < args.size()-1) {
                    buffer.append(Tokens.SPACE).append(SimpleDBExecutionFactory.INTERSECTION).append(Tokens.SPACE);
                    buffer.append(SimpleDBMetadataProcessor.getQuotedName(this.previousColumn));
                    buffer.append(Tokens.SPACE).append(Tokens.EQ).append(Tokens.SPACE);
                }
            }
            this.skipCompare = true;
        }
        else {
            super.visit(obj);
        }
    }

    @Override
    public void visit(ColumnReference obj) {
        buffer.append(SimpleDBMetadataProcessor.getQuotedName(obj.getMetadataObject()));
        this.previousColumn = obj.getMetadataObject();
    }

    public List<String> getProjectedColumns() {
        return this.projectedColumns;
    }

    public static String getSQLString(LanguageObject obj) {
        SimpleDBSQLVisitor visitor = new SimpleDBSQLVisitor();
        visitor.append(obj);
        return visitor.toString();
    }

    @Override
    protected void appendBaseName(NamedTable obj) {
        buffer.append(SimpleDBMetadataProcessor.getQuotedName(obj.getMetadataObject()));
    }

}
