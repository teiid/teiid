/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
 */

package org.teiid.translator.simpledb;

import static org.teiid.language.SQLConstants.Reserved.FROM;
import static org.teiid.language.SQLConstants.Reserved.NULL;
import static org.teiid.language.SQLConstants.Reserved.SELECT;
import static org.teiid.language.SQLConstants.Reserved.WHERE;

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
    
    private List<Column> projectedColumns = new ArrayList<Column>();
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
        
        if (obj.getDerivedColumns().size() > 1) {
            List<DerivedColumn> allowedColumns = new ArrayList<DerivedColumn>();
            for(int i = 0; i < obj.getDerivedColumns().size(); i++) {
                DerivedColumn dc = obj.getDerivedColumns().get(i);
                if (dc.getExpression() instanceof ColumnReference) {
                    ColumnReference column = (ColumnReference)dc.getExpression();
                    if (SimpleDBMetadataProcessor.isItemName(column.getMetadataObject())) {
                        this.projectedColumns.add(column.getMetadataObject());
                    }
                    else {
                        allowedColumns.add(dc);
                    }
                }
                else {
                    this.exceptions.add(new TranslatorException(SimpleDBPlugin.Event.TEIID24004, SimpleDBPlugin.Util.gs(SimpleDBPlugin.Event.TEIID24004, dc)));
                }
            }
            append(allowedColumns);
        }
        else {
            append(obj.getDerivedColumns());
        }

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
        if (!useSelectLimit() && obj.getLimit() != null) {
            buffer.append(Tokens.SPACE);
            append(obj.getLimit());
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
            buffer.append(SQLStringVisitor.getRecordName(this.previousColumn));
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
    public void visit(DerivedColumn obj) {
        super.visit(obj);
        if (obj.getExpression() instanceof ColumnReference) {
            ColumnReference ref = (ColumnReference)obj.getExpression();
            this.projectedColumns.add(ref.getMetadataObject());
        }
        else {
            
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
                    buffer.append(SQLStringVisitor.getRecordName(this.previousColumn));
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
        buffer.append(SQLStringVisitor.getRecordName(obj.getMetadataObject()));
        this.previousColumn = obj.getMetadataObject();
    }    
    
    public List<Column> getProjectedColumns() {
        return this.projectedColumns;
    }

    public static String getSQLString(LanguageObject obj) {
        SimpleDBSQLVisitor visitor = new SimpleDBSQLVisitor();
        visitor.append(obj);
        return visitor.toString();
    }
    
}
