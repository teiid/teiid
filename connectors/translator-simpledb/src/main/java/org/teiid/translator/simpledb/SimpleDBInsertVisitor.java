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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.teiid.language.*;
import org.teiid.language.visitor.HierarchyVisitor;
import org.teiid.metadata.Column;
import org.teiid.resource.adpter.simpledb.SimpleDBDataTypeManager;
import org.teiid.translator.TranslatorException;

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