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
import java.util.HashMap;
import java.util.Map;

import org.teiid.language.Array;
import org.teiid.language.Literal;
import org.teiid.language.SetClause;
import org.teiid.language.Update;
import org.teiid.language.visitor.HierarchyVisitor;
import org.teiid.metadata.Column;
import org.teiid.metadata.Table;
import org.teiid.resource.adpter.simpledb.SimpleDBDataTypeManager;
import org.teiid.translator.TranslatorException;

public class SimpleDBUpdateVisitor extends HierarchyVisitor{
    private Table table;
    private Map<String, Object> attributes = new HashMap<String, Object>();
    private String criteria;
    private ArrayList<TranslatorException> exceptions = new ArrayList<TranslatorException>();

    public SimpleDBUpdateVisitor(Update update) {
        visitNode(update);
    }
    
    public void checkExceptions() throws TranslatorException {
        if (!this.exceptions.isEmpty()) {
            throw this.exceptions.get(0);
        }
    }

    @Override
    public void visit(Update obj) {
        if (obj.getParameterValues() != null) {
            this.exceptions.add(new TranslatorException(SimpleDBPlugin.Event.TEIID24006, SimpleDBPlugin.Util.gs(SimpleDBPlugin.Event.TEIID24006)));
        }
        
        this.table = obj.getTable().getMetadataObject();
        for(SetClause setClause : obj.getChanges()){
            visitNode(setClause);
        }
        if (obj.getWhere() != null) {
            this.criteria = SimpleDBSQLVisitor.getSQLString(obj.getWhere());
        }
    }

    @Override
    public void visit(SetClause obj) {
        Column column = obj.getSymbol().getMetadataObject();
        if (obj.getValue() instanceof Literal){
            try {
                Literal l = (Literal) obj.getValue();
                this.attributes.put(SimpleDBMetadataProcessor.getName(column), SimpleDBDataTypeManager.convertToSimpleDBType(l.getValue(), column.getJavaType()));
            } catch (TranslatorException e) {
                this.exceptions.add(e);
            }
        }
        else if (obj.getValue() instanceof Array) {
            try {
                Array array  = (Array)obj.getValue();
                String[] result = SimpleDBInsertVisitor.getValuesArray(array);
                this.attributes.put(SimpleDBMetadataProcessor.getName(column), result);                
            } catch (TranslatorException e) {
                this.exceptions.add(e);
            }            
        }
        else {
            this.exceptions.add(new TranslatorException(SimpleDBPlugin.Event.TEIID24001, SimpleDBPlugin.Util.gs(SimpleDBPlugin.Event.TEIID24001)));             
        }
    }

    public Table getTable() {
        return table;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }
    
    public String getCriteria() {
        return this.criteria;
    }    
}
