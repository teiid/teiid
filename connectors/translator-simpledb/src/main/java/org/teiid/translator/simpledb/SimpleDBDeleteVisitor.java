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

import org.teiid.language.Delete;
import org.teiid.language.NamedTable;
import org.teiid.language.visitor.HierarchyVisitor;
import org.teiid.metadata.Table;
import org.teiid.translator.TranslatorException;

public class SimpleDBDeleteVisitor extends HierarchyVisitor {

    private Table table;
    private String criteria;
    private ArrayList<TranslatorException> exceptions = new ArrayList<TranslatorException>();

    public SimpleDBDeleteVisitor(Delete delete) {
        visitNode(delete);
    }

    public Table getTable(){
        return this.table;
    }
    
    public String getCriteria() {
        return this.criteria;
    }
    
    public void checkExceptions() throws TranslatorException {
        if (!this.exceptions.isEmpty()) {
            throw this.exceptions.get(0);
        }
    }

    @Override
    public void visit(NamedTable obj) {
        super.visit(obj);
        this.table = obj.getMetadataObject();
    }

    @Override
    public void visit(Delete obj) {
        if (obj.getParameterValues() != null) {
            this.exceptions.add(new TranslatorException(SimpleDBPlugin.Event.TEIID24007, SimpleDBPlugin.Util.gs(SimpleDBPlugin.Event.TEIID24007)));
        }
        
        visitNode(obj.getTable());
        if (obj.getWhere() != null) {
            this.criteria = SimpleDBSQLVisitor.getSQLString(obj.getWhere());
        }
    }
}
