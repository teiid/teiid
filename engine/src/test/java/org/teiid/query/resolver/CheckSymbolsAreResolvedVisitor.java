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

package org.teiid.query.resolver;

import java.util.ArrayList;
import java.util.Collection;

import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.symbol.CaseExpression;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Function;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.symbol.Reference;
import org.teiid.query.sql.symbol.ScalarSubquery;
import org.teiid.query.sql.symbol.SearchedCaseExpression;


/**
 * Used to verify that all symbols in a LanguageObject were resolved
 * with respect to runtime metadata
 */
public class CheckSymbolsAreResolvedVisitor extends LanguageVisitor {

    private Collection<LanguageObject> unresolvedSymbols;
    
	public CheckSymbolsAreResolvedVisitor() { 
        unresolvedSymbols = new ArrayList<LanguageObject>();    
    }
    
    /**
     * Get the Collection of any unresolved symbols
     * @return Collection of any unresolved Symbols; may
     * be empty but never null
     */
    public Collection<LanguageObject> getUnresolvedSymbols(){
        return this.unresolvedSymbols;
    }
    
    public void visit(CaseExpression obj) {
        if (obj.getType() == null){
            this.unresolvedSymbols.add(obj);
        }
    }
    
    public void visit(ElementSymbol obj) {
        if (obj.getMetadataID() == null){
            this.unresolvedSymbols.add(obj);
        }
    }

    public void visit(GroupSymbol obj) {
        if (!obj.isResolved()){
            this.unresolvedSymbols.add(obj);
        }
    }

    public void visit(SearchedCaseExpression obj) {
        if (obj.getType() == null){
            this.unresolvedSymbols.add(obj);
        }
    }
    
    public void visit(ScalarSubquery obj) {
        if (obj.getType() == null){
            this.unresolvedSymbols.add(obj);
        }
    }
    
    public void visit(Function obj) {
        if (obj.getFunctionDescriptor() == null){
            this.unresolvedSymbols.add(obj);
        }
    }
    
    public void visit(Reference obj) {
        if (obj.getType() == null){
            this.unresolvedSymbols.add(obj);
        }
    }
}
