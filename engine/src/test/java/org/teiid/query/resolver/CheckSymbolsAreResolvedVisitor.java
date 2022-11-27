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
