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

import org.teiid.query.metadata.TempMetadataID;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.navigator.DeepPreOrderNavigator;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.symbol.Symbol;


/**
 * Used to verify that all symbols in a LanguageObject were resolved
 * with respect to runtime metadata - also that they aren't resolved
 * to TemporaryMetadatID's.
 */
public class CheckNoTempMetadataIDsVisitor extends LanguageVisitor {

    private Collection<Symbol> symbolsWithTempMetadataIDs;

    /**
     * By default, this visitor deeply traverses all commands, and there are
     * no symbols to ignore
     */
    public CheckNoTempMetadataIDsVisitor() {
        symbolsWithTempMetadataIDs = new ArrayList<Symbol>();
    }

    /**
     * Get the Collection of any unresolved symbols
     * @return Collection of any unresolved Symbols; may
     * be empty but never null
     */
    public Collection<Symbol> getSymbols(){
        return this.symbolsWithTempMetadataIDs;
    }

    /**
     * By default, this visitor deeply traverses all commands, and there are
     * no symbols to ignore
     */
    public static final Collection<Symbol> checkSymbols(LanguageObject obj){
        CheckNoTempMetadataIDsVisitor visitor = new CheckNoTempMetadataIDsVisitor();
        DeepPreOrderNavigator.doVisit(obj, visitor);
        return visitor.getSymbols();
    }

    // visitor methods

    public void visit(ElementSymbol obj) {
        if (obj.getMetadataID() instanceof TempMetadataID){
            this.symbolsWithTempMetadataIDs.add(obj);
        }
    }

    public void visit(GroupSymbol obj) {
        if (obj.getMetadataID() instanceof TempMetadataID){
            this.symbolsWithTempMetadataIDs.add(obj);
        }
    }

}
