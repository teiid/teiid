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

package org.teiid.query.sql.visitor;

import java.util.Map;

import org.teiid.core.util.Assertion;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.navigator.DeepPreOrderNavigator;
import org.teiid.query.sql.symbol.Symbol;


/**
 * <p> This class is used to update LanguageObjects by replacing the virtual elements/
 * groups present in them with their physical counterparts. It is currently used only
 * to visit Insert/Delete/Update objects and parts of those objects.
 */
public class StaticSymbolMappingVisitor extends AbstractSymbolMappingVisitor {

    private Map symbolMap; // Map between virtual elements/groups and their physical elements

    /**
     * <p> This constructor initialises this object by setting the symbolMap and
     * passing in the command object that is being visited.
     * @param symbolMap A map of virtual elements/groups to their physical counterparts
     */
    public StaticSymbolMappingVisitor(Map symbolMap) {
        super();

        Assertion.isNotNull(symbolMap);
        this.symbolMap = symbolMap;
    }

    /*
     * @see AbstractSymbolMappingVisitor#getMappedSymbol(Symbol)
     */
    protected Symbol getMappedSymbol(Symbol symbol) {
        return (Symbol) this.symbolMap.get(symbol);
    }

    public static void mapSymbols(LanguageObject obj, Map symbolMap) {
        if (obj == null || symbolMap.isEmpty()) {
            return;
        }
        StaticSymbolMappingVisitor ssmv = new StaticSymbolMappingVisitor(symbolMap);
        DeepPreOrderNavigator.doVisit(obj, ssmv);
    }

}
