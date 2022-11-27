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

package org.teiid.query.metadata;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.teiid.query.sql.symbol.ElementSymbol;


public class GroupInfo implements Serializable {

    private static final long serialVersionUID = 5724520038004637086L;

    public static final String CACHE_PREFIX = "groupinfo/"; //$NON-NLS-1$

    private Map<Object, ElementSymbol> idToSymbolMap;
    private List<ElementSymbol> symbolList;
    private Map<String, ElementSymbol> shortNameToSymbolMap;

    public GroupInfo(LinkedHashMap<Object, ElementSymbol> symbols) {
        this.idToSymbolMap = symbols;
        this.symbolList = Collections.unmodifiableList(new ArrayList<ElementSymbol>(symbols.values()));
        this.shortNameToSymbolMap = new TreeMap<String, ElementSymbol>(String.CASE_INSENSITIVE_ORDER);
        for (ElementSymbol symbol : symbolList) {
            shortNameToSymbolMap.put(symbol.getShortName(), symbol);
        }
    }

    public List<ElementSymbol> getSymbolList() {
        return symbolList;
    }

    public ElementSymbol getSymbol(Object metadataID) {
        return idToSymbolMap.get(metadataID);
    }

    public ElementSymbol getSymbol(String shortCanonicalName) {
        return shortNameToSymbolMap.get(shortCanonicalName);
    }

}
