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

package org.teiid.query.sql.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.util.Assertion;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.resolver.util.ResolverUtil;
import org.teiid.query.sql.symbol.AliasSymbol;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.ExpressionSymbol;
import org.teiid.query.sql.symbol.GroupSymbol;


public class SymbolMap {

    private LinkedHashMap<ElementSymbol, Expression> map = new LinkedHashMap<ElementSymbol, Expression>();
    private Map<ElementSymbol, Expression> unmodifiableMap = Collections.unmodifiableMap(map);

    public SymbolMap() {
    }

    public SymbolMap clone() {
        SymbolMap clonedMap = new SymbolMap();
        for (Map.Entry<ElementSymbol, Expression> entry : map.entrySet()) {
            clonedMap.addMapping(entry.getKey().clone(), (Expression)entry.getValue().clone());
        }
        return clonedMap;
    }

    public Map<Expression, ElementSymbol> inserseMapping() {
        HashMap<Expression, ElementSymbol> inverseMap = new HashMap<Expression, ElementSymbol>();
        for (Map.Entry<ElementSymbol, Expression> entry : this.map.entrySet()) {
            inverseMap.put(entry.getValue(), entry.getKey());
        }
        return inverseMap;
    }

    /**
     * @return true if the map did not already contained the given symbol
     */
    public boolean addMapping(ElementSymbol symbol,
                              Expression expression) {
        return map.put(symbol, getExpression(expression)) == null;
    }

    public static final Expression getExpression(Expression symbol) {
        if (symbol instanceof AliasSymbol) {
            symbol = ((AliasSymbol)symbol).getSymbol();
        }

        if (symbol instanceof ExpressionSymbol) {
            ExpressionSymbol exprSymbol = (ExpressionSymbol)symbol;
            return exprSymbol.getExpression();
        }

        return symbol;
    }

    public Expression getMappedExpression(ElementSymbol symbol) {
        return map.get(symbol);
    }

    public Map<ElementSymbol, Expression> asUpdatableMap() {
        return this.map;
    }

    public Map<ElementSymbol, Expression> asMap() {
        return unmodifiableMap;
    }

    public List<ElementSymbol> getKeys() {
        return new ArrayList<ElementSymbol>(map.keySet());
    }

    public List<Expression> getValues() {
        return new ArrayList<Expression>(map.values());
    }

    public static final SymbolMap createSymbolMap(GroupSymbol virtualGroup,
                                                  List<? extends Expression> projectCols, QueryMetadataInterface metadata) throws QueryMetadataException, TeiidComponentException {
        return createSymbolMap(ResolverUtil.resolveElementsInGroup(virtualGroup, metadata), projectCols);
    }

    public static final SymbolMap createSymbolMap(List<ElementSymbol> virtualElements,
                                                  List<? extends Expression> mappedCols) {
        Assertion.assertTrue(virtualElements.size() == mappedCols.size());
        SymbolMap symbolMap = new SymbolMap();
        Iterator<ElementSymbol> keyIter = virtualElements.iterator();
        for (Expression symbol : mappedCols) {
            symbolMap.addMapping(keyIter.next(), symbol);
        }

        return symbolMap;
    }

    /**
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return map.toString();
    }

}
