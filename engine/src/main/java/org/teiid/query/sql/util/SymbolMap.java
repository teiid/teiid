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
import org.teiid.query.sql.symbol.AggregateSymbol;
import org.teiid.query.sql.symbol.AliasSymbol;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.ExpressionSymbol;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.symbol.SingleElementSymbol;


public class SymbolMap {

    private LinkedHashMap<ElementSymbol, Expression> map = new LinkedHashMap<ElementSymbol, Expression>();
    private Map<ElementSymbol, Expression> unmodifiableMap = Collections.unmodifiableMap(map);

    public SymbolMap() {
	}
    
    public SymbolMap clone() {
    	SymbolMap clonedMap = new SymbolMap();
    	for (Map.Entry<ElementSymbol, Expression> entry : map.entrySet()) {
			clonedMap.addMapping((ElementSymbol)entry.getKey().clone(), (Expression)entry.getValue().clone());
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
        if (!(symbol instanceof SingleElementSymbol)) {
            return symbol;
        }
        if (symbol instanceof AliasSymbol) {
            symbol = ((AliasSymbol)symbol).getSymbol();
        }

        if (symbol instanceof ExpressionSymbol && !(symbol instanceof AggregateSymbol)) {
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
                                                  List<? extends SingleElementSymbol> projectCols, QueryMetadataInterface metadata) throws QueryMetadataException, TeiidComponentException {
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
