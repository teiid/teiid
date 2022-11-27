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

package org.teiid.query.sql.symbol;

import java.util.LinkedList;
import java.util.List;

import org.teiid.core.util.EquivalenceUtil;
import org.teiid.core.util.HashCodeUtil;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.visitor.SQLStringVisitor;

/**
 * <p>This is a subclass of Symbol representing multiple output columns.
 */
public class MultipleElementSymbol implements Expression {
    private List<ElementSymbol> elementSymbols;
    private GroupSymbol group;

    public MultipleElementSymbol() {
    }

    /**
     * Construct a multiple element symbol
     * @param name Name of the symbol
     */
    public MultipleElementSymbol(String name){
        this.group = new GroupSymbol(name);
    }

    @Override
    public Class<?> getType() {
        return null;
    }

    /**
     * Set the {@link ElementSymbol}s that this symbol refers to
     * @param elementSymbols List of {@link ElementSymbol}
     */
    public void setElementSymbols(List<ElementSymbol> elementSymbols){
        this.elementSymbols = elementSymbols;
    }

    /**
     * Get the element symbols referred to by this multiple element symbol
     * @return List of {@link ElementSymbol}s, may be null
     */
    public List<ElementSymbol> getElementSymbols(){
        return this.elementSymbols;
    }

    /**
     * Add an element symbol referenced by this multiple element symbol
     * @param symbol Element symbol referenced by this multiple element symbol
     */
    public void addElementSymbol(ElementSymbol symbol) {
        if(getElementSymbols() == null) {
            setElementSymbols(new LinkedList<ElementSymbol>());
        }
        getElementSymbols().add(symbol);
    }

    public void acceptVisitor(LanguageVisitor visitor) {
        visitor.visit(this);
    }

    /**
     * Return a deep copy of this object
     * @return Deep copy of this object
     */
    public Object clone() {
        MultipleElementSymbol copy = new MultipleElementSymbol();
        if (group != null) {
            copy.group = group.clone();
        }

        List<ElementSymbol> elements = getElementSymbols();
        if(elements != null && elements.size() > 0) {
            copy.setElementSymbols(LanguageObject.Util.deepClone(elements, ElementSymbol.class));
        }

        return copy;
    }

    /**
     * @return null if selecting all groups, otherwise the specific group
     */
    public GroupSymbol getGroup() {
        return group;
    }

    public void setGroup(GroupSymbol group) {
        this.group = group;
    }

    @Override
    public String toString() {
        return SQLStringVisitor.getSQLString(this);
    }

    @Override
    public int hashCode() {
        return HashCodeUtil.hashCode(0, group);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof MultipleElementSymbol)) {
            return false;
        }
        MultipleElementSymbol other = (MultipleElementSymbol)obj;
        return EquivalenceUtil.areEqual(this.group, other.group);
    }

}
