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

package org.teiid.query.resolver.util;

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;

import org.teiid.query.sql.symbol.ElementSymbol;


/**
 * This class represents both virtual and physical access patterns.
 *
 * If a virtual access pattern is initially unsatisfied, it may be
 * transformed by RuleMergeVirtual.  In this case, the history of the
 * access pattern will contain its previous definitions.
 */
public class AccessPattern implements Comparable<AccessPattern>, Cloneable {

    private Set<ElementSymbol> unsatisfied = new HashSet<ElementSymbol>();
    private LinkedList<Collection<ElementSymbol>> history = new LinkedList<Collection<ElementSymbol>>();

    public AccessPattern(Collection<ElementSymbol> elements) {
        unsatisfied.addAll(elements);
        history.add(elements);
    }

    public Collection<ElementSymbol> getCurrentElements() {
        return history.getFirst();
    }

    public void addElementHistory(Collection<ElementSymbol> elements) {
        this.history.addFirst(elements);
    }

    /**
     * @return Returns the history.
     */
    public LinkedList<Collection<ElementSymbol>> getHistory() {
        return this.history;
    }

    /**
     * @return Returns the unsatisfied.
     */
    public Set<ElementSymbol> getUnsatisfied() {
        return this.unsatisfied;
    }

    /**
     * @param unstaisfied The unsatisfied to set.
     */
    public void setUnsatisfied(Set<ElementSymbol> unstaisfied) {
        this.unsatisfied = unstaisfied;
    }

    /**
     * @see java.lang.Object#toString()
     */
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("Access Pattern: Unsatisfied "); //$NON-NLS-1$
        sb.append(unsatisfied);
        sb.append(" History "); //$NON-NLS-1$
        sb.append(history);
        return sb.toString();
    }

    @Override
    public int compareTo(AccessPattern other) {
        if (this.unsatisfied.size() > other.unsatisfied.size()){
            return 1;
        } else if (this.unsatisfied.size() < other.unsatisfied.size()){
            return -1;
        }
        return 0;
    }

    /**
     * @see java.lang.Object#clone()
     */
    public Object clone() {
        try {
            return super.clone();
        } catch (CloneNotSupportedException err) {
            return null;
        }
    }

}
