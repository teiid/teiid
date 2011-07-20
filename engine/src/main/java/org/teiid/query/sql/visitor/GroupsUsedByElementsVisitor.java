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

package org.teiid.query.sql.visitor;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.GroupSymbol;


public class GroupsUsedByElementsVisitor {

    /**
     * Helper to quickly get the groups from obj in the elements collection
     * @param obj Language object
     * @param elements Collection to collect groups in
     */
    public static final void getGroups(LanguageObject obj, Collection<GroupSymbol> groups) {
        Collection<ElementSymbol> elements = ElementCollectorVisitor.getElements(obj, true);

        for (ElementSymbol elementSymbol : elements) {
        	if (elementSymbol.getGroupSymbol() != null) {
        		groups.add(elementSymbol.getGroupSymbol());  
        	}
        }
    }

    /**
     * Helper to quickly get the groups from obj in a collection.  Duplicates
     * are removed.
     * @param obj Language object
     * @return Collection of {@link org.teiid.query.sql.symbol.GroupSymbol}
     */
    public static final Set<GroupSymbol> getGroups(LanguageObject obj) {
        Set<GroupSymbol> groups = new HashSet<GroupSymbol>();
        getGroups(obj, groups);
        return groups;
    }
    
    public static Set<GroupSymbol> getGroups(Collection<? extends LanguageObject> objects) {
        Set<GroupSymbol> groups = new HashSet<GroupSymbol>();
        getGroups(objects, groups);
        return groups;
    }

    public static void getGroups(Collection<? extends LanguageObject> objects, Set<GroupSymbol> groups) {
        // Get groups from elements     
        for (LanguageObject languageObject : objects) {
            if (languageObject instanceof ElementSymbol) {
                ElementSymbol elem = (ElementSymbol) languageObject;
                groups.add(elem.getGroupSymbol());
            } else {
                GroupsUsedByElementsVisitor.getGroups(languageObject, groups);
            }
        }
    }


}
