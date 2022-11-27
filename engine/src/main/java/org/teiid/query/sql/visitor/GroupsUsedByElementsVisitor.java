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

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;

import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.GroupSymbol;


public class GroupsUsedByElementsVisitor {

    /**
     * Helper to quickly get the groups from obj in the elements collection
     * @param obj Language object
     * @param groups Collection to collect groups in
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
        Set<GroupSymbol> groups = new LinkedHashSet<GroupSymbol>();
        getGroups(obj, groups);
        return groups;
    }

    public static Set<GroupSymbol> getGroups(Collection<? extends LanguageObject> objects) {
        Set<GroupSymbol> groups = new LinkedHashSet<GroupSymbol>();
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
