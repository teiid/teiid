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

package org.teiid.language.visitor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.teiid.language.ColumnReference;
import org.teiid.language.LanguageObject;
import org.teiid.language.NamedTable;


/**
 * This visitor can be used to collect all objects of a certain type in a language
 * tree.  Each visit method does an instanceof method to check whether the object
 * is of the expected type.
 */
public class CollectorVisitor<T> extends HierarchyVisitor {

    private Class<T> type;
    private Collection<T> objects = new ArrayList<T>();

    public CollectorVisitor(Class<T> type) {
        this.type = type;
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public void visitNode(LanguageObject obj) {
        if(type.isInstance(obj)) {
            this.objects.add((T)obj);
        }
    	super.visitNode(obj);
    }

    public Collection<T> getCollectedObjects() {
        return this.objects;
    }

    /**
     * This is a utility method to instantiate and run the visitor in conjunction 
     * with a HierarchyVisitor to collect all objects of the specified type
     * of the specified tree in the language object tree.
     * @param type Language object type to look for
     * @param object Root of the language object tree
     * @return Collection of LanguageObject of the specified type
     */
    public static <T> Collection<T> collectObjects(Class<T> type, LanguageObject object) {
        CollectorVisitor<T> visitor = new CollectorVisitor<T>(type);
        visitor.visitNode(object);
        return visitor.getCollectedObjects();
    }
    
    /**
     * This is a utility method for a common use of this visitor, which is to collect
     * all elements in an object tree.
     * @param type Language object type to look for
     * @param object Root of the language object tree
     * @return Collection of IElement of the specified type
     */
    public static Collection<ColumnReference> collectElements(LanguageObject object) {
        return CollectorVisitor.collectObjects(ColumnReference.class, object);
    }

    /**
     * This is a utility method for a common use of this visitor, which is to collect
     * all groups in an object tree.
     * @param type Language object type to look for
     * @param object Root of the language object tree
     * @return Collection of IGroup of the specified type
     */
    public static Collection<NamedTable> collectGroups(LanguageObject object) {
        return CollectorVisitor.collectObjects(NamedTable.class, object);
    }
        
    /**
     * This is a utility method for a common use of this visitor, which is to collect
     * all groups used by all elements in an object tree.
     * @param type Language object type to look for
     * @param object Root of the language object tree
     * @return Set of IGroup
     */
    public static Set<NamedTable> collectGroupsUsedByElements(LanguageObject object) {
        Set<NamedTable> groups = new HashSet<NamedTable>();
        for (ColumnReference element : CollectorVisitor.collectElements(object)) {
            if(element.getTable() != null) {
                groups.add(element.getTable());
            }
        }
        return groups;
    }
    
}
