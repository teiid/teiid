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

package org.teiid.connector.visitor.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.teiid.connector.language.AggregateFunction;
import org.teiid.connector.language.AndOr;
import org.teiid.connector.language.Argument;
import org.teiid.connector.language.BatchedUpdates;
import org.teiid.connector.language.Call;
import org.teiid.connector.language.ColumnReference;
import org.teiid.connector.language.Comparison;
import org.teiid.connector.language.Delete;
import org.teiid.connector.language.DerivedColumn;
import org.teiid.connector.language.DerivedTable;
import org.teiid.connector.language.Exists;
import org.teiid.connector.language.ExpressionValueSource;
import org.teiid.connector.language.Function;
import org.teiid.connector.language.GroupBy;
import org.teiid.connector.language.In;
import org.teiid.connector.language.Insert;
import org.teiid.connector.language.IsNull;
import org.teiid.connector.language.Join;
import org.teiid.connector.language.LanguageObject;
import org.teiid.connector.language.Like;
import org.teiid.connector.language.Limit;
import org.teiid.connector.language.Literal;
import org.teiid.connector.language.NamedTable;
import org.teiid.connector.language.Not;
import org.teiid.connector.language.OrderBy;
import org.teiid.connector.language.ScalarSubquery;
import org.teiid.connector.language.SearchedCase;
import org.teiid.connector.language.SearchedWhenClause;
import org.teiid.connector.language.Select;
import org.teiid.connector.language.SetClause;
import org.teiid.connector.language.SetQuery;
import org.teiid.connector.language.SortSpecification;
import org.teiid.connector.language.SubqueryComparison;
import org.teiid.connector.language.SubqueryIn;
import org.teiid.connector.language.Update;
import org.teiid.connector.visitor.framework.DelegatingHierarchyVisitor;
import org.teiid.connector.visitor.framework.LanguageObjectVisitor;


/**
 * This visitor can be used to collect all objects of a certain type in a language
 * tree.  Each visit method does an instanceof method to check whether the object
 * is of the expected type.
 */
public class CollectorVisitor<T> implements LanguageObjectVisitor {

    private Class<T> type;
    private Collection<T> objects = new ArrayList<T>();

    public CollectorVisitor(Class<T> type) {
        this.type = type;
    }

    @SuppressWarnings("unchecked")
	private void checkInstance(LanguageObject obj) {
        if(type.isInstance(obj)) {
            this.objects.add((T)obj);
        }
    }
    
    public Collection<T> getCollectedObjects() {
        return this.objects;
    }

    public void visit(AggregateFunction obj) {
        checkInstance(obj);        
    }
    
    public void visit(BatchedUpdates obj) {
        checkInstance(obj);
    }

    public void visit(Comparison obj) {
        checkInstance(obj);
    }

    public void visit(AndOr obj) {
        checkInstance(obj);
    }

    public void visit(Delete obj) {
        checkInstance(obj);
    }

    public void visit(ColumnReference obj) {
        checkInstance(obj);
    }

    public void visit(Exists obj) {
        checkInstance(obj);
    }

    public void visit(Function obj) {
        checkInstance(obj);
    }

    public void visit(NamedTable obj) {
        checkInstance(obj);
    }

    public void visit(GroupBy obj) {
        checkInstance(obj);
    }

    public void visit(In obj) {
        checkInstance(obj);
    }

    public void visit(DerivedTable obj) {
        checkInstance(obj);
    }

    public void visit(Insert obj) {
        checkInstance(obj);
    }

    public void visit(ExpressionValueSource obj) {
        checkInstance(obj);
    }

    public void visit(IsNull obj) {
        checkInstance(obj);
    }

    public void visit(Join obj) {
        checkInstance(obj);
    }

    public void visit(Like obj) {
        checkInstance(obj);
    }

    public void visit(Limit obj) {
        checkInstance(obj);
    }

    public void visit(Literal obj) {
        checkInstance(obj);
    }

    public void visit(Not obj) {
        checkInstance(obj);
    }

    public void visit(OrderBy obj) {
        checkInstance(obj);
    }

    public void visit(SortSpecification obj) {
        checkInstance(obj);
    }

    public void visit(Argument obj) {
        checkInstance(obj);
    }

    public void visit(Call obj) {
        checkInstance(obj);
    }

    public void visit(Select obj) {
        checkInstance(obj);
    }

    public void visit(ScalarSubquery obj) {
        checkInstance(obj);
    }

    public void visit(SearchedCase obj) {
        checkInstance(obj);
    }

    public void visit(DerivedColumn obj) {
        checkInstance(obj);
    }

    public void visit(SubqueryComparison obj) {
        checkInstance(obj);
    }

    public void visit(SubqueryIn obj) {
        checkInstance(obj);
    }

    public void visit(Update obj) {
        checkInstance(obj);
    }
    
    public void visit(SetQuery obj) {
        checkInstance(obj);
    }
    
    @Override
    public void visit(SetClause obj) {
        checkInstance(obj);
    }
    
    @Override
    public void visit(SearchedWhenClause obj) {
    	checkInstance(obj);    	
    }

    /**
     * This is a utility method to instantiate and run the visitor in conjunction 
     * with a HierarchyVisitor to collect all objects of the specified type
     * of the specified tree in the language object tree.
     * @param type Language object type to look for
     * @param object Root of the language object tree
     * @return Collection of ILanguageObject of the specified type
     */
    public static <T> Collection<T> collectObjects(Class<T> type, LanguageObject object) {
        CollectorVisitor<T> visitor = new CollectorVisitor<T>(type);
        DelegatingHierarchyVisitor hierarchyVisitor = new DelegatingHierarchyVisitor(visitor, null);
        object.acceptVisitor(hierarchyVisitor);
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
