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

import org.teiid.language.AggregateFunction;
import org.teiid.language.AndOr;
import org.teiid.language.Argument;
import org.teiid.language.BatchedUpdates;
import org.teiid.language.Call;
import org.teiid.language.ColumnReference;
import org.teiid.language.Comparison;
import org.teiid.language.Delete;
import org.teiid.language.DerivedColumn;
import org.teiid.language.DerivedTable;
import org.teiid.language.Exists;
import org.teiid.language.ExpressionValueSource;
import org.teiid.language.Function;
import org.teiid.language.GroupBy;
import org.teiid.language.In;
import org.teiid.language.Insert;
import org.teiid.language.IsNull;
import org.teiid.language.IteratorValueSource;
import org.teiid.language.Join;
import org.teiid.language.LanguageObject;
import org.teiid.language.Like;
import org.teiid.language.Limit;
import org.teiid.language.Literal;
import org.teiid.language.NamedTable;
import org.teiid.language.Not;
import org.teiid.language.OrderBy;
import org.teiid.language.ScalarSubquery;
import org.teiid.language.SearchedCase;
import org.teiid.language.SearchedWhenClause;
import org.teiid.language.Select;
import org.teiid.language.SetClause;
import org.teiid.language.SetQuery;
import org.teiid.language.SortSpecification;
import org.teiid.language.SubqueryComparison;
import org.teiid.language.SubqueryIn;
import org.teiid.language.Update;


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
    
    @Override
    public void visit(IteratorValueSource obj) {
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
