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

import org.teiid.connector.language.IAggregate;
import org.teiid.connector.language.IBatchedUpdates;
import org.teiid.connector.language.ICompareCriteria;
import org.teiid.connector.language.ICompoundCriteria;
import org.teiid.connector.language.IDelete;
import org.teiid.connector.language.IElement;
import org.teiid.connector.language.IExistsCriteria;
import org.teiid.connector.language.IFrom;
import org.teiid.connector.language.IFunction;
import org.teiid.connector.language.IGroup;
import org.teiid.connector.language.IGroupBy;
import org.teiid.connector.language.IInCriteria;
import org.teiid.connector.language.IInlineView;
import org.teiid.connector.language.IInsert;
import org.teiid.connector.language.IInsertExpressionValueSource;
import org.teiid.connector.language.IIsNullCriteria;
import org.teiid.connector.language.IJoin;
import org.teiid.connector.language.ILanguageObject;
import org.teiid.connector.language.ILikeCriteria;
import org.teiid.connector.language.ILimit;
import org.teiid.connector.language.ILiteral;
import org.teiid.connector.language.INotCriteria;
import org.teiid.connector.language.IOrderBy;
import org.teiid.connector.language.IOrderByItem;
import org.teiid.connector.language.IParameter;
import org.teiid.connector.language.IProcedure;
import org.teiid.connector.language.IQuery;
import org.teiid.connector.language.IScalarSubquery;
import org.teiid.connector.language.ISearchedCaseExpression;
import org.teiid.connector.language.ISelect;
import org.teiid.connector.language.ISelectSymbol;
import org.teiid.connector.language.ISetClause;
import org.teiid.connector.language.ISetClauseList;
import org.teiid.connector.language.ISetQuery;
import org.teiid.connector.language.ISubqueryCompareCriteria;
import org.teiid.connector.language.ISubqueryInCriteria;
import org.teiid.connector.language.IUpdate;
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
	private void checkInstance(ILanguageObject obj) {
        if(type.isInstance(obj)) {
            this.objects.add((T)obj);
        }
    }
    
    public Collection<T> getCollectedObjects() {
        return this.objects;
    }

    /* 
     * @see com.metamatrix.data.visitor.framework.LanguageObjectVisitor#visit(com.metamatrix.data.language.IAggregate)
     */
    public void visit(IAggregate obj) {
        checkInstance(obj);        
    }
    
    /*
     * @see com.metamatrix.data.visitor.framework.LanguageObjectVisitor#visit(com.metamatrix.data.language.IBatchedUpdates)
     * @since 4.2
     */
    public void visit(IBatchedUpdates obj) {
        checkInstance(obj);
    }

    /* 
     * @see com.metamatrix.data.visitor.framework.LanguageObjectVisitor#visit(com.metamatrix.data.language.ICompareCriteria)
     */
    public void visit(ICompareCriteria obj) {
        checkInstance(obj);
    }

    /* 
     * @see com.metamatrix.data.visitor.framework.LanguageObjectVisitor#visit(com.metamatrix.data.language.ICompoundCriteria)
     */
    public void visit(ICompoundCriteria obj) {
        checkInstance(obj);
    }

    /* 
     * @see com.metamatrix.data.visitor.framework.LanguageObjectVisitor#visit(com.metamatrix.data.language.IDelete)
     */
    public void visit(IDelete obj) {
        checkInstance(obj);
    }

    /* 
     * @see com.metamatrix.data.visitor.framework.LanguageObjectVisitor#visit(com.metamatrix.data.language.IElement)
     */
    public void visit(IElement obj) {
        checkInstance(obj);
    }

    /* 
     * @see com.metamatrix.data.visitor.framework.LanguageObjectVisitor#visit(com.metamatrix.data.language.IExistsCriteria)
     */
    public void visit(IExistsCriteria obj) {
        checkInstance(obj);
    }

    /* 
     * @see com.metamatrix.data.visitor.framework.LanguageObjectVisitor#visit(com.metamatrix.data.language.IFrom)
     */
    public void visit(IFrom obj) {
        checkInstance(obj);
    }

    /* 
     * @see com.metamatrix.data.visitor.framework.LanguageObjectVisitor#visit(com.metamatrix.data.language.IFunction)
     */
    public void visit(IFunction obj) {
        checkInstance(obj);
    }

    /* 
     * @see com.metamatrix.data.visitor.framework.LanguageObjectVisitor#visit(com.metamatrix.data.language.IGroup)
     */
    public void visit(IGroup obj) {
        checkInstance(obj);
    }

    /* 
     * @see com.metamatrix.data.visitor.framework.LanguageObjectVisitor#visit(com.metamatrix.data.language.IGroupBy)
     */
    public void visit(IGroupBy obj) {
        checkInstance(obj);
    }

    /* 
     * @see com.metamatrix.data.visitor.framework.LanguageObjectVisitor#visit(com.metamatrix.data.language.IInCriteria)
     */
    public void visit(IInCriteria obj) {
        checkInstance(obj);
    }

    public void visit(IInlineView obj) {
        checkInstance(obj);
    }

    /* 
     * @see com.metamatrix.data.visitor.framework.LanguageObjectVisitor#visit(com.metamatrix.data.language.IInsert)
     */
    public void visit(IInsert obj) {
        checkInstance(obj);
    }

    /* 
     * @see com.metamatrix.data.visitor.framework.LanguageObjectVisitor#visit(com.metamatrix.data.language.IBulkInsert)
     */
    public void visit(IInsertExpressionValueSource obj) {
        checkInstance(obj);
    }
    /* 
     * @see com.metamatrix.data.visitor.framework.LanguageObjectVisitor#visit(com.metamatrix.data.language.IIsNullCriteria)
     */
    public void visit(IIsNullCriteria obj) {
        checkInstance(obj);
    }

    /* 
     * @see com.metamatrix.data.visitor.framework.LanguageObjectVisitor#visit(com.metamatrix.data.language.IJoin)
     */
    public void visit(IJoin obj) {
        checkInstance(obj);
    }

    /* 
     * @see com.metamatrix.data.visitor.framework.LanguageObjectVisitor#visit(com.metamatrix.data.language.ILikeCriteria)
     */
    public void visit(ILikeCriteria obj) {
        checkInstance(obj);
    }

    /* 
     * @see com.metamatrix.data.visitor.framework.LanguageObjectVisitor#visit(com.metamatrix.data.language.ILimit)
     */
    public void visit(ILimit obj) {
        checkInstance(obj);
    }

    /* 
     * @see com.metamatrix.data.visitor.framework.LanguageObjectVisitor#visit(com.metamatrix.data.language.ILiteral)
     */
    public void visit(ILiteral obj) {
        checkInstance(obj);
    }

    /* 
     * @see com.metamatrix.data.visitor.framework.LanguageObjectVisitor#visit(com.metamatrix.data.language.INotCriteria)
     */
    public void visit(INotCriteria obj) {
        checkInstance(obj);
    }

    /* 
     * @see com.metamatrix.data.visitor.framework.LanguageObjectVisitor#visit(com.metamatrix.data.language.IOrderBy)
     */
    public void visit(IOrderBy obj) {
        checkInstance(obj);
    }

    /* 
     * @see com.metamatrix.data.visitor.framework.LanguageObjectVisitor#visit(com.metamatrix.data.language.IOrderByItem)
     */
    public void visit(IOrderByItem obj) {
        checkInstance(obj);
    }

    /* 
     * @see com.metamatrix.data.visitor.framework.LanguageObjectVisitor#visit(com.metamatrix.data.language.IParameter)
     */
    public void visit(IParameter obj) {
        checkInstance(obj);
    }

    /* 
     * @see com.metamatrix.data.visitor.framework.LanguageObjectVisitor#visit(com.metamatrix.data.language.IProcedure)
     */
    public void visit(IProcedure obj) {
        checkInstance(obj);
    }

    /* 
     * @see com.metamatrix.data.visitor.framework.LanguageObjectVisitor#visit(com.metamatrix.data.language.IQuery)
     */
    public void visit(IQuery obj) {
        checkInstance(obj);
    }

    /* 
     * @see com.metamatrix.data.visitor.framework.LanguageObjectVisitor#visit(com.metamatrix.data.language.IScalarSubquery)
     */
    public void visit(IScalarSubquery obj) {
        checkInstance(obj);
    }

    /* 
     * @see com.metamatrix.data.visitor.framework.LanguageObjectVisitor#visit(com.metamatrix.data.language.ISearchedCaseExpression)
     */
    public void visit(ISearchedCaseExpression obj) {
        checkInstance(obj);
    }

    /* 
     * @see com.metamatrix.data.visitor.framework.LanguageObjectVisitor#visit(com.metamatrix.data.language.ISelect)
     */
    public void visit(ISelect obj) {
        checkInstance(obj);
    }

    /* 
     * @see com.metamatrix.data.visitor.framework.LanguageObjectVisitor#visit(com.metamatrix.data.language.ISelectSymbol)
     */
    public void visit(ISelectSymbol obj) {
        checkInstance(obj);
    }

    /* 
     * @see com.metamatrix.data.visitor.framework.LanguageObjectVisitor#visit(com.metamatrix.data.language.ISubqueryCompareCriteria)
     */
    public void visit(ISubqueryCompareCriteria obj) {
        checkInstance(obj);
    }

    /* 
     * @see com.metamatrix.data.visitor.framework.LanguageObjectVisitor#visit(com.metamatrix.data.language.ISubqueryInCriteria)
     */
    public void visit(ISubqueryInCriteria obj) {
        checkInstance(obj);
    }

    /* 
     * @see com.metamatrix.data.visitor.framework.LanguageObjectVisitor#visit(com.metamatrix.data.language.IUpdate)
     */
    public void visit(IUpdate obj) {
        checkInstance(obj);
    }
    
    public void visit(ISetQuery obj) {
        checkInstance(obj);
    }
    
    @Override
    public void visit(ISetClauseList obj) {
        checkInstance(obj);
    }
    
    @Override
    public void visit(ISetClause obj) {
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
    public static <T> Collection<T> collectObjects(Class<T> type, ILanguageObject object) {
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
    public static Collection<IElement> collectElements(ILanguageObject object) {
        return CollectorVisitor.collectObjects(IElement.class, object);
    }

    /**
     * This is a utility method for a common use of this visitor, which is to collect
     * all groups in an object tree.
     * @param type Language object type to look for
     * @param object Root of the language object tree
     * @return Collection of IGroup of the specified type
     */
    public static Collection<IGroup> collectGroups(ILanguageObject object) {
        return CollectorVisitor.collectObjects(IGroup.class, object);
    }
        
    /**
     * This is a utility method for a common use of this visitor, which is to collect
     * all groups used by all elements in an object tree.
     * @param type Language object type to look for
     * @param object Root of the language object tree
     * @return Set of IGroup
     */
    public static Set<IGroup> collectGroupsUsedByElements(ILanguageObject object) {
        Set<IGroup> groups = new HashSet<IGroup>();
        for (IElement element : CollectorVisitor.collectElements(object)) {
            if(element.getGroup() != null) {
                groups.add(element.getGroup());
            }
        }
        return groups;
    }
    
}
