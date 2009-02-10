/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

package com.metamatrix.connector.visitor.util;

import java.util.*;

import com.metamatrix.connector.language.*;
import com.metamatrix.connector.visitor.framework.DelegatingHierarchyVisitor;
import com.metamatrix.connector.visitor.framework.LanguageObjectVisitor;

/**
 * This visitor can be used to collect all objects of a certain type in a language
 * tree.  Each visit method does an instanceof method to check whether the object
 * is of the expected type.
 */
public class CollectorVisitor implements LanguageObjectVisitor {

    private Class type;
    private Collection objects;

    public CollectorVisitor(Class type) {
        this.type = type;
        reset();
    }

    public void reset() {
        objects= new ArrayList();
    }

    private void checkInstance(ILanguageObject obj) {
        if(type.isInstance(obj)) {
            this.objects.add(obj);
        }
    }
    
    public Collection getCollectedObjects() {
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
     * @see com.metamatrix.data.visitor.framework.LanguageObjectVisitor#visit(com.metamatrix.data.language.ICaseExpression)
     */
    public void visit(ICaseExpression obj) {
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
    public void visit(IBulkInsert obj) {
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
    public static Collection collectObjects(Class type, ILanguageObject object) {
        CollectorVisitor visitor = new CollectorVisitor(type);
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
    public static Collection collectElements(ILanguageObject object) {
        return CollectorVisitor.collectObjects(IElement.class, object);
    }

    /**
     * This is a utility method for a common use of this visitor, which is to collect
     * all groups in an object tree.
     * @param type Language object type to look for
     * @param object Root of the language object tree
     * @return Collection of IGroup of the specified type
     */
    public static Collection collectGroups(ILanguageObject object) {
        return CollectorVisitor.collectObjects(IGroup.class, object);
    }
        
    /**
     * This is a utility method for a common use of this visitor, which is to collect
     * all groups used by all elements in an object tree.
     * @param type Language object type to look for
     * @param object Root of the language object tree
     * @return Set of IGroup
     */
    public static Set collectGroupsUsedByElements(ILanguageObject object) {
        Collection elements = CollectorVisitor.collectElements(object);        
        Set groups = new HashSet();
        Iterator iter = elements.iterator();
        while(iter.hasNext()) {
            IElement element = (IElement) iter.next();
            if(element.getGroup() != null) {
                groups.add(element.getGroup());
            }
        }
        
        return groups;
    }
    
}
