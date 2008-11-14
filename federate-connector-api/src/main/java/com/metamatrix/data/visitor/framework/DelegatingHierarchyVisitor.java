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

package com.metamatrix.data.visitor.framework;

import com.metamatrix.data.DataPlugin;
import com.metamatrix.data.language.*;

/**
 * Delegates pre- and post-processing for each node in the hierarchy to
 * delegate visitors.
 */
public class DelegatingHierarchyVisitor extends HierarchyVisitor {
    private LanguageObjectVisitor postVisitor = null;
    
    private LanguageObjectVisitor preVisitor = null;
    
    public DelegatingHierarchyVisitor(LanguageObjectVisitor preProcessingDelegate,
                                      LanguageObjectVisitor postProcessingDelegate) {
        if (preProcessingDelegate == null && postProcessingDelegate == null) {
            throw new IllegalArgumentException(DataPlugin.Util.getString("DelegatingHierarchyVisitor.The_pre-_and_post-processing_visitors_cannot_both_be_null._1")); //$NON-NLS-1$
        }
        this.preVisitor = preProcessingDelegate;
        this.postVisitor = postProcessingDelegate;
    }

    protected LanguageObjectVisitor getPostVisitor() {
        return postVisitor;
    }
    
    protected LanguageObjectVisitor getPreVisitor() {
        return preVisitor;
    }

    public void visit(IAggregate obj) {
        if (preVisitor != null) {
            preVisitor.visit(obj);
        }
        super.visit(obj);
        if (postVisitor != null) {
            postVisitor.visit(obj);
        }
    }
    
    public void visit(IBatchedUpdates obj) {
        if (preVisitor != null) {
            preVisitor.visit(obj);
        }
        super.visit(obj);
        if (postVisitor != null) {
            postVisitor.visit(obj);
        }
    }
    public void visit(ICaseExpression obj) {
        if (preVisitor != null) {
            preVisitor.visit(obj);
        }
        super.visit(obj);
        if (postVisitor != null) {
            postVisitor.visit(obj);
        }
    }
    public void visit(ICompareCriteria obj) {
        if (preVisitor != null) {
            preVisitor.visit(obj);
        }
        super.visit(obj);
        if (postVisitor != null) {
            postVisitor.visit(obj);
        }
    }
    public void visit(ICompoundCriteria obj) {
        if (preVisitor != null) {
            preVisitor.visit(obj);
        }
        super.visit(obj);
        if (postVisitor != null) {
            postVisitor.visit(obj);
        }
    }
    public void visit(IDelete obj) {
        if (preVisitor != null) {
            preVisitor.visit(obj);
        }
        super.visit(obj);
        if (postVisitor != null) {
            postVisitor.visit(obj);
        }
    }
    public void visit(IElement obj) {
        if (preVisitor != null) {
            preVisitor.visit(obj);
        }
        super.visit(obj);
        if (postVisitor != null) {
            postVisitor.visit(obj);
        }
    }
    public void visit(IProcedure obj) {
        if (preVisitor != null) {
            preVisitor.visit(obj);
        }
        super.visit(obj);
        if (postVisitor != null) {
            postVisitor.visit(obj);
        }
    }
    /* 
     * @see com.metamatrix.data.visitor.LanguageObjectVisitor#visit(com.metamatrix.data.language.IExistsCriteria)
     */
    public void visit(IExistsCriteria obj) {
        if (preVisitor != null) {
            preVisitor.visit(obj);
        }
        super.visit(obj);
        if (postVisitor != null) {
            postVisitor.visit(obj);
        }
    }
    public void visit(IFrom obj) {
        if (preVisitor != null) {
            preVisitor.visit(obj);
        }
        super.visit(obj);
        if (postVisitor != null) {
            postVisitor.visit(obj);
        }        
    }
    public void visit(IFunction obj) {
        if (preVisitor != null) {
            preVisitor.visit(obj);
        }
        super.visit(obj);
        if (postVisitor != null) {
            postVisitor.visit(obj);
        }
    }
    public void visit(IGroup obj) {
        if (preVisitor != null) {
            preVisitor.visit(obj);
        }
        super.visit(obj);
        if (postVisitor != null) {
            postVisitor.visit(obj);
        }
    }
    public void visit(IGroupBy obj) {
        if (preVisitor != null) {
            preVisitor.visit(obj);
        }
        super.visit(obj);
        if (postVisitor != null) {
            postVisitor.visit(obj);
        }
    }
    public void visit(IInCriteria obj) {
        if (preVisitor != null) {
            preVisitor.visit(obj);
        }
        super.visit(obj);
        if (postVisitor != null) {
            postVisitor.visit(obj);
        }
    }
    
    public void visit(IInlineView obj) {
        if (preVisitor != null) {
            preVisitor.visit(obj);
        }
        super.visit(obj);
        if (postVisitor != null) {
            postVisitor.visit(obj);
        }
    }

    public void visit(IInsert obj) {
        if (preVisitor != null) {
            preVisitor.visit(obj);
        }
        super.visit(obj);
        if (postVisitor != null) {
            postVisitor.visit(obj);
        }
    }
    public void visit(IIsNullCriteria obj) {
        if (preVisitor != null) {
            preVisitor.visit(obj);
        }
        super.visit(obj);
        if (postVisitor != null) {
            postVisitor.visit(obj);
        }
    }
    public void visit(IJoin obj) {
        if (preVisitor != null) {
            preVisitor.visit(obj);
        }
        super.visit(obj);
        if (postVisitor != null) {
            postVisitor.visit(obj);
        }
    }
    public void visit(ILikeCriteria obj) {
        if (preVisitor != null) {
            preVisitor.visit(obj);
        }
        super.visit(obj);
        if (postVisitor != null) {
            postVisitor.visit(obj);
        }
    }
    public void visit(ILimit obj) {
        if (preVisitor != null) {
            preVisitor.visit(obj);
        }
        super.visit(obj);
        if (postVisitor != null) {
            postVisitor.visit(obj);
        }
    }
    public void visit(ILiteral obj) {
        if (preVisitor != null) {
            preVisitor.visit(obj);
        }
        super.visit(obj);
        if (postVisitor != null) {
            postVisitor.visit(obj);
        }
    }
    public void visit(INotCriteria obj) {
        if (preVisitor != null) {
            preVisitor.visit(obj);
        }
        super.visit(obj);
        if (postVisitor != null) {
            postVisitor.visit(obj);
        }
    }
    public void visit(IOrderBy obj) {
        if (preVisitor != null) {
            preVisitor.visit(obj);
        }
        super.visit(obj);
        if (postVisitor != null) {
            postVisitor.visit(obj);
        }
    }
    public void visit(IOrderByItem obj) {
        if (preVisitor != null) {
            preVisitor.visit(obj);
        }
        super.visit(obj);
        if (postVisitor != null) {
            postVisitor.visit(obj);
        }
    }
    public void visit(IParameter obj) {
        if (preVisitor != null) {
            preVisitor.visit(obj);
        }
        super.visit(obj);
        if (postVisitor != null) {
            postVisitor.visit(obj);
        }
    }
    public void visit(IQuery obj) {
        if (preVisitor != null) {
            preVisitor.visit(obj);
        }
        super.visit(obj);
        if (postVisitor != null) {
            postVisitor.visit(obj);
        }
    }

    /* 
     * @see com.metamatrix.data.visitor.LanguageObjectVisitor#visit(com.metamatrix.data.language.IScalarSubquery)
     */
    public void visit(IScalarSubquery obj) {
        if (preVisitor != null) {
            preVisitor.visit(obj);
        }
        super.visit(obj);
        if (postVisitor != null) {
            postVisitor.visit(obj);
        }
    }
    public void visit(ISearchedCaseExpression obj) {
        if (preVisitor != null) {
            preVisitor.visit(obj);
        }
        super.visit(obj);
        if (postVisitor != null) {
            postVisitor.visit(obj);
        }
    }
    public void visit(ISelect obj) {
        if (preVisitor != null) {
            preVisitor.visit(obj);
        }
        super.visit(obj);
        if (postVisitor != null) {
            postVisitor.visit(obj);
        }
    }
    public void visit(ISelectSymbol obj) {
        if (preVisitor != null) {
            preVisitor.visit(obj);
        }
        super.visit(obj);
        if (postVisitor != null) {
            postVisitor.visit(obj);
        }
    }
    public void visit(ISubqueryCompareCriteria obj) {
        if (preVisitor != null) {
            preVisitor.visit(obj);
        }
        super.visit(obj);
        if (postVisitor != null) {
            postVisitor.visit(obj);
        }
    }
    public void visit(ISubqueryInCriteria obj) {
        if (preVisitor != null) {
            preVisitor.visit(obj);
        }
        super.visit(obj);
        if (postVisitor != null) {
            postVisitor.visit(obj);
        }
    }
    
    
    public void visit(ISetQuery obj) {
        if (preVisitor != null) {
            preVisitor.visit(obj);
        }
        super.visit(obj);
        if (postVisitor != null) {
            postVisitor.visit(obj);
        }
    }

    public void visit(IUpdate obj) {
        if (preVisitor != null) {
            preVisitor.visit(obj);
        }
        super.visit(obj);
        if (postVisitor != null) {
            postVisitor.visit(obj);
        }
    }
    
    /** 
     * This utility method can be used to execute the behaviorVisitor in a pre-order walk
     * of the language objects.  "Pre-order" in this case means that the visit method of the 
     * behaviorVisitor will be called before the visit method of it's children.  It is expected
     * that the behavior visit does NOT perform iteration, as that function will be performed
     * by the HierarchyVisitor.
     * @param behaviorVisitor The visitor specifying what behavior is performed at each node type
     * @param object The root of the object tree to perform visitation on
     */
    public static void preOrderVisit(LanguageObjectVisitor behaviorVisitor, ILanguageObject object) {
        DelegatingHierarchyVisitor hierarchyVisitor = new DelegatingHierarchyVisitor(behaviorVisitor, null);
        object.acceptVisitor(hierarchyVisitor);
    }

    /** 
     * This utility method can be used to execute the behaviorVisitor in a post-order walk
     * of the language objects.  "Post-order" in this case means that the visit method of the 
     * behaviorVisitor will be called after the visit method of it's children.  It is expected
     * that the behavior visit does NOT perform iteration, as that function will be performed
     * by the HierarchyVisitor.
     * @param behaviorVisitor The visitor specifying what behavior is performed at each node type
     * @param object The root of the object tree to perform visitation on
     */
    public static void postOrderVisit(LanguageObjectVisitor behaviorVisitor, ILanguageObject object) {
        DelegatingHierarchyVisitor hierarchyVisitor = new DelegatingHierarchyVisitor(null, behaviorVisitor);
        object.acceptVisitor(hierarchyVisitor);
    }
    
}
