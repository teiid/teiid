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

package org.teiid.connector.visitor.framework;

import org.teiid.connector.DataPlugin;
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

    public void visit(AggregateFunction obj) {
        if (preVisitor != null) {
            preVisitor.visit(obj);
        }
        super.visit(obj);
        if (postVisitor != null) {
            postVisitor.visit(obj);
        }
    }
    
    public void visit(BatchedUpdates obj) {
        if (preVisitor != null) {
            preVisitor.visit(obj);
        }
        super.visit(obj);
        if (postVisitor != null) {
            postVisitor.visit(obj);
        }
    }

    public void visit(Comparison obj) {
        if (preVisitor != null) {
            preVisitor.visit(obj);
        }
        super.visit(obj);
        if (postVisitor != null) {
            postVisitor.visit(obj);
        }
    }
    public void visit(AndOr obj) {
        if (preVisitor != null) {
            preVisitor.visit(obj);
        }
        super.visit(obj);
        if (postVisitor != null) {
            postVisitor.visit(obj);
        }
    }
    public void visit(Delete obj) {
        if (preVisitor != null) {
            preVisitor.visit(obj);
        }
        super.visit(obj);
        if (postVisitor != null) {
            postVisitor.visit(obj);
        }
    }
    public void visit(ColumnReference obj) {
        if (preVisitor != null) {
            preVisitor.visit(obj);
        }
        super.visit(obj);
        if (postVisitor != null) {
            postVisitor.visit(obj);
        }
    }
    public void visit(Call obj) {
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
    public void visit(Exists obj) {
        if (preVisitor != null) {
            preVisitor.visit(obj);
        }
        super.visit(obj);
        if (postVisitor != null) {
            postVisitor.visit(obj);
        }
    }
    public void visit(Function obj) {
        if (preVisitor != null) {
            preVisitor.visit(obj);
        }
        super.visit(obj);
        if (postVisitor != null) {
            postVisitor.visit(obj);
        }
    }
    public void visit(NamedTable obj) {
        if (preVisitor != null) {
            preVisitor.visit(obj);
        }
        super.visit(obj);
        if (postVisitor != null) {
            postVisitor.visit(obj);
        }
    }
    public void visit(GroupBy obj) {
        if (preVisitor != null) {
            preVisitor.visit(obj);
        }
        super.visit(obj);
        if (postVisitor != null) {
            postVisitor.visit(obj);
        }
    }
    public void visit(In obj) {
        if (preVisitor != null) {
            preVisitor.visit(obj);
        }
        super.visit(obj);
        if (postVisitor != null) {
            postVisitor.visit(obj);
        }
    }
    
    public void visit(DerivedTable obj) {
        if (preVisitor != null) {
            preVisitor.visit(obj);
        }
        super.visit(obj);
        if (postVisitor != null) {
            postVisitor.visit(obj);
        }
    }

    public void visit(Insert obj) {
        if (preVisitor != null) {
            preVisitor.visit(obj);
        }
        super.visit(obj);
        if (postVisitor != null) {
            postVisitor.visit(obj);
        }
    }
    public void visit(IsNull obj) {
        if (preVisitor != null) {
            preVisitor.visit(obj);
        }
        super.visit(obj);
        if (postVisitor != null) {
            postVisitor.visit(obj);
        }
    }
    public void visit(Join obj) {
        if (preVisitor != null) {
            preVisitor.visit(obj);
        }
        super.visit(obj);
        if (postVisitor != null) {
            postVisitor.visit(obj);
        }
    }
    public void visit(Like obj) {
        if (preVisitor != null) {
            preVisitor.visit(obj);
        }
        super.visit(obj);
        if (postVisitor != null) {
            postVisitor.visit(obj);
        }
    }
    public void visit(Limit obj) {
        if (preVisitor != null) {
            preVisitor.visit(obj);
        }
        super.visit(obj);
        if (postVisitor != null) {
            postVisitor.visit(obj);
        }
    }
    public void visit(Literal obj) {
        if (preVisitor != null) {
            preVisitor.visit(obj);
        }
        super.visit(obj);
        if (postVisitor != null) {
            postVisitor.visit(obj);
        }
    }
    public void visit(Not obj) {
        if (preVisitor != null) {
            preVisitor.visit(obj);
        }
        super.visit(obj);
        if (postVisitor != null) {
            postVisitor.visit(obj);
        }
    }
    public void visit(OrderBy obj) {
        if (preVisitor != null) {
            preVisitor.visit(obj);
        }
        super.visit(obj);
        if (postVisitor != null) {
            postVisitor.visit(obj);
        }
    }
    public void visit(SortSpecification obj) {
        if (preVisitor != null) {
            preVisitor.visit(obj);
        }
        super.visit(obj);
        if (postVisitor != null) {
            postVisitor.visit(obj);
        }
    }
    public void visit(Argument obj) {
        if (preVisitor != null) {
            preVisitor.visit(obj);
        }
        super.visit(obj);
        if (postVisitor != null) {
            postVisitor.visit(obj);
        }
    }
    public void visit(Select obj) {
        if (preVisitor != null) {
            preVisitor.visit(obj);
        }
        super.visit(obj);
        if (postVisitor != null) {
            postVisitor.visit(obj);
        }
    }

    public void visit(ScalarSubquery obj) {
        if (preVisitor != null) {
            preVisitor.visit(obj);
        }
        super.visit(obj);
        if (postVisitor != null) {
            postVisitor.visit(obj);
        }
    }
    public void visit(SearchedCase obj) {
        if (preVisitor != null) {
            preVisitor.visit(obj);
        }
        super.visit(obj);
        if (postVisitor != null) {
            postVisitor.visit(obj);
        }
    }
    public void visit(DerivedColumn obj) {
        if (preVisitor != null) {
            preVisitor.visit(obj);
        }
        super.visit(obj);
        if (postVisitor != null) {
            postVisitor.visit(obj);
        }
    }
    public void visit(SubqueryComparison obj) {
        if (preVisitor != null) {
            preVisitor.visit(obj);
        }
        super.visit(obj);
        if (postVisitor != null) {
            postVisitor.visit(obj);
        }
    }
    public void visit(SubqueryIn obj) {
        if (preVisitor != null) {
            preVisitor.visit(obj);
        }
        super.visit(obj);
        if (postVisitor != null) {
            postVisitor.visit(obj);
        }
    }
    
    public void visit(SetQuery obj) {
        if (preVisitor != null) {
            preVisitor.visit(obj);
        }
        super.visit(obj);
        if (postVisitor != null) {
            postVisitor.visit(obj);
        }
    }

    public void visit(Update obj) {
        if (preVisitor != null) {
            preVisitor.visit(obj);
        }
        super.visit(obj);
        if (postVisitor != null) {
            postVisitor.visit(obj);
        }
    }
    
    @Override
    public void visit(SetClause obj) {
        if (preVisitor != null) {
            preVisitor.visit(obj);
        }
        super.visit(obj);
        if (postVisitor != null) {
            postVisitor.visit(obj);
        }
    }
    
    @Override
    public void visit(SearchedWhenClause obj) {
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
    public static void preOrderVisit(LanguageObjectVisitor behaviorVisitor, LanguageObject object) {
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
    public static void postOrderVisit(LanguageObjectVisitor behaviorVisitor, LanguageObject object) {
        DelegatingHierarchyVisitor hierarchyVisitor = new DelegatingHierarchyVisitor(null, behaviorVisitor);
        object.acceptVisitor(hierarchyVisitor);
    }
    
}
