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

import org.teiid.connector.DataPlugin;
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
import org.teiid.language.Function;
import org.teiid.language.GroupBy;
import org.teiid.language.In;
import org.teiid.language.Insert;
import org.teiid.language.IsNull;
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
