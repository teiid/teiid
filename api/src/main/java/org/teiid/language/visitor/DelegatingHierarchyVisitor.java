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
import org.teiid.language.LanguageObject;

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

    @Override
    public void visitNode(LanguageObject obj) {
    	if (obj == null) {
    		return;
    	}
    	if (preVisitor != null) {
    		obj.acceptVisitor(preVisitor);
        }
        super.visitNode(obj);
        if (postVisitor != null) {
            obj.acceptVisitor(postVisitor);
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
