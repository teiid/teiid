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
