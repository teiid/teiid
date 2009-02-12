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

package com.metamatrix.query.optimizer.xml;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import com.metamatrix.query.mapping.xml.MappingAllNode;
import com.metamatrix.query.mapping.xml.MappingAttribute;
import com.metamatrix.query.mapping.xml.MappingBaseNode;
import com.metamatrix.query.mapping.xml.MappingChoiceNode;
import com.metamatrix.query.mapping.xml.MappingDocument;
import com.metamatrix.query.mapping.xml.MappingElement;
import com.metamatrix.query.mapping.xml.MappingRecursiveElement;
import com.metamatrix.query.mapping.xml.MappingSequenceNode;
import com.metamatrix.query.mapping.xml.MappingSourceNode;
import com.metamatrix.query.mapping.xml.MappingVisitor;
import com.metamatrix.query.mapping.xml.Navigator;

/** 
 * This vistor marks all the nodes in the Mapping Document to 
 * "exclude".
 */
public class MarkExcludeVisitor extends MappingVisitor{
    HashSet keepNodes;
    
    public MarkExcludeVisitor(HashSet keppNodes) {
        this.keepNodes = keppNodes;
    }

    // User non-selectable node; just exclude
    public void visit(MappingAllNode all) {
        all.setExclude(true);
    }
    
    //  User non-selectable node; just exclude
    public void visit(MappingSourceNode element) {
        element.setExclude(true);
    }

    // User non-selectable node; just exclude
    public void visit(MappingChoiceNode choice) {
        choice.setExclude(true);
    }
    // User non-selectable node; just exclude
    public void visit(MappingRecursiveElement element) {
        element.setExclude(true);
    }
    // User non-selectable node; just exclude
    public void visit(MappingSequenceNode sequence) {
        sequence.setExclude(true);
    }
     
    public void visit(MappingAttribute attribute) {
        String name = attribute.getCanonicalName();
        if (keepNodes.contains(name)) {
            // we need to keep this; if not already excluded
            if (!attribute.isExcluded()) {
                includeSelf(attribute.getParentNode());
                includeAncestors(attribute.getParentNode());
            }
        }
        else {
            if (!attribute.isAlwaysInclude()) {            
                attribute.setExclude(true);
            }
        }
    }

    public void visit(MappingElement element) {
        String name = element.getCanonicalName();
        if (keepNodes.contains(name)) {
            // we need to keep this; if not already excluded
            if (!element.isExcluded()) {
                includeSelf(element);
                includeAncestors(element);
            }
        }
        else {
            if (!element.isAlwaysInclude()) {
                element.setExclude(true);
            }
        }        
    }

    void includeSelf(MappingBaseNode element) {
        element.setExclude(false);
        
        // if this element has any attributes which are "must have" then
        // turn the exclude off on them.
        if (element instanceof MappingElement) {
            MappingElement elem = (MappingElement)element;
            List attributes = elem.getAttributes();
            if (attributes != null && !attributes.isEmpty()) {
                for (Iterator i = attributes.iterator(); i.hasNext();) {
                    MappingAttribute attribute = (MappingAttribute)i.next();
                    if (attribute.isAlwaysInclude()) {
                        attribute.setExclude(false);
                    }
                }
            }
        }
    }
    
    void includeAncestors(MappingBaseNode element) {
        MappingBaseNode parent = element.getParentNode();
        if (parent != null) {
            includeSelf(parent);
            includeAncestors(parent);
        }
    }
        
    public static MappingDocument markExcludedNodes(MappingDocument doc, HashSet keepNodes) {
        MarkExcludeVisitor visitor = new MarkExcludeVisitor(keepNodes);
        doc.acceptVisitor(new Navigator(true, visitor));
        return doc;
    }
    
}
