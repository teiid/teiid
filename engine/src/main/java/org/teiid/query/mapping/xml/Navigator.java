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

package org.teiid.query.mapping.xml;



/** 
 * This visitor walks the Mapping node top/down or bottom/up and lets the supplied
 * visitor visit the nodes at each stage.  
 */
public class Navigator extends MappingVisitor {
    
    private MappingVisitor realVisitor;
    protected boolean preOrder = true;
    
    public Navigator(boolean preOrder, MappingVisitor visitor) {
        this.preOrder = preOrder;
        this.realVisitor = visitor;
    }
        
    public void visit(MappingNode node) {
        if (preOrder) {
            node.acceptVisitor(realVisitor);
        }
        walkChildNodes(node);
        if (!preOrder) {
            node.acceptVisitor(realVisitor);
        }
    }

    /** 
     * @see org.teiid.query.mapping.xml.MappingVisitor#visit(org.teiid.query.mapping.xml.MappingElement)
     */
    public void visit(MappingElement element) {
        if (preOrder) {
            element.acceptVisitor(realVisitor);
        }
        // attributes
        walkAttributes(element);
        
        // walk the children
        walkChildNodes(element);
        
        if (!preOrder) {
            element.acceptVisitor(realVisitor);
        }
    }

    /** 
     * @see org.teiid.query.mapping.xml.MappingVisitor#visit(org.teiid.query.mapping.xml.MappingRecursiveElement)
     */
    public void visit(MappingRecursiveElement element) {
        this.realVisitor.visit(element);
        
        // Do not walk the children of the recursive class as they are
        // of its own kind which has been already been walked.
    }
    
    /** 
     * @see org.teiid.query.mapping.xml.MappingVisitor#shouldAbort()
     */
    protected boolean shouldAbort() {
        return super.shouldAbort() || realVisitor.shouldAbort();
    }
 
}
