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

import java.util.Iterator;
import java.util.List;


/** 
 * A Visitor framework for navigating the Mapping Nodes
 */
public class MappingVisitor {
    private boolean abort = false;

    protected void setAbort(boolean abort) {
        this.abort = abort;
    }
    
    protected boolean shouldAbort() {
        return abort;
    }    
    
    /**
	 * @param node  
	 */
    public void visit(MappingNode node) {}
    
    public void visit(MappingDocument doc) {
        visit((MappingNode)doc);
    }
    public void visit(MappingElement element) {
        visit((MappingBaseNode)element);
    }
    public void visit(MappingAttribute attribute) {
        visit((MappingNode)attribute);
    }
    public void visit(MappingBaseNode baseNode) {
        visit((MappingNode)baseNode);
    }
    public void visit(MappingChoiceNode choice) {
        visit((MappingBaseNode)choice);
    }
    public void visit(MappingSequenceNode sequence) {
        visit((MappingBaseNode)sequence);
    }
    public void visit(MappingAllNode all) {
        visit((MappingBaseNode)all);
    }
    public void visit(MappingCommentNode comment) {
        visit((MappingNode)comment);
    }
    public void visit(MappingCriteriaNode node) {
        visit((MappingBaseNode)node);
    }
    public void visit(MappingRecursiveElement element) {
        visit((MappingElement)element);
    }
    public void visit(MappingSourceNode element) {
        visit((MappingBaseNode)element);
    }
    /** 
     * @param element
     */
    protected void walkChildNodes(MappingNode element) {

        List<MappingNode> children = element.getNodeChildren();
        for(Iterator<MappingNode> i=children.iterator(); i.hasNext();) {
            
            if (shouldAbort()) {
                break;
            }
            
            MappingNode node = i.next();            
            node.acceptVisitor(this);
        }
    }    
    
    /** 
     * @param element
     */
    protected void walkAttributes(MappingElement element) {
        List attributes = element.getAttributes();
        for(Iterator i=attributes.iterator(); i.hasNext();) {
            if (shouldAbort()) {
                break;
            }            
            visit((MappingAttribute)i.next());
        }
    }     
}
