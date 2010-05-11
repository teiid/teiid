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

package org.teiid.query.optimizer.xml;

import org.teiid.query.mapping.xml.MappingAttribute;
import org.teiid.query.mapping.xml.MappingCommentNode;
import org.teiid.query.mapping.xml.MappingElement;
import org.teiid.query.mapping.xml.MappingNode;
import org.teiid.query.mapping.xml.MappingRecursiveElement;
import org.teiid.query.mapping.xml.MappingVisitor;
import org.teiid.query.processor.xml.AddCommentInstruction;
import org.teiid.query.processor.xml.AddNodeInstruction;
import org.teiid.query.processor.xml.NodeDescriptor;
import org.teiid.query.processor.xml.ProcessorInstruction;
import org.teiid.query.sql.symbol.ElementSymbol;


/** 
 * Builds a Element/Attribute or Comment tag for the XML Plan Node.
 */
public class TagBuilderVisitor extends MappingVisitor {
    ProcessorInstruction tag;
    
    public void visit(MappingAttribute attribute) {
        NodeDescriptor descriptor = NodeDescriptor.createNodeDescriptor(attribute);
        if (attribute.getNameInSource() != null) {
            ElementSymbol symbol = attribute.getElementSymbol();
            tag = new AddNodeInstruction(descriptor, symbol);                
        }
        else {
            tag = new AddNodeInstruction(descriptor);            
        }
    }

    public void visit(MappingCommentNode comment) {
        tag = new AddCommentInstruction(comment.getComment());
    }

    public void visit(MappingElement element) {
        visitNode(element);
    }

    public void visit(MappingRecursiveElement element) {
        visitNode(element);
    }

    void visitNode(MappingElement element) {
        NodeDescriptor descriptor = NodeDescriptor.createNodeDescriptor(element);            
        if (element.getNameInSource() != null) {
            ElementSymbol symbol = element.getElementSymbol();
            tag = new AddNodeInstruction(descriptor, symbol);                
        }
        else {
            tag = new AddNodeInstruction(descriptor);            
        }
        
        if(element.isNillable()) {
            NodeDescriptor nillDescriptor = NodeDescriptor.createNillableNode();
            ((AddNodeInstruction)tag).setNillableDescriptor(nillDescriptor);
        }
    }
              
    /**
     * Build tags which are outputs in the resulting xml document.
     * @param node
     * @param planEnv
     * @return
     */
    public static ProcessorInstruction buildTag(MappingNode node) {
        TagBuilderVisitor visitor = new TagBuilderVisitor();
        node.acceptVisitor(visitor);
        return visitor.tag;
    }     
}
