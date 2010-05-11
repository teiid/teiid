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

import org.teiid.query.mapping.xml.MappingBaseNode;
import org.teiid.query.mapping.xml.MappingDocument;
import org.teiid.query.mapping.xml.MappingRecursiveElement;
import org.teiid.query.mapping.xml.MappingSourceNode;
import org.teiid.query.mapping.xml.MappingVisitor;
import org.teiid.query.mapping.xml.Navigator;


/** 
 * This class visits all the base nodes with "source" property set and extracts then and makes
 * then independent nodes. for example:
 *  <code>
 *  ParentElement
 *      MappingElement A (source X)
 *  
 *  converted to
 *  ParentElement
 *      SourceNode X
 *          MappingElement A
 *  </code>
 */
public class SourceNodeGenaratorVisitor extends MappingVisitor {

    
    /** 
     * In this code since we are only traversing the child nodes after the modification
     * the removal and addtion of the nodes to the current may not affect the underlying
     * collection object.
     */
    public void visit(MappingBaseNode baseNode) {
        String source = baseNode.getSource();
        if(source != null) {
            // create the source node
            MappingSourceNode sourceNode = new MappingSourceNode(source);
            if (baseNode instanceof MappingRecursiveElement) {
                sourceNode.setAliasResultName(((MappingRecursiveElement)baseNode).getMappingClass());
            }
            
            // Get the parent of current node
            MappingBaseNode parent = baseNode.getParentNode();
            
            // remove the current node from its parent
            parent.removeChildNode(baseNode);
            
            // make source is child of current nodes parent
            parent.addSourceNode(sourceNode);
            
            // now make the current node, child of the source.
            sourceNode.addChild(baseNode);
            sourceNode.setExclude(baseNode.isExcluded());
            sourceNode.setMinOccurrs(baseNode.getMinOccurence());
            sourceNode.setMaxOccurrs(baseNode.getMaxOccurence());
            sourceNode.setStagingTables(baseNode.getStagingTables());
            baseNode.setSource(null);
            baseNode.setStagingTables(null);
        }
    } 
    
    /**
     * This visitor will extract the 'source' property off of the all the base nodes
     * and create a separate MappingSource node and make the original node as its immediate
     * child. 
     * @param doc
     * @return
     */
    public static MappingDocument extractSourceNodes(MappingDocument doc) {
        SourceNodeGenaratorVisitor real = new SourceNodeGenaratorVisitor();
        doc.acceptVisitor(new Navigator(true, real));
        return doc;
    }
  
}
