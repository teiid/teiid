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

import java.util.ArrayList;
import java.util.List;

import org.teiid.core.TeiidRuntimeException;
import org.teiid.query.QueryPlugin;



/** 
 * This is base class to define all nodes except the attribute. However, this quite not
 * enough to define a Element node. Specially designed for sequence, choice and all node 
 * types
 */
public abstract class MappingBaseNode extends MappingNode {
    // An ID on the recursive parent as to who the recursive child node is?  
    String recursionId;
    
    protected MappingBaseNode() {
        // implicit constructor
    }
    
    public MappingBaseNode setMinOccurrs(int cardinality) {
        setProperty(MappingNodeConstants.Properties.CARDINALITY_MIN_BOUND, new Integer(cardinality));
        return this;
    }

    public MappingBaseNode setMaxOccurrs(int cardinality) {
        setProperty(MappingNodeConstants.Properties.CARDINALITY_MAX_BOUND, new Integer(cardinality));
        return this;
    }    
         
    public MappingBaseNode setSource(String source) {
        if (source != null) {
            setProperty(MappingNodeConstants.Properties.RESULT_SET_NAME, source);
        }
        else {
            removeProperty(MappingNodeConstants.Properties.RESULT_SET_NAME);
        }
        return this;
    }
         
    public String getSource() {
        return (String)getProperty(MappingNodeConstants.Properties.RESULT_SET_NAME);
    }     
    
    public MappingElement addChildElement(MappingElement elem) {
        if (elem.isRecursive()) {
            MappingRecursiveElement recursiveElement = (MappingRecursiveElement)elem;
            MappingBaseNode recursiveRoot = getRecursiveRootNode(recursiveElement);
            recursiveRoot.setRootRecursiveNode(true, recursiveElement.getMappingClass().toUpperCase());
            addChild(elem);
        }
        else {
            addChild(elem);
        }
        return elem;
    }
    
    private MappingBaseNode getRecursiveRootNode(MappingRecursiveElement elem) {
        if (hasSource(elem.getMappingClass())) {
            return this;
        }
        MappingBaseNode parent = this.getParentNode();
        if (parent != null) {
            return parent.getRecursiveRootNode(elem);
        }
        throw new TeiidRuntimeException(QueryPlugin.Util.getString("invalid_recurive_node", elem)); //$NON-NLS-1$
    }
    
    /**
     * Any node with its parent node of Source Node, is like property on node itself, 
     * as all the source nodes will have atmost one child. 
     * @param source
     * @return
     */
    private boolean hasSource(String source) {
        return source.equals(getSource());
    }
    
    public MappingChoiceNode addChoiceNode(MappingChoiceNode elem) {
        addChild(elem);
        return elem;
    }
    
    public MappingSequenceNode addSequenceNode(MappingSequenceNode elem) {
        addChild(elem);
        return elem;
    }
    
    public MappingAllNode addAllNode(MappingAllNode elem) {
        addChild(elem);
        return elem;
    }     
    
    public MappingSourceNode addSourceNode(MappingSourceNode elem) {
        addChild(elem);
        return elem;
    }     
    
    public MappingCriteriaNode addCriteriaNode(MappingCriteriaNode node) {
        addChild(node);
        return node;        
    }
    
    public MappingBaseNode getParentNode() {
        if (getParent() instanceof MappingBaseNode) {
            return (MappingBaseNode)getParent();
        }
        return null;
    }
    
    public String getName() {
        // if we decide to give the choice/seq/all nodes names then
        // we need to change the logic in the "getFullName()"
        return null;
    }
    
    public String getCanonicalName() {
        return getFullyQualifiedName().toUpperCase();
    }
            
    public void removeChildNode(MappingBaseNode toRemove) {
        getChildren().remove(toRemove);
    }
        
    public int getMinOccurence() {
        Integer occur = (Integer)getProperty(MappingNodeConstants.Properties.CARDINALITY_MIN_BOUND);
        if (occur != null) {
            return occur.intValue();
        }
        return 1;
    }
    
    public int getMaxOccurence() {
        Integer occur = (Integer)getProperty(MappingNodeConstants.Properties.CARDINALITY_MAX_BOUND);
        if (occur != null) {
            return occur.intValue();
        }
        return 1;
    }
                    
    /**
     * specify the element is a recursive root
     * @param root
     */
    void setRootRecursiveNode(boolean root, String recursionId) {
        setProperty(MappingNodeConstants.Properties.IS_RECURSIVE_ROOT, Boolean.valueOf(root));
        this.recursionId = recursionId;
    }
        
    public String getRecursionId() {
        return this.recursionId;
    }
    
    public boolean isRootRecursiveNode() {
        return Boolean.TRUE.equals(getProperty(MappingNodeConstants.Properties.IS_RECURSIVE_ROOT));        
    }
    
    /**
     * Get the document node of this node.
     * @return
     */
    public MappingDocument getDocument() {
        if (isDocumentNode()) {
            return (MappingDocument)this;
        }
        return getParentNode().getDocument();
    }
    
    public boolean isDocumentNode() {
        return false;
    }
    
    /**
     * A tag root node is the first visual node (Element to be specific) in the document tree
     * which is the root element in the output xml document. 
     * @return true if 
     */
    public boolean isTagRoot() {
        return false;
    }
    
    public List<String> getStagingTables() {
        return (List<String>)getProperty(MappingNodeConstants.Properties.TEMP_GROUP_NAMES);
    }
    
    public void setStagingTables(List<String> tables) {
        if (tables != null) {
            setProperty(MappingNodeConstants.Properties.TEMP_GROUP_NAMES, tables);
        }
        else {
            removeProperty(MappingNodeConstants.Properties.TEMP_GROUP_NAMES);
        }
    }
    
    public void addStagingTable(String tablename) {
        if (tablename == null) {
            return;
        }
        List<String> tables = getStagingTables();
        if (tables == null || tables.isEmpty()) {
            tables = new ArrayList<String>();
        }
        tables.add(tablename);
        setProperty(MappingNodeConstants.Properties.TEMP_GROUP_NAMES, tables);
    }    
    
}
