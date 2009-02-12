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

package com.metamatrix.jdbc.api;

import java.util.Collection;
import java.util.LinkedList;

/**
 * A helper class to walk a query plan and execute some action at each node.
 * Subclasses should override the appropriate visit() methods to get 
 * notification when a node of each type is visited.  
 */
public abstract class PlanVisitor {

    /**
     * Construct the visitor with a reference to the root node.
     */
    public PlanVisitor() {
    }
    
    
    /** 
     * <p>This method controls how a tree is visited from the rootNode.  This implementation
     * will walk the tree in pre-order (a node is visited before it's children) and will
     * visit a particular node in the following order: node itself, each property of the 
     * node, and finally the children of the node, in order.</p>
     * 
     * <p>Subclasses may override this method to manipulate the visitor order if desired.</p>
     * 
     * @param rootNode The rootNode of the tree
     */
    public void visit(PlanNode rootNode) {
        LinkedList nodeStack = new LinkedList();
        nodeStack.add(rootNode);
        
        while(! nodeStack.isEmpty()) {
            // Obtain next node
            PlanNode node = (PlanNode) nodeStack.removeFirst();
            
            // Visit node
            visitNode(node);
                        
            // Add children to stack
            nodeStack.addAll(node.getChildren());
        }
    }   

    /**
     * Visit a node.
     * @param node The node being visited
     */
    protected abstract void visitNode(PlanNode node);
    
    /**
     * Visit a property value.
     * @param node The node being visited
     * @param propertyName The property name being visited
     * @param propertyValue The property value being visited
     */
    protected abstract void visitPropertyValue(PlanNode node, String propertyName, Object propertyValue);
    
    /**
     * Visit a container property - the container property will be visited, then
     * each particular value of the container will be visited.
     * @param node The node being visited
     * @param propertyName The property name being visited
     * @param propertyValue The collection value being visited
     */
    protected abstract void visitContainerProperty(PlanNode node, String propertyName, Collection propertyValue);
        
}
