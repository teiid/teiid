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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * This visitor class can be used to output a formatted text representation of the 
 * plan.
 */
public class TextOutputVisitor extends PlanVisitor {

    // Initialization state
    private DisplayHelper displayHelper;
    private int tabs = 0;
    
    // Processing state
    private Map nodeLevels = new HashMap();
    private StringBuffer text = new StringBuffer();
    
    /**
     * Construct the visitor.
     * @param displayHelper Helper to help format the display
     * @param initialTabs Number of tabs to put before every line
     */
    public TextOutputVisitor(DisplayHelper displayHelper, int initialTabs) {
        super();
        this.displayHelper = displayHelper;
        this.tabs = initialTabs;
    }

    /**
     * Return string representation of the plan
     * @return String representation of the plan
     */
    public String getText() {
        return this.text.toString();
    }

    /* 
     * @see com.metamatrix.jdbc.plan.PlanVisitor#visitNode(com.metamatrix.jdbc.plan.PlanNode)
     */
    protected void visitNode(PlanNode node) {
		    
        // Determine level and record in nodeLevels
        PlanNode parent = node.getParent();
        Integer nodeLevel; 
        if(parent == null) {
            nodeLevel = new Integer(0);
        } else {
            Integer parentLevel = (Integer) nodeLevels.get(parent);            
            nodeLevel = new Integer(parentLevel.intValue() + 1);
        }
        nodeLevels.put(node, nodeLevel);
        
        // Start line for this node with indentation
        int numTabs = nodeLevel.intValue() + this.tabs;
        for(int i=0; i<numTabs; i++) {
            text.append("  "); //$NON-NLS-1$
        }
        String name=displayHelper.getName(node);
        text.append(name);        
        text.append("\n"); //$NON-NLS-1$
        
        // Print properties appropriately
        int propTabs = numTabs + 1;
        List orderedProps = displayHelper.getOrderedProperties(node);
        for(int i=0; i< orderedProps.size(); i++) {
			String propName = (String) orderedProps.get(i);
            Object propObject=node.getProperties().get(propName);

            // Print leading spaces for prop name
        	for(int t=0; t<propTabs; t++) {
				text.append("  "); //$NON-NLS-1$
			}
            
            // Print prop name and value                       
			if (propObject instanceof Collection) {
                printCollectionValue(propTabs, propName, propObject);
            } else if(propObject instanceof PlanNode) {
                printNodeValue(numTabs, propName, propObject);                
			} else {     // something like String, Integer, Boolean, etc
                printScalarValue(node, propName);
			}
        }
    }

    private void printScalarValue(PlanNode node, String propName) {
        text.append("+ "); //$NON-NLS-1$
        text.append(displayHelper.getPropertyName(propName));
        text.append(": "); //$NON-NLS-1$
        text.append(node.getProperties().get(propName));
        text.append("\n");                 //$NON-NLS-1$
    }

    private void printNodeValue(int numTabs, String propName, Object propObject) {
        text.append("+ "); //$NON-NLS-1$
        text.append(displayHelper.getPropertyName(propName));
        text.append(":\n"); //$NON-NLS-1$ 
        
        TextOutputVisitor nestedVisitor = new TextOutputVisitor(this.displayHelper, numTabs+2);
        PlanNode nestedNode = (PlanNode) propObject;
        nestedVisitor.visit(nestedNode);
        text.append(nestedVisitor.getText());
    }

    private void printCollectionValue(int propTabs, String propName, Object propObject) {
        //Get the prop name from the key value
        text.append("+ "); //$NON-NLS-1$
        text.append(displayHelper.getPropertyName(propName));
        text.append(":\n"); //$NON-NLS-1$ 
        // Visit collection property
        Collection collection = (Collection) propObject;
        Iterator collectionIter = collection.iterator();
        for(int v=1; collectionIter.hasNext(); v++) {
        	Object collectionValue = collectionIter.next();
        	for(int t=0; t<propTabs+2; t++) {
        		text.append("  "); //$NON-NLS-1$
        	}
            text.append(v);
            text.append(": "); //$NON-NLS-1$
        	text.append(collectionValue);
            text.append("\n"); //$NON-NLS-1$
        }
    }

    /* 
     * @see com.metamatrix.jdbc.plan.PlanVisitor#visitPropertyValue(java.lang.String, java.lang.Object)
     */
    protected void visitPropertyValue(PlanNode node, String propertyName, Object propertyValue) {

    }

    /* 
     * @see com.metamatrix.jdbc.plan.PlanVisitor#visitContainerProperty(java.lang.String, java.util.Collection)
     */
    protected void visitContainerProperty(PlanNode node, String propertyName, Collection propertyValue) {

    }

}
