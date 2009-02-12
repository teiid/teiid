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

import com.metamatrix.core.util.StringUtil;

/**
 * This visitor class can be used to output an XML representation of the plan.
 */
public class XMLOutputVisitor {

    // Initialization state
    private DisplayHelper displayHelper;
    private int tabs = 0;
    
    // Processing state
    private Map nodeLevels = new HashMap();
    private StringBuffer text;

    public XMLOutputVisitor(DisplayHelper displayHelper) {
        this(displayHelper, 0, true);
    }
    
    public XMLOutputVisitor(DisplayHelper displayHelper, int initialTabs, boolean printXMLHeader) {
        this.displayHelper = displayHelper;
        this.tabs = initialTabs;

        text = new StringBuffer();
        if(printXMLHeader) {
            text.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"); //$NON-NLS-1$
        }
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
    public void visit(PlanNode node) {
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
        text.append(getTab(numTabs));

        String name=displayHelper.getName(node);
        text.append("<node name=\""); //$NON-NLS-1$
        text.append(escapeForXML(name));        
        text.append("\">\n"); //$NON-NLS-1$
        
        // Print properties appropriately
        int propTabs = numTabs + 2;
        List orderedProps = displayHelper.getOrderedProperties(node);
        if(orderedProps.size() > 0) {
            text.append(getTab(numTabs+1));
            text.append("<properties>\n"); //$NON-NLS-1$
            for(int i=0; i< orderedProps.size(); i++) {
                String propName = (String) orderedProps.get(i);
                Object propObject=node.getProperties().get(propName);
    
                // Print leading spaces for prop name
                text.append(getTab(propTabs));
                
                // Print prop name and value                       
                if (propObject instanceof Collection) {
                    printCollectionValue(propTabs, propName, propObject);
                } else if(propObject instanceof PlanNode) {
                    printNodeValue(propTabs, propName, propObject);                
                } else {     // something like String, Integer, Boolean, etc
                    printScalarValue(node, propName);
                }
            }
            text.append(getTab(numTabs+1));
            text.append("</properties>\n"); //$NON-NLS-1$
        }
        
        // Visit children
        List children = node.getChildren();
        Iterator childIter = children.iterator();
        while(childIter.hasNext()) {
            PlanNode child = (PlanNode) childIter.next();
            visit(child);
        }
        
        text.append(getTab(numTabs));
        text.append("</node>\n"); //$NON-NLS-1$        
    }

    private void printScalarValue(PlanNode node, String propName) {
        text.append("<property name=\""); //$NON-NLS-1$
        text.append(escapeForXML(displayHelper.getPropertyName(propName)));
        text.append("\" value=\""); //$NON-NLS-1$
        text.append(escapeForXML(node.getProperties().get(propName)));
        text.append("\"/>\n");                 //$NON-NLS-1$
    }

    private void printNodeValue(int numTabs, String propName, Object propObject) {
        text.append("<property name=\""); //$NON-NLS-1$
        text.append(escapeForXML(displayHelper.getPropertyName(propName)));
        text.append("\">\n"); //$NON-NLS-1$
        
        XMLOutputVisitor nestedVisitor = new XMLOutputVisitor(this.displayHelper, numTabs+1, false);
        PlanNode nestedNode = (PlanNode) propObject;
        nestedVisitor.visit(nestedNode);
        text.append(nestedVisitor.getText());
        
        text.append(getTab(numTabs));                
        text.append("</property>\n"); //$NON-NLS-1$
    }

    private void printCollectionValue(int propTabs, String propName, Object propObject) {
        text.append("<property name=\""); //$NON-NLS-1$
        text.append(escapeForXML(displayHelper.getPropertyName(propName)));
        text.append("\">\n"); //$NON-NLS-1$
        
        text.append(getTab(propTabs+1));
        text.append("<collection>\n"); //$NON-NLS-1$
        
        // Visit collection property
        Collection collection = (Collection) propObject;
        Iterator collectionIter = collection.iterator();
        for(int v=1; collectionIter.hasNext(); v++) {
            Object collectionValue = collectionIter.next();
                        
            text.append(getTab(propTabs+2));
            text.append("<value>"); //$NON-NLS-1$
            text.append(escapeForXML(collectionValue));
            text.append("</value>\n"); //$NON-NLS-1$
        }
        
        text.append(getTab(propTabs+1));
        text.append("</collection>\n"); //$NON-NLS-1$

        text.append(getTab(propTabs));
        text.append("</property>\n");        //$NON-NLS-1$
    }

    private Map tabCache = new HashMap();
    private String getTab(int tab) {
        Integer tabKey = new Integer(tab);
        String tabStr = (String) tabCache.get(tabKey);
        if(tabStr != null) {
            return tabStr;        
        }
        StringBuffer tabBuffer = new StringBuffer();
        for(int t=0; t<tab; t++) {
            tabBuffer.append("  "); //$NON-NLS-1$
        }    
        tabStr = tabBuffer.toString();
        tabCache.put(tabKey, tabStr);        
        return tabStr;
    }

    private String escapeForXML(Object obj) {
        if(obj == null) {
            return "null"; //$NON-NLS-1$
        }
        
        String str = obj.toString();
        if(str.indexOf("<") >= 0 || str.indexOf(">") >= 0 || str.indexOf("\"") >= 0) { //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
            StringBuffer newStr = new StringBuffer(str);
            StringUtil.replaceAll(newStr, "<", "&lt;"); //$NON-NLS-1$ //$NON-NLS-2$
            StringUtil.replaceAll(newStr, ">", "&gt;"); //$NON-NLS-1$ //$NON-NLS-2$
            StringUtil.replaceAll(newStr, "\"", "&quot;"); //$NON-NLS-1$ //$NON-NLS-2$
            return newStr.toString();
        }
        return str;
    }
    
    /**
     * Helper method to perform a conversion using the {@link DefaultDisplayHelper}.
     * @param node The root of the plan tree to convert
     * @return The plan tree converted to XML
     */
    public static String convertToXML(PlanNode node) {
        XMLOutputVisitor visitor = new XMLOutputVisitor(new DefaultDisplayHelper());
        visitor.visit(node);
        return visitor.getText();
    }
}
