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

package com.metamatrix.jdbc;

import java.util.*;
import java.util.List;
import java.util.Map;

import com.metamatrix.jdbc.api.PlanNode;

/**
 * Implements the plan node
 */
class PlanNodeImpl implements PlanNode {

    private Map props;
    private PlanNode parent;
    private List children = new ArrayList();

    /**
     * 
     */
    PlanNodeImpl(Map props) {
        this.props = props;
    }

    void setParent(PlanNode parent) {
        this.parent = parent;
    }

    /* 
     * @see com.metamatrix.jdbc.plan.PlanNode#getParent()
     */
    public PlanNode getParent() {
        return this.parent;
    }

    void addChild(PlanNode child) {
        children.add(child); 
    }

    /* 
     * @see com.metamatrix.jdbc.plan.PlanNode#getChildren()
     */
    public List getChildren() {
        return this.children;
    }

    /* 
     * @see com.metamatrix.jdbc.plan.PlanNode#getProperties()
     */
    public Map getProperties() {
        return this.props;
    }
    
    private static final String PROP_CHILDREN = "children"; //$NON-NLS-1$
    
    static PlanNode constructFromMap(Map properties) {
        // Construct node without child property
        Map copy = new HashMap(properties);
        List childMaps = (List) copy.remove(PROP_CHILDREN);
        
        // Convert any subplans to PlanNodes as well
        Iterator keyIter = copy.keySet().iterator();
        while(keyIter.hasNext()) {
            Object key = keyIter.next();
            Object value = copy.get(key);
            if(value instanceof Map) {
                copy.put(key, constructFromMap((Map)value));
            }
        }

        // Construct this node                
        PlanNodeImpl node = new PlanNodeImpl(copy); 

        // Then construct children and connect
        if(childMaps != null) {
            for(int i=0; i<childMaps.size(); i++) {
                Map childMap = (Map) childMaps.get(i);
                PlanNodeImpl child = (PlanNodeImpl) constructFromMap(childMap);
                child.setParent(node);
                node.addChild(child);
            }
        }
        
        // And return
        return node;
    }

}
