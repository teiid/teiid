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

package org.teiid.client.plan;

import java.util.*;

public class PlanNode {

	//TODO: consolidate these constants with Describable
	public static final String OUTPUT_COLS = "outputCols"; //$NON-NLS-1$
	public static final String TYPE = "type"; //$NON-NLS-1$
	static final String PROP_CHILDREN = "children"; //$NON-NLS-1$
    
    private Map<String, Object> props;
    private PlanNode parent;
    private List<PlanNode> children = new ArrayList<PlanNode>();
	
    PlanNode(Map<String, Object> props) {
        this.props = props;
    }

    void setParent(PlanNode parent) {
        this.parent = parent;
    }

    public PlanNode getParent() {
        return this.parent;
    }

    void addChild(PlanNode child) {
        children.add(child); 
    }

    public List<PlanNode> getChildren() {
        return this.children;
    }

    public Map<String, Object> getProperties() {
        return this.props;
    }
    
    public static PlanNode constructFromMap(Map properties) {
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
        PlanNode node = new PlanNode(copy); 

        // Then construct children and connect
        if(childMaps != null) {
            for(int i=0; i<childMaps.size(); i++) {
                Map childMap = (Map) childMaps.get(i);
                PlanNode child = constructFromMap(childMap);
                child.setParent(node);
                node.addChild(child);
            }
        }
        
        // And return
        return node;
    }

}
