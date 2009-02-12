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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.metamatrix.jdbc.api.PlanNode;

/**
 */
public class FakePlanNode implements PlanNode {

    public static final String PROP_TYPE = "type"; //$NON-NLS-1$
    public static final String PROP_DESCRIPTION = "desc"; //$NON-NLS-1$
    public static final String PROP_OUTPUT_COLS = "outputCols"; //$NON-NLS-1$

    private PlanNode parent;
    private Map props = new HashMap();
    private List children = new ArrayList();

    /**
     * 
     */
    public FakePlanNode(String type, String description) {
        super();
        this.props.put(PROP_TYPE, type);
        this.props.put(PROP_DESCRIPTION, description);
    }

    public void setParent(PlanNode parent) { 
        this.parent = parent;
    }

    /* 
     * @see com.metamatrix.jdbc.plan.PlanNode#getParent()
     */
    public PlanNode getParent() {
        return this.parent;
    }

    public void addChild(PlanNode child) {
        this.children.add(child);
    }

    /* 
     * @see com.metamatrix.jdbc.plan.PlanNode#getChildren()
     */
    public List getChildren() {
        return this.children;
    }

    public void setProperty(String prop, Object value) {
        this.props.put(prop, value);
    }

    /* 
     * @see com.metamatrix.jdbc.plan.PlanNode#getProperties()
     */
    public Map getProperties() {
        return this.props;
    }

}
