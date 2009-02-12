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
import java.util.List;

import com.metamatrix.jdbc.api.DisplayHelper;
import com.metamatrix.jdbc.api.PlanNode;

/**
 */
public class FakeDisplayHelper implements DisplayHelper {

    /**
     * 
     */
    public FakeDisplayHelper() {
        super();
    }

    /* 
     * @see com.metamatrix.jdbc.plan.DisplayHelper#getName(com.metamatrix.jdbc.plan.PlanNode)
     */
    public String getName(PlanNode node) {        
        return (String) node.getProperties().get(FakePlanNode.PROP_TYPE);
    }

    /* 
     * @see com.metamatrix.jdbc.plan.DisplayHelper#getDescription(com.metamatrix.jdbc.plan.PlanNode)
     */
    public String getDescription(PlanNode node) {
        return (String) node.getProperties().get(FakePlanNode.PROP_DESCRIPTION);
    }

    /* 
     * @see com.metamatrix.jdbc.plan.DisplayHelper#getOrderedProperties(com.metamatrix.jdbc.plan.PlanNode)
     */
    public List getOrderedProperties(PlanNode node) {
        List props = new ArrayList(node.getProperties().keySet());
        props.remove(FakePlanNode.PROP_TYPE);
        props.remove(FakePlanNode.PROP_DESCRIPTION);
        if(props.contains(FakePlanNode.PROP_OUTPUT_COLS)) {
            props.remove(FakePlanNode.PROP_OUTPUT_COLS);
            props.add(0, FakePlanNode.PROP_OUTPUT_COLS);            
        }
        return props;
    }

    /* 
     * @see com.metamatrix.jdbc.plan.DisplayHelper#getPropertyName(java.lang.String)
     */
    public String getPropertyName(String property) {
        if(property.equals(FakePlanNode.PROP_OUTPUT_COLS)) {
            return "Output columns"; //$NON-NLS-1$
        }
        return property;
    }

    /* 
     * @see com.metamatrix.jdbc.plan.DisplayHelper#setMaxDescriptionLength(int)
     */
    public void setMaxDescriptionLength(int length) {
        // ignore
    }

}
