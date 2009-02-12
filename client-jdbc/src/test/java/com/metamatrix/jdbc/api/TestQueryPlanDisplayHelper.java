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

import java.util.Arrays;
import java.util.Map;

import junit.framework.TestCase;

import com.metamatrix.jdbc.api.tools.QueryPlanDisplayHelper;

/**
 */
public class TestQueryPlanDisplayHelper extends TestCase {

    public TestQueryPlanDisplayHelper(String name) {
        super(name);
    }

    private QueryPlanDisplayHelper loadHelper() throws Exception {
        QueryPlanDisplayHelper helper = new QueryPlanDisplayHelper();
        
        return helper;
    }
    
    public void testGetNameWNullType() throws Exception{

        FakePlanNode plan = (FakePlanNode)TestTextOutputVisitor.example2();
        
        Map nodeProps = plan.getProperties();
        /*
         * test with a null plan type.  This is a test to combat defect # 18009
         */
        nodeProps.put("type",null); //$NON-NLS-1$
        
        QueryPlanDisplayHelper helper = loadHelper();
        
        String name = helper.getName(plan);
        assertEquals("Node", name); //$NON-NLS-1$
    }
    

    public void testGetDescriptionWNullType() throws Exception{

        FakePlanNode plan = (FakePlanNode)TestTextOutputVisitor.example2();
        
        Map nodeProps = plan.getProperties();
        /*
         * test with a null plan type.  This is a test to combat defect # 18009
         */
        nodeProps.put("type",null); //$NON-NLS-1$
        
        QueryPlanDisplayHelper helper = loadHelper();
        
        String desc = helper.getDescription(plan);
        assertEquals("", desc); //$NON-NLS-1$
    }
     
    public void testGetNameNullDescription() throws Exception {
        QueryPlanDisplayHelper helper = loadHelper();
        
        FakePlanNode node = new FakePlanNode("Access", null); //$NON-NLS-1$
        node.setProperty("sql", "SELECT A, B, C FROM MYTABLE"); //$NON-NLS-1$ //$NON-NLS-2$
        assertEquals("Access [SELECT A, B, C FROM MYTABLE]", helper.getName(node)); //$NON-NLS-1$
        
        node = new FakePlanNode("Join", null); //$NON-NLS-1$
        node.setProperty("joinType", "LEFT OUTER JOIN"); //$NON-NLS-1$ //$NON-NLS-2$
        node.setProperty("joinCriteria", Arrays.asList(new String[] {"A.B = B.B"})); //$NON-NLS-1$ //$NON-NLS-2$
        assertEquals("Join [LEFT OUTER JOIN ON A.B = B.B]", helper.getName(node)); //$NON-NLS-1$
        
        node = new FakePlanNode("Project", null); //$NON-NLS-1$
        node.setProperty("selectCols", Arrays.asList(new String[] {"a", "b", "c"})); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
        assertEquals("Project [a, b, c]", helper.getName(node)); //$NON-NLS-1$
    }
    
    public void testGetName() throws Exception {
        QueryPlanDisplayHelper helper = loadHelper();
        
        FakePlanNode node = new FakePlanNode("Access", "Access Node Description"); //$NON-NLS-1$ //$NON-NLS-2$
        node.setProperty("sql", "SELECT A, B, C FROM MYTABLE"); //$NON-NLS-1$ //$NON-NLS-2$
        assertEquals("Access [Access Node Description]", helper.getName(node)); //$NON-NLS-1$
    }
    
}
