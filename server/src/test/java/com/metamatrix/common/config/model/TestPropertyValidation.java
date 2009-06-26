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

package com.metamatrix.common.config.model;

import junit.framework.TestCase;

import com.metamatrix.common.config.api.exceptions.ConfigurationException;

public class TestPropertyValidation extends TestCase {

    /**
     * @param name
     */
    public TestPropertyValidation(String name) {
        super(name);

    }

    // ################################## TESTS ################################

    public void testBoundariesMultiCastPort() throws Exception{
        PropertyValidations val = new PropertyValidations();
        val.isPropertyValid(PropertyValidations.UDP_MCAST_ADDR_PROPERTY, "224.0.0.0" ); //$NON-NLS-1$

        val.isPropertyValid(PropertyValidations.UDP_MCAST_ADDR_PROPERTY, "224.255.255.255" ); //$NON-NLS-1$

    }
    
    public void testInvalidAlphaMultiCastPort() throws Exception{
        try {
              PropertyValidations val = new PropertyValidations();
              val.isPropertyValid(PropertyValidations.UDP_MCAST_ADDR_PROPERTY, "224.a.c.d" ); //$NON-NLS-1$
              fail("224.a.c.d multicast port has alpha characters and is invalid"); //$NON-NLS-1$
        } catch (ConfigurationException ce) {
             
        }
    }    
    
    public void testMissingNodesMultiCastPort() throws Exception{
        try {
              PropertyValidations val = new PropertyValidations();
              val.isPropertyValid(PropertyValidations.UDP_MCAST_ADDR_PROPERTY, "224.10." ); //$NON-NLS-1$
              fail("224.10. multicast port is missing nodes and is invalid"); //$NON-NLS-1$
        } catch (ConfigurationException ce) {
            
        }
    }     

}
