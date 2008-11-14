/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

import java.net.URLEncoder;

import junit.framework.TestCase;


/** 
 * @since 4.3
 */
public class TestMMServerConnection extends TestCase {

    
    public void testGetServerURL_NoProperties() {        
        String result = MMServerConnection.getServerURL("jdbc:metamatrix:designtimecatalog@mm://slwxp172:44401;user=ddifranco;password=mm"); //$NON-NLS-1$
        assertEquals("mm://slwxp172:44401", result);         //$NON-NLS-1$
    }

    public void testGetServerURL_Properties() {        
        String result = MMServerConnection.getServerURL("jdbc:metamatrix:designtimecatalog@mm://slwxp172:44401;user=ddifranco;password=mm"); //$NON-NLS-1$
        assertEquals("mm://slwxp172:44401", result);         //$NON-NLS-1$
    }
    
    /**
     * Test getServerURL with a valid URL and password that contains at least 
     * one ASCII character in the range of 32 to 126 excluding the ; and = sign.
     *
     * @since 5.0.2
     */
    public void testGetServerURL_PasswordProperties() throws Exception {        
        String result = null;
        String srcURL = "jdbc:metamatrix:designtimecatalog@mm://slwxp172:44401;user=ddifranco;password="; //$NON-NLS-1$
        String password = null;
        String tgtURL = "mm://slwxp172:44401"; //$NON-NLS-1$
        

        for ( char ch = 32; ch <= 126; ch++ ) {
            //exclude URL reserved characters
        	if ( ch != ';' && ch != '=' && ch != '%') {
        		password = ch+"mm"; //$NON-NLS-1$
        		result = MMServerConnection.getServerURL(srcURL+URLEncoder.encode(password, "UTF-8")); //$NON-NLS-1$ 
        		assertEquals("Failed to obtain correct ServerURL when using password "+password,tgtURL, result);         //$NON-NLS-1$
        	}
        }
        	
    }
    
    public void testGetServerURL_2Servers() {       
        String result = MMServerConnection.getServerURL("jdbc:metamatrix:designtimecatalog@mm://slwxp172:44401,slabc123:12345;user=ddifranco;password=mm"); //$NON-NLS-1$
        assertEquals("mm://slwxp172:44401,slabc123:12345", result);         //$NON-NLS-1$
    }

}
