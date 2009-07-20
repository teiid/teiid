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

package org.teiid.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.DriverPropertyInfo;

import org.junit.Test;

public class TestTeiidDriver {
    TeiidDriver drv = new TeiidDriver();

    
    @Test public void testGetPropertyInfo1() throws Exception {        
        DriverPropertyInfo info[] = drv.getPropertyInfo("jdbc:teiid:vdb@mm://localhost:12345", null); //$NON-NLS-1$
        assertEquals(20, info.length);
    }
    
    @Test public void testAccepts() throws Exception {
    	assertTrue(drv.acceptsURL("jdbc:teiid:vdb@mm://localhost:12345")); //$NON-NLS-1$
    	assertTrue(drv.acceptsURL("jdbc:teiid:vdb@mm://localhost:12345;user=foo;password=bar")); //$NON-NLS-1$
    	assertTrue(drv.acceptsURL("jdbc:teiid:vdb")); //$NON-NLS-1$
    	assertTrue(drv.acceptsURL("jdbc:teiid:vdb@/foo/blah/deploy.properties")); //$NON-NLS-1$
    	
    	assertTrue(drv.acceptsURL("jdbc:metamatrix:vdb@mm://localhost:12345")); //$NON-NLS-1$
    	assertTrue(drv.acceptsURL("jdbc:metamatrix:vdb@mm://localhost:12345;user=foo;password=bar")); //$NON-NLS-1$
    	assertTrue(drv.acceptsURL("jdbc:metamatrix:vdb")); //$NON-NLS-1$
    	assertTrue(drv.acceptsURL("jdbc:metamatrix:vdb@/foo/blah/deploy.properties")); //$NON-NLS-1$
    	
    }
    
}
