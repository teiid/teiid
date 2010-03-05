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

package org.teiid.connector.xmlsource.soap;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.fail;

import java.io.File;

import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.ConnectorLogger;
import org.teiid.connector.xmlsource.soap.SoapService.OperationNotFoundException;

import com.metamatrix.core.util.UnitTestUtil;

@Ignore
public class TestSoapConnector {
    
	@Test
    public void testNoWSDL() throws Exception {
        SoapManagedConnectionFactory env = Mockito.mock(SoapManagedConnectionFactory.class);
        Mockito.stub(env.getLogger()).toReturn(Mockito.mock(ConnectorLogger.class));

        try {
            SoapConnector c = new SoapConnector();
            c.initialize(env);
            fail("WSDL is not set; must have failed"); //$NON-NLS-1$
        } catch (ConnectorException e) {
            //pass
        }
    }

	@Test
    public void testWSDLLoad() throws Exception {
        File wsdlFile = new File(UnitTestUtil.getTestDataPath()+"/stockquotes.xml"); //$NON-NLS-1$

        SoapManagedConnectionFactory env = Mockito.mock(SoapManagedConnectionFactory.class);
        Mockito.stub(env.getLogger()).toReturn(Mockito.mock(ConnectorLogger.class));
        Mockito.stub(env.getWsdl()).toReturn(wsdlFile.toURL().toString());
        
        SoapConnector c = new SoapConnector();
        c.initialize(env);
        
        assertEquals("StockQuotes", c.getService().getServiceName().getLocalPart()); //$NON-NLS-1$
        assertTrue("Operation Not Found", (c.getService().findOperation("GetQuote")!=null)); //$NON-NLS-1$ //$NON-NLS-2$
        try {
        	c.getService().findOperation("GetQuoteX");
        	fail("Operation Should Not have Found"); //$NON-NLS-1$ //$NON-NLS-2$
        } catch(OperationNotFoundException e) {
        	
        }
    }
}
