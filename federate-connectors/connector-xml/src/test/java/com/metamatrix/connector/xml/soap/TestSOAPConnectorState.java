package com.metamatrix.connector.xml.soap;

import com.metamatrix.connector.xml.SOAPConnectorState;
import com.metamatrix.connector.xml.base.ProxyObjectFactory;

import junit.framework.TestCase;

public class TestSOAPConnectorState extends TestCase {

	SOAPConnectorState baseState;
	
	@Override
	protected void setUp() throws Exception {
		super.setUp();
		baseState = new SOAPConnectorStateImpl();
		baseState.setState(ProxyObjectFactory.createSOAPStateHTTPBasic());
	}

	public void testIsEncoded() {
		assertTrue(baseState.isEncoded());
	}

	public void testIsRPC() {
		assertTrue(baseState.isRPC());
	}

	public void testIsExceptionOnFault() {
		assertTrue(baseState.isExceptionOnFault());
	}
}
