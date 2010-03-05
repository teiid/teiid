package org.teiid.connector.xml.soap;

import org.teiid.connector.xml.SOAPConnectorState;

import junit.framework.TestCase;


public class TestSOAPConnectorState extends TestCase {

	SOAPConnectorState baseState;
	
	@Override
	protected void setUp() throws Exception {
		super.setUp();
		baseState = new SOAPConnectorStateImpl();
		baseState.setState(FakeSoapmanagedFactory.createSOAPStateHTTPBasic());
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
