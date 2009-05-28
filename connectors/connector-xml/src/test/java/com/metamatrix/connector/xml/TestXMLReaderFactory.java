package com.metamatrix.connector.xml;

import junit.framework.TestCase;

import org.xml.sax.XMLReader;

import com.metamatrix.cdk.api.EnvironmentUtility;
import com.metamatrix.cdk.api.SysLogger;
import com.metamatrix.connector.xml.base.ProxyObjectFactory;
import com.metamatrix.connector.xml.file.FileConnectorState;
import com.metamatrix.connector.xml.streaming.ReaderFactory;

public class TestXMLReaderFactory extends TestCase {

	XMLConnectorState state;
	
	@Override
	protected void setUp() throws Exception {
		super.setUp();
		state = new FileConnectorState();
    	state.setLogger(new SysLogger(false));
    	state.setState(EnvironmentUtility.createEnvironment(
    			ProxyObjectFactory.getDefaultFileProps()));
	}

	@Override
	protected void tearDown() throws Exception {
		super.tearDown();
	}

	public void testGetSAXBuilder() {
		try {
			XMLReader reader = ReaderFactory.getXMLReader(state);
			assertNotNull(reader);
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}

}
