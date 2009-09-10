package org.teiid.connector.metadata.runtime;

import org.junit.Test;

import com.metamatrix.core.util.UnitTestUtil;
import com.metamatrix.jdbc.api.AbstractMMQueryTestCase;

public class TestMetadataInConnector extends AbstractMMQueryTestCase {
	
    private static final String DQP_PROP_FILE = UnitTestUtil.getTestDataPath() + "/metadata/dqp.properties;user=test"; //$NON-NLS-1$
    private static final String VDB = "TestExtensions"; //$NON-NLS-1$

 		
    @Test public void testMetadataTable() throws Exception {
    	getConnection(VDB, DQP_PROP_FILE, ""); //$NON-NLS-1$
    	execute("Select * from TableA"); //$NON-NLS-1$
    	String expected[] = {"column1[string]    column2[integer]"}; //$NON-NLS-1$
    	assertResults(expected); 
    	closeConnection();
    }
    
    @Test public void testMetadataProcedure() throws Exception {
    	getConnection(VDB, DQP_PROP_FILE, ""); //$NON-NLS-1$
    	execute("exec AnyModel.ProcedureB(?)", new Object[] {"foo"}); //$NON-NLS-1$ //$NON-NLS-2$
    	String expected[] = {"column1[string]    column2[integer]"}; //$NON-NLS-1$
    	assertResults(expected);
    	closeConnection();
    }	
    
}
