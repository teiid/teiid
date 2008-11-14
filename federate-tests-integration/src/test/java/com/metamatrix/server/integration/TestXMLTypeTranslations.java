/*
 * Copyright � 2000-2005 MetaMatrix, Inc.  All rights reserved.
 */
package com.metamatrix.server.integration;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.metamatrix.connector.jdbc.oracle.OracleCapabilities;
import com.metamatrix.core.util.UnitTestUtil;
import com.metamatrix.dqp.internal.datamgr.CapabilitiesConverter;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.optimizer.capabilities.FakeCapabilitiesFinder;
import com.metamatrix.query.processor.HardcodedDataManager;
import com.metamatrix.query.processor.ProcessorPlan;
import com.metamatrix.query.unittest.TimestampUtil;

public class TestXMLTypeTranslations extends BaseQueryTest {

    private static final boolean DEBUG = false;

    public TestXMLTypeTranslations(String name) {
        super(name);
    }
        
    //NOTE that the gMonth and gDay values are invalid (but properly formatted)
    public void testXSDTranslations() throws Exception {
        TimestampUtil tsutil = new TimestampUtil();
        
        FakeCapabilitiesFinder finder = new FakeCapabilitiesFinder();
        finder.addCapabilities("sample", CapabilitiesConverter.convertCapabilities(new OracleCapabilities())); //$NON-NLS-1$
        
        QueryMetadataInterface metadata = createMetadata(UnitTestUtil.getTestDataPath()+"/xmltypes/test.vdb"); //$NON-NLS-1$
        
        String sql = "select * from testdocument.testDocument";  //$NON-NLS-1$

        
        ProcessorPlan plan = createPlan(metadata,  
                 sql, 
                 finder, DEBUG);

        HardcodedDataManager dataMgr = new HardcodedDataManager();
        
        Set models = new HashSet();
        models.add("sample"); //$NON-NLS-1$
        dataMgr.setValidModels(models);
        
        dataMgr.addData("SELECT g_0.\"timestamp\", g_0.\"double\", g_0.\"float\", convert(g_0.\"double\", biginteger), convert(g_0.\"double\", biginteger), convert(g_0.\"date\", timestamp), convert(g_0.\"double\", biginteger), convert(g_0.\"date\", timestamp), '1' FROM sample.RUNTIMEVALUE AS g_0", //$NON-NLS-1$ 
                        
                        new List[] { Arrays.asList(new Object[] { 
                            tsutil.createTimestamp(3, 3, 4, 5, 6, 10, 200), 
                            new Double(Double.NEGATIVE_INFINITY), 
                            new Float(Float.POSITIVE_INFINITY), 
                            new BigInteger("100"), //$NON-NLS-1$
                            new BigInteger("100"), //$NON-NLS-1$
                            tsutil.createTimestamp(3, 3, 4, 5, 6, 7, 0), 
                            new BigInteger("100"), //$NON-NLS-1$
                            tsutil.createTimestamp(3, 3, 4, 5, 6, 7, 0),
                            "1" //$NON-NLS-1$
                                                   })});
        
        
        List[] expected = new List[] { Arrays.asList(new Object[] {"<?xml version=\"1.0\" encoding=\"UTF-8\"?><XSDTypesNS:test xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\" xmlns:XSDTypesNS=\"http://www.metamatrix.com/XMLSchema/DataSets/XSDTypes\"><book><datetime>1903-04-04T05:06:10.0000002</datetime><double>-INF</double><float>INF</float><gday>---100</gday><gmonth>--100</gmonth><gmonthday>--04-04</gmonthday><gyear>0100</gyear><gyearmonth>1903-04</gyearmonth><string>1</string></book></XSDTypesNS:test>" })};                     //$NON-NLS-1$
        doProcess(plan, dataMgr , expected, DEBUG);
        
    }
    

}
