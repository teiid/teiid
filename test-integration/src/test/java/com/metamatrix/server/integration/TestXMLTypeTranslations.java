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

package com.metamatrix.server.integration;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.teiid.connector.jdbc.oracle.OracleCapabilities;
import org.teiid.dqp.internal.datamgr.CapabilitiesConverter;

import com.metamatrix.core.util.UnitTestUtil;
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
                            TimestampUtil.createTimestamp(3, 3, 4, 5, 6, 10, 200), 
                            new Double(Double.NEGATIVE_INFINITY), 
                            new Float(Float.POSITIVE_INFINITY), 
                            new BigInteger("100"), //$NON-NLS-1$
                            new BigInteger("100"), //$NON-NLS-1$
                            TimestampUtil.createTimestamp(3, 3, 4, 5, 6, 7, 0), 
                            new BigInteger("100"), //$NON-NLS-1$
                            TimestampUtil.createTimestamp(3, 3, 4, 5, 6, 7, 0),
                            "1" //$NON-NLS-1$
                                                   })});
        
        
        List[] expected = new List[] { Arrays.asList(new Object[] {"<?xml version=\"1.0\" encoding=\"UTF-8\"?><XSDTypesNS:test xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\" xmlns:XSDTypesNS=\"http://www.metamatrix.com/XMLSchema/DataSets/XSDTypes\"><book><datetime>1903-04-04T05:06:10.0000002</datetime><double>-INF</double><float>INF</float><gday>---100</gday><gmonth>--100</gmonth><gmonthday>--04-04</gmonthday><gyear>0100</gyear><gyearmonth>1903-04</gyearmonth><string>1</string></book></XSDTypesNS:test>" })};                     //$NON-NLS-1$
        doProcess(plan, dataMgr , expected, DEBUG);
        
    }
    

}
