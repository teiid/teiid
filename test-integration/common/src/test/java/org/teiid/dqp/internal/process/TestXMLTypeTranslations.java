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

package org.teiid.dqp.internal.process;

import java.io.FileInputStream;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.mockito.Mockito;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.dqp.message.RequestID;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.optimizer.capabilities.BasicSourceCapabilities;
import org.teiid.query.optimizer.capabilities.FakeCapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.SourceCapabilities.Capability;
import org.teiid.query.processor.HardcodedDataManager;
import org.teiid.translator.SourceSystemFunctions;


public class TestXMLTypeTranslations extends BaseQueryTest {

    private static final boolean DEBUG = false;

    public TestXMLTypeTranslations(String name) {
        super(name);
    }
        
    //NOTE that the gMonth and gDay values are invalid (but properly formatted)
    public void testXSDTranslations() throws Exception {
        FakeCapabilitiesFinder finder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities bsc = new BasicSourceCapabilities();
        bsc.setFunctionSupport(SourceSystemFunctions.CONVERT, true);
        bsc.setCapabilitySupport(Capability.QUERY_SELECT_EXPRESSION, true);
        bsc.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        finder.addCapabilities("sample", bsc); //$NON-NLS-1$
        
        QueryMetadataInterface metadata = createMetadata(UnitTestUtil.getTestDataPath()+"/test.vdb"); //$NON-NLS-1$
        
        String sql = "select * from testdocument.testDocument";  //$NON-NLS-1$

        HardcodedDataManager dataMgr = new HardcodedDataManager();
        
        Set<String> models = new HashSet<String>();
        models.add("sample"); //$NON-NLS-1$
        dataMgr.setValidModels(models);
        
        Timestamp ts = new Timestamp(-2106305630000l);
        ts.setNanos(3000000);
        
        dataMgr.addData("SELECT g_0.\"timestamp\", g_0.\"double\", g_0.\"float\", convert(g_0.\"double\", biginteger), convert(g_0.\"date\", timestamp) FROM sample.RUNTIMEVALUE AS g_0", //$NON-NLS-1$ 
                        
                        new List[] { Arrays.asList(new Object[] { 
                            ts, 
                            new Double(Double.NEGATIVE_INFINITY), 
                            new Float(Float.POSITIVE_INFINITY), 
                            new BigInteger("100"), //$NON-NLS-1$
                            ts, 
                                                   })});
        
        
        List<?>[] expected = new List[] { Arrays.asList(new Object[] {"<?xml version=\"1.0\" encoding=\"UTF-8\"?><XSDTypesNS:test xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\" xmlns:XSDTypesNS=\"http://www.metamatrix.com/XMLSchema/DataSets/XSDTypes\"><book><datetime>1903-04-04T11:06:10.006Z</datetime><double>-INF</double><float>INF</float><gday>---100</gday><gmonth>--100</gmonth><gmonthday>--04-04</gmonthday><gyear>0100</gyear><gyearmonth>1903-04Z</gyearmonth><string>1</string></book></XSDTypesNS:test>" })};                     //$NON-NLS-1$
        doProcess(metadata,  
                sql, 
                finder, dataMgr , expected, DEBUG);
        
    }
    
    public void testGetXmlSchemas() throws Exception {
        FakeCapabilitiesFinder finder = new FakeCapabilitiesFinder();
        finder.addCapabilities("sample", new BasicSourceCapabilities()); //$NON-NLS-1$
        
        TransformationMetadata metadata = createMetadata(UnitTestUtil.getTestDataPath()+"/test.vdb"); //$NON-NLS-1$
        
        String sql = "call getXMLSchemas('testdocument.testDocument')";  //$NON-NLS-1$

        DQPCore core = Mockito.mock(DQPCore.class);
        RequestWorkItem rwi = Mockito.mock(RequestWorkItem.class);
        DQPWorkContext workContext = new DQPWorkContext();
        VDBMetaData vdb = new VDBMetaData();
        vdb.addAttchment(TransformationMetadata.class, metadata);
        workContext.getSession().setVdb(vdb);
        Mockito.stub(rwi.getDqpWorkContext()).toReturn(workContext);
        
        Mockito.stub(core.getRequestWorkItem((RequestID)Mockito.anyObject())).toReturn(rwi);
        DataTierManagerImpl dataMgr = new DataTierManagerImpl(core, null, true);
        doProcess(metadata,  
                sql, 
                finder, dataMgr , new List[] {Arrays.asList(new String(ObjectConverterUtil.convertToByteArray(new FileInputStream(UnitTestUtil.getTestDataFile("test-schema.xsd")))))}, DEBUG); //$NON-NLS-1$
        
    }
    
}
