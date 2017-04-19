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

import java.util.Arrays;
import java.util.List;

import org.teiid.dqp.internal.datamgr.CapabilitiesConverter;
import org.teiid.query.optimizer.capabilities.DefaultCapabilitiesFinder;
import org.teiid.query.processor.HardcodedDataManager;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.query.unittest.TimestampUtil;
import org.teiid.translator.jdbc.sybase.SybaseExecutionFactory;
import org.teiid.util.Version;

@SuppressWarnings("nls")
public class TestSybasePlanning extends BaseQueryTest {

    private static final boolean DEBUG = false;

    public TestSybasePlanning(String name) {
        super(name);
    }

    public void testQuery3() throws Exception{
        SybaseExecutionFactory ef = new SybaseExecutionFactory();
        ef.setDatabaseVersion(Version.DEFAULT_VERSION);
        
        DefaultCapabilitiesFinder capFinder = new DefaultCapabilitiesFinder(CapabilitiesConverter.convertCapabilities(ef));
        
        HardcodedDataManager dataMgr = new HardcodedDataManager();
    
        List<?>[] expected =
            new List<?>[] { Arrays.asList("2002") };

        dataMgr.addData("SELECT g_0.TimestampValue FROM BQT1.SmallA AS g_0", //$NON-NLS-1$
                        new List<?>[] {Arrays.asList(TimestampUtil.createTimestamp(102, 1, 2, 3, 4, 5, 6))});

        doProcess(RealMetadataFactory.exampleBQTCached(),  
                "select formattimestamp(timestampvalue, 'yyyy') from bqt1.smalla", //$NON-NLS-1$
                capFinder, dataMgr, expected, DEBUG);
        
    }
    
}
