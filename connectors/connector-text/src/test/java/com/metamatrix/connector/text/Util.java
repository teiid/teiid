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

package com.metamatrix.connector.text;

import java.io.File;
import java.util.List;

import junit.framework.Assert;

import org.mockito.Mockito;
import org.teiid.connector.api.ConnectorLogger;
import org.teiid.connector.metadata.runtime.Column;
import org.teiid.connector.metadata.runtime.MetadataStore;
import org.teiid.connector.metadata.runtime.Schema;
import org.teiid.connector.metadata.runtime.Table;
import org.teiid.metadata.CompositeMetadataStore;
import org.teiid.metadata.TransformationMetadata;

import com.metamatrix.cdk.api.ConnectorHost;
import com.metamatrix.cdk.api.TranslationUtility;
import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.core.util.UnitTestUtil;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.unittest.RealMetadataFactory;

public class Util {

	static void helpTestExecution(String vdb, String descriptorFile, String sql, int maxBatchSize, int expectedRowCount) throws Exception {
		descriptorFile = UnitTestUtil.getTestDataPath() + File.separator + descriptorFile;
	   
        TextManagedConnectionFactory config = Mockito.mock(TextManagedConnectionFactory.class);
        Mockito.stub(config.getDescriptorFile()).toReturn(descriptorFile);
        Mockito.stub(config.isPartialStartupAllowed()).toReturn(true);
        Mockito.stub(config.getDateResultFormats()).toReturn("yyyy-MM-dd,hh:mm:ss,hh:mm,dd/mm/yyyy");
        Mockito.stub(config.getDateResultFormatsDelimiter()).toReturn(",");
        Mockito.stub(config.getLogger()).toReturn(Mockito.mock(ConnectorLogger.class));	    
	    
	    ConnectorHost host = new ConnectorHost(new TextConnector(), config, UnitTestUtil.getTestDataPath() + File.separator + vdb);
	    List results = host.executeCommand(sql);
	    Assert.assertEquals("Total row count doesn't match expected size. ", expectedRowCount, results.size()); //$NON-NLS-1$
	}

	public static ConnectorHost getConnectorHostWithFakeMetadata(String descriptorFile) throws Exception {
        TextManagedConnectionFactory config = Mockito.mock(TextManagedConnectionFactory.class);
        Mockito.stub(config.getDescriptorFile()).toReturn(descriptorFile);
        Mockito.stub(config.isPartialStartupAllowed()).toReturn(true);
        Mockito.stub(config.getDateResultFormats()).toReturn("yyyy-MM-dd,hh:mm:ss,hh:mm,dd/mm/yyyy");
        Mockito.stub(config.getDateResultFormatsDelimiter()).toReturn(",");
        Mockito.stub(config.isEnforceColumnCount()).toReturn(true);
        
        Mockito.stub(config.getLogger()).toReturn(Mockito.mock(ConnectorLogger.class));	
	    
	    
	    ConnectorHost host = new ConnectorHost(new TextConnector(), config, new TranslationUtility(exampleText()));
	    return host;
	}
	
    public static QueryMetadataInterface exampleText() { 
    	MetadataStore store = new MetadataStore();
        // Create models
        Schema lib = RealMetadataFactory.createPhysicalModel("Text", store); //$NON-NLS-1$

        // Create physical groups
        Table library = RealMetadataFactory.createPhysicalGroup("Library", lib); //$NON-NLS-1$
        
        // Create physical elements
        String[] elemNames = new String[] { 
             "ID", "PDate", "Author" }; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
             
        String[] elemTypes = new String[] { DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.DATE, 
            DataTypeManager.DefaultDataTypes.STRING };
               
        List<Column> libe1 = RealMetadataFactory.createElements( library, elemNames, elemTypes);
        int index = 0;
        for (Column column : libe1) {
			column.setNameInSource(String.valueOf(index++));
		}
        return new TransformationMetadata(null, new CompositeMetadataStore(store), null, null);
    }

}
