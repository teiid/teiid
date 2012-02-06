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

package org.teiid.translator.coherence;

import java.util.Iterator;
import java.util.List;

import junit.framework.TestCase;

import org.junit.Test;
import org.teiid.cdk.api.ConnectorHost;
import org.teiid.cdk.api.TranslationUtility;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.metadata.Column;
import org.teiid.metadata.MetadataStore;
import org.teiid.metadata.Schema;
import org.teiid.metadata.Table;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.resource.adapter.coherence.CoherenceManagedConnectionFactory;


public class TestCoherenceTranslator extends TestCase  {
	
	public static final String CACHE_NAME = "Trades";
	public static final String OBJECT_TRANSLATOR = "org.teiid.translator.coherence.TradesCacheSource";

	
	public static QueryMetadataInterface metadata = null;
	
	public static ConnectorHost host = null;
	
	public static final int NUMLEGS = 10;
	public static final int NUMTRADES = 3;

	static {
		new TradesCacheSource();		
		
	}

	private ConnectorHost setup() throws Exception {
		CoherenceManagedConnectionFactory connFactory = new CoherenceManagedConnectionFactory();
		connFactory.setCacheName(CACHE_NAME);
		connFactory.setCacheTranslatorClassName(OBJECT_TRANSLATOR);
		
		CoherenceExecutionFactory execFactory = new CoherenceExecutionFactory();
			
		ConnectorHost host = new ConnectorHost(execFactory, connFactory.createConnectionFactory().getConnection(), UnitTestUtil.getTestDataPath() + "/Trade.vdb");
		return host;
	}

	/**
	 * This will instantiate the {@link CoherenceManagedConnectionFactory} and
	 * obtain a connection to
	 * 
	 * @throws Exception
	 */
	public void testGet1TradeWith10Legs() throws Exception {
			
		ConnectorHost host = setup();
		
		List<List> actualResults = host.executeCommand("select tradeid, name, LegId, notational From Trade where tradeid = 1");
		
		for (Iterator it=actualResults.iterator(); it.hasNext();) {
			List row = (List) it.next();
//			System.out.println("ActualResults Columns #: " + row.size());
			for (Iterator rowit=row.iterator(); rowit.hasNext();) {
				Object actualValue = rowit.next();
//				System.out.println("Result value type: " + actualValue.getClass().getName() + "  value: " + actualValue);
			}
			
		}
		
        // Compare actual and expected results
		// should get back the 10 legs associated with the trade
        assertEquals("Did not get expected number of rows", 10, actualResults.size()); //$NON-NLS-1$
             
	}

	public void testGetAllTrades() throws Exception {
		
		ConnectorHost host = setup();
		
		List actualResults = host.executeCommand("select tradeid, name From Trade");
		
        // Compare actual and expected results
		// should get back the 10 legs for each trade (3) totaling 30
        assertEquals("Did not get expected number of rows", 3, actualResults.size()); //$NON-NLS-1$
             
	}	
	
	public void testTradesAndLegsWhereTradeLessThanGreatThan() throws Exception {

		
		ConnectorHost host = setup();
		
		List actualResults = host.executeCommand("select tradeid, legid, notational From Trade where tradeid > 2");
		
        assertEquals("Did not get expected number of rows", 10, actualResults.size()); //$NON-NLS-1$
        
		actualResults = host.executeCommand("select tradeid, legid, notational From Trade where tradeid < 3");
		
        assertEquals("Did not get expected number of rows", 20, actualResults.size()); //$NON-NLS-1$

		actualResults = host.executeCommand("select tradeid, legid, notational From Trade where tradeid <= 3");
		
        assertEquals("Did not get expected number of rows", 30, actualResults.size()); //$NON-NLS-1$
        
        actualResults = host.executeCommand("select tradeid, legid, notational From Trade where tradeid >= 1");
		
        assertEquals("Did not get expected number of rows", 30, actualResults.size()); //$NON-NLS-1$
        
		actualResults = host.executeCommand("select tradeid, legid, notational From Trade where tradeid < 1");
		
        assertEquals("Did not get expected number of rows", 0, actualResults.size()); //$NON-NLS-1$
              
           
	}	
	
	/**
	 * This is not supported out-of-the-box in Coherence, but can be developed
	 * @throws Exception
	 */
//	public void testTradesAndLegsWhereLegLessThanGreatThan() throws Exception {
//
//		CoherenceManagedConnectionFactory connFactory = new CoherenceManagedConnectionFactory();
//		connFactory.setCacheName(CACHE_NAME);
//		connFactory.setCacheTranslatorClassName(OBJECT_TRANSLATOR);
//
//		CoherenceExecutionFactory execFactory = new CoherenceExecutionFactory();
//			
//		ConnectorHost host = new ConnectorHost(execFactory, connFactory.createConnectionFactory().getConnection(), getTradeTranslationUtility());
//		
//		List actualResults = host.executeCommand("select tradeid, legid, notational From Trade where legid > 2");
//		
//        assertEquals("Did not get expected number of rows", 10, actualResults.size()); //$NON-NLS-1$
//        
//		actualResults = host.executeCommand("select tradeid, legid, notational From Trade where legid < 3");
//		
//        assertEquals("Did not get expected number of rows", 20, actualResults.size()); //$NON-NLS-1$
//
//		actualResults = host.executeCommand("select tradeid, legid, notational From Trade where legid <= 3");
//		
//        assertEquals("Did not get expected number of rows", 30, actualResults.size()); //$NON-NLS-1$
//        
//        actualResults = host.executeCommand("select tradeid, legid, notational From Trade where legid >= 1");
//		
//        assertEquals("Did not get expected number of rows", 30, actualResults.size()); //$NON-NLS-1$
//        
//		actualResults = host.executeCommand("select tradeid, legid, notational From Trade where legid < 1");
//		
//        assertEquals("Did not get expected number of rows", 0, actualResults.size()); //$NON-NLS-1$
//              
//           
//	}	
	

	public void testLikeTradesWithLegs() throws Exception {

		CoherenceManagedConnectionFactory connFactory = new CoherenceManagedConnectionFactory();
		connFactory.setCacheName(CACHE_NAME);
		connFactory.setCacheTranslatorClassName(OBJECT_TRANSLATOR);

		CoherenceExecutionFactory execFactory = new CoherenceExecutionFactory();
			
		ConnectorHost host = new ConnectorHost(execFactory, connFactory.createConnectionFactory().getConnection(),getTradeTranslationUtility());
		
		List actualResults = host.executeCommand("select tradeid, name, legid, notational From Trade where Name like 'Trade%' ");
		
        // Compare actual and expected results
		// should get back the 10 legs for each trade (3) totaling 30
        assertEquals("Did not get expected number of rows", 30, actualResults.size()); //$NON-NLS-1$
        
        actualResults = host.executeCommand("select tradeid, legid, notational From Trade where Name like '%2%' ");
		
        // Compare actual and expected results
		// should get back the 10 legs for each trade (3) totaling 30
        assertEquals("Did not get expected number of rows", 10, actualResults.size()); //$NON-NLS-1$
        
             
	}		

   
    
	public TranslationUtility getTradeTranslationUtility() {
		MetadataStore metadataStore = new MetadataStore();
        // Create models
        Schema trading = RealMetadataFactory.createPhysicalModel("CoherenceModel", metadataStore); //$NON-NLS-1$
        
        // Create physical groups
        Table quotes = RealMetadataFactory.createPhysicalGroup("TRADE", trading); //$NON-NLS-1$
                
        // Create physical elements
        String[] elemNames = new String[] {
                "NAME", "TRADEID", "LEGID", "NOTATIONAL", "LEGNAME"  //$NON-NLS-1$ //$NON-NLS-2$
        };
        String[] elemTypes = new String[] {  
        		DataTypeManager.DefaultDataTypes.STRING,
                DataTypeManager.DefaultDataTypes.LONG, 
                DataTypeManager.DefaultDataTypes.LONG,
                DataTypeManager.DefaultDataTypes.DOUBLE,
        		DataTypeManager.DefaultDataTypes.STRING
        };
        
        List<Column> cols = RealMetadataFactory.createElements(quotes, elemNames, elemTypes);
        
        // Set name in source on each column
        String[] nameInSource = new String[] {
        		"Name",
            	"TradeId", 
                "Legs.LegId",           
                "Legs.Notational",
                "Legs.Name"
        };
        for(int i=0; i<nameInSource.length; i++) {
            cols.get(i).setNameInSource(nameInSource[i]);
        }
        
        // Set column-specific properties
 //       cols.get(0).setSelectable(false);
 //       cols.get(0).setSearchType(SearchType.Unsearchable);
        
		return new TranslationUtility(RealMetadataFactory.createTransformationMetadata(metadataStore, "trading"));
	}
    
}
