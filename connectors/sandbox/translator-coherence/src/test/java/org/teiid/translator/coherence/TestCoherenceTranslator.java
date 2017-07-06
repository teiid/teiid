/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.teiid.translator.coherence;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import junit.framework.TestCase;

import org.teiid.cdk.api.ConnectorHost;
import org.teiid.cdk.api.TranslationUtility;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.metadata.Column;
import org.teiid.metadata.ForeignKey;
import org.teiid.metadata.KeyRecord;
import org.teiid.metadata.KeyRecord.Type;
import org.teiid.metadata.MetadataStore;
import org.teiid.metadata.Schema;
import org.teiid.metadata.Table;
import org.teiid.query.mapping.relational.QueryNode;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.resource.adapter.coherence.CoherenceConnection;
import org.teiid.resource.adapter.coherence.CoherenceManagedConnectionFactory;


public class TestCoherenceTranslator extends TestCase  {
	
	public static final String OBJECT_TRANSLATOR = "org.teiid.translator.coherence.TradesCacheSource";
	
	public static QueryMetadataInterface metadata = null;

	protected ConnectorHost host = null;
	
	@Override
	protected void setUp() throws Exception {
		// TODO Auto-generated method stub
		super.setUp();
		
		new TradesCacheSource().addMetadata();
		
		CoherenceManagedConnectionFactory connFactory = new CoherenceManagedConnectionFactory();
		connFactory.setCacheName(TradesCacheSource.CACHE_NAME);
		connFactory.setCacheTranslatorClassName(OBJECT_TRANSLATOR);
		CoherenceConnection conn = (CoherenceConnection) connFactory.createConnectionFactory().getConnection();
			
		CoherenceExecutionFactory execFactory = new CoherenceExecutionFactory();
			
		host = new ConnectorHost(execFactory, conn,
				//getTradeTranslationUtility());
				UnitTestUtil.getTestDataPath() + "/Coherence_Designer_Project/Trade.vdb");
	}
	
	@Override
	protected void tearDown() throws Exception {
		// TODO Auto-generated method stub
		super.tearDown();
		
		
		host = null;
	}

	public void testInsertAndDelete() throws Exception {
		StringBuilder tradeSQL = new StringBuilder();
		
					tradeSQL.append("INSERT INTO Trade_Object.Trade (tradeid, name) VALUES (");
					tradeSQL.append(99);
					tradeSQL.append(",");
					tradeSQL.append(" 'TradeName ");
					tradeSQL.append(99);
					tradeSQL.append("')");
												
		host.executeCommand(tradeSQL.toString());
		
		List actualResults = host.executeCommand("select * From Trade_Object.Trade where Trade_Object.Trade.TradeID = 99");
		//tradeid, name
        // Compare actual and expected results
		// should get back the 10 legs for each trade (3) totaling 30
        assertEquals("Did not get expected number of rows", 1, actualResults.size()); //$NON-NLS-1$
        
        
//		tradeSQL = new StringBuilder();
//		
//		tradeSQL.append("INSERT INTO Trade_Object.Leg (tradeid, LegID, name, Notational) VALUES (");
//		tradeSQL.append(99);
//		tradeSQL.append(",");
//		tradeSQL.append(1);
//		tradeSQL.append(",");		
//		tradeSQL.append(" 'Leg ");
//		tradeSQL.append(1);
//		tradeSQL.append("',");		
//		tradeSQL.append(1.24);
//		tradeSQL.append(")");
//									
//		System.out.println(tradeSQL.toString());
//		host.executeCommand(tradeSQL.toString());
//
//		tradeSQL = new StringBuilder();
//		
//		tradeSQL.append("INSERT INTO Trade_Object.Leg (tradeid, LegID, name, Notational) VALUES (");
//		tradeSQL.append(99);
//		tradeSQL.append(",");
//		tradeSQL.append(2);
//		tradeSQL.append(",");		
//		tradeSQL.append(" 'Leg ");
//		tradeSQL.append(2);
//		tradeSQL.append("',");		
//		tradeSQL.append(2.48);		
//		tradeSQL.append(")");
//									
//		System.out.println(tradeSQL.toString());
//		host.executeCommand(tradeSQL.toString());
		
		
		
//		actualResults = host.executeCommand(
//	      		"SELECT Trade_Object.Trade.TradeID, Trade_Object.Trade.Name, Trade_Object.Leg.LegID, Trade_Object.Leg.Notational, Trade_Object.Leg.Name AS LegName " +
//	        	" FROM Trade_Object.Trade, Trade_Object.Leg " +
//	        	 "WHERE	Trade_Object.Trade.TradeID = Trade_Object.Leg.TradeID and Trade_Object.Trade.TradeID = 99");
//
//	     assertEquals("Did not get expected number of rows", 2, actualResults.size()); //$NON-NLS-1$
        
 
		actualResults = host.executeCommand("DELETE FROM Trade_Object.Trade Where Trade_Object.Trade.TradeID = 99");
		
		actualResults = host.executeCommand("select * From Trade_Object.Trade where Trade_Object.Trade.TradeID = 99");
		//tradeid, name
        // Compare actual and expected results
		// should get back the 10 legs for each trade (3) totaling 30
        assertEquals("Did not get expected number of rows", 0, actualResults.size()); //$NON-NLS-1$
		
     
        
             
	}	
	
//	public void testUpdateChildOfExisingParent() throws Exception {
//		StringBuilder tradeSQL = new StringBuilder();
//		
//		
//		tradeSQL.append("INSERT INTO Trade_Object.Leg (tradeid, LegID, name, Notational) VALUES (");
//		tradeSQL.append(99);
//		tradeSQL.append(",");
//		tradeSQL.append(1);
//		tradeSQL.append(",");		
//		tradeSQL.append(" 'Leg ");
//		tradeSQL.append(1);
//		tradeSQL.append("',");		
//		tradeSQL.append(1.24);
//		tradeSQL.append(")");
////									
////		System.out.println(tradeSQL.toString());
////		host.executeCommand(tradeSQL.toString());												
//		host.executeCommand(tradeSQL.toString());
//	}	

	/**
	 * This will instantiate the {@link CoherenceManagedConnectionFactory} and
	 * obtain a connection to
	 * 
	 * @throws Exception
	 */
	public void testGet1TradeWith10Legs() throws Exception {
			
		List<List> actualResults = host.executeCommand(
	      		"SELECT Trade_Object.Trade.TradeID, Trade_Object.Trade.Name, Trade_Object.Leg.LegID, Trade_Object.Leg.Notational, Trade_Object.Leg.Name AS LegName " +
	        	" FROM Trade_Object.Trade, Trade_Object.Leg " +
	        	 "WHERE	Trade_Object.Trade.TradeID = Trade_Object.Leg.TradeID ");
		
	    //    		"SELECT * FROM Trade.Trad " +
	      //  			" WHERE Trade.Trade.TradeID = Trade.Trade.TradeID");
	  
				
		//"select tradeid, name, LegId, notational From Trade_View.Trades where tradeid = 1");
		
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
        assertEquals("Did not get expected number of rows", 30, actualResults.size()); //$NON-NLS-1$
             
	}

	public void testGetAllTrades() throws Exception {
		
		List actualResults = host.executeCommand("select * From Trade_Object.Trade");
		//tradeid, name
        // Compare actual and expected results
		// should get back the 10 legs for each trade (3) totaling 30
        assertEquals("Did not get expected number of rows", 3, actualResults.size()); //$NON-NLS-1$
             
	}	
	
	public void testTradesAndLegsWhereTradeLessThanGreatThan() throws Exception {
		
		String prefix = "SELECT Trade_Object.Trade.TradeID, Trade_Object.Trade.Name, Trade_Object.Leg.LegID, Trade_Object.Leg.Notational, Trade_Object.Leg.Name AS LegName " +
    	" FROM Trade_Object.Trade, Trade_Object.Leg " +
   	 "WHERE	Trade_Object.Trade.TradeID = Trade_Object.Leg.TradeID and ";
		
		List actualResults = host.executeCommand(prefix + "Trade_Object.Trade.TradeID > 2");
		
        assertEquals("Did not get expected number of rows", 10, actualResults.size()); //$NON-NLS-1$
        
		actualResults = host.executeCommand(prefix + "Trade_Object.Trade.TradeID < 3");
		
        assertEquals("Did not get expected number of rows", 20, actualResults.size()); //$NON-NLS-1$

		actualResults = host.executeCommand(prefix + "Trade_Object.Trade.TradeID <= 3");
		
        assertEquals("Did not get expected number of rows", 30, actualResults.size()); //$NON-NLS-1$
        
        actualResults = host.executeCommand(prefix + "Trade_Object.Trade.TradeID >= 1");
		
        assertEquals("Did not get expected number of rows", 30, actualResults.size()); //$NON-NLS-1$
        
		actualResults = host.executeCommand(prefix + "Trade_Object.Trade.TradeID < 1");
		
        assertEquals("Did not get expected number of rows", 0, actualResults.size()); //$NON-NLS-1$
              
           
	}	
	
	

	public void testLikeTradesWithLegs() throws Exception {

		String prefix = "SELECT Trade_Object.Trade.TradeID, Trade_Object.Trade.Name, Trade_Object.Leg.LegID, Trade_Object.Leg.Notational, Trade_Object.Leg.Name AS LegName " +
    	" FROM Trade_Object.Trade, Trade_Object.Leg " +
   	 "WHERE	Trade_Object.Trade.TradeID = Trade_Object.Leg.TradeID and ";

	
		List actualResults = host.executeCommand(prefix + " Trade_Object.Trade.Name like 'Trade%' ");
		
        // Compare actual and expected results
		// should get back the 10 legs for each trade (3) totaling 30
        assertEquals("Did not get expected number of rows", 30, actualResults.size()); //$NON-NLS-1$
        
        actualResults = host.executeCommand(prefix + " Trade_Object.Trade.Name like '%2%' ");
		
        // Compare actual and expected results
		// should get back the 10 legs for each trade (3) totaling 30
        assertEquals("Did not get expected number of rows", 10, actualResults.size()); //$NON-NLS-1$
        
             
	}		

   
    /**
     * this is used as an alternative to loading the vdb
     * @return
     */
	private TranslationUtility getTradeTranslationUtility() {
		MetadataStore metadataStore = new MetadataStore();
        // Create TRADE
        Schema trading = RealMetadataFactory.createPhysicalModel("Trade_Object", metadataStore); //$NON-NLS-1$
        
        // Create physical groups
        Table trade = RealMetadataFactory.createPhysicalGroup("TRADE", trading); //$NON-NLS-1$
        trade.setNameInSource("org.teiid.translator.coherence.Trade");
                
        // Create physical elements
        String[] elemNames = new String[] {
                "NAME", "TRADEID"  //$NON-NLS-1$ //$NON-NLS-2$
        };
        String[] elemTypes = new String[] {  
        		DataTypeManager.DefaultDataTypes.STRING,
                DataTypeManager.DefaultDataTypes.LONG
        };
        
        List<Column> cols = RealMetadataFactory.createElements(trade, elemNames, elemTypes);
        
        // Set name in source on each column
        String[] nameInSource = new String[] {
        		"Name",
            	"TradeId"
        };
        for(int i=0; i<nameInSource.length; i++) {
            cols.get(i).setNameInSource(nameInSource[i]);
        }
        List<Column> keys = new ArrayList(1);
        keys.add(cols.get(1));
        
        KeyRecord trade_pk = RealMetadataFactory.createKey(Type.Primary, "TradeID_PK", trade, keys);
        
        // LEG
        Table leg = RealMetadataFactory.createPhysicalGroup("LEG", trading); //$NON-NLS-1$
        leg.setNameInSource("org.teiid.translator.coherence.Leg");
        
        // Create physical elements
        String[] legNames = new String[] {
                "LEGID", "NOTATIONAL", "NAME", "TRADEID"  //$NON-NLS-1$ //$NON-NLS-2$
        };
        String[] legTypes = new String[] {  
                DataTypeManager.DefaultDataTypes.LONG,
                DataTypeManager.DefaultDataTypes.DOUBLE,
        		DataTypeManager.DefaultDataTypes.STRING,
        		DataTypeManager.DefaultDataTypes.LONG
                       };
        
        List<Column> legcols = RealMetadataFactory.createElements(leg, legNames, legTypes);
        
        // Set name in source on each column
        String[] legnameInSource = new String[] {
                "LegId",           
                "Notational",
                "Name",
                "TradeId"
        };
        for(int i=0; i<legnameInSource.length; i++) {
            legcols.get(i).setNameInSource(legnameInSource[i]);
        }
        legcols.get(3).setSelectable(false);
        legcols.get(3).setUpdatable(false);
        
        List<Column> legkeys = new ArrayList(1);
        keys.add(legcols.get(0));
        
        
        
        RealMetadataFactory.createKey(Type.Primary, "Leg_ID_PK", leg, legkeys);
        
        List<Column> foreignkey = new ArrayList(1);
        foreignkey.add(legcols.get(3));
        
        ForeignKey fk = RealMetadataFactory.createForeignKey("TRADE_FK", leg, foreignkey, trade_pk);
        fk.setNameInSource("Legs");
        fk.setParent(trade);
 
       
        // Set column-specific properties
 //       cols.get(0).setSelectable(false);
 //       cols.get(0).setSearchType(SearchType.Unsearchable);
        
        Schema tradeview = RealMetadataFactory.createVirtualModel("Trade_View", metadataStore);
        
        QueryNode qn = new QueryNode(
        		"SELECT Trade_Object.Trade.TradeID, Trade_Object.Trade.Name, Trade_Object.Leg.LegID, Trade_Object.Leg.Notational, Trade_Object.Leg.Name AS LegName " +
        	" FROM Trade_Object.Trade, Trade_Object.Leg " +
        	 "WHERE	Trade_Object.Trade.TradeID = Trade_Object.Leg.TradeID " +    		
        		"SELECT * FROM Trade.Trad " +
        			" WHERE Trade.Trade.TradeID = Trade.Trade.TradeID");
        
        Table trades = RealMetadataFactory.createVirtualGroup("Trades", tradeview, qn);
        
  		return new TranslationUtility(RealMetadataFactory.createTransformationMetadata(metadataStore, "Trade"));
	}
    
}
