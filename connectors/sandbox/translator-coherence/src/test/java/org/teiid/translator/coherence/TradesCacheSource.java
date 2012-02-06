package org.teiid.translator.coherence;


import java.util.HashMap;
import java.util.Map;

import org.teiid.core.types.DataTypeManager;
import org.teiid.metadata.Table;
import org.teiid.translator.TranslatorException;

import com.tangosol.net.CacheFactory;
import com.tangosol.net.NamedCache;

/**
 * Sample implementation of the SouceCacheAdapter that will
 * translate the Trade related objects to the row/columns that
 * are returned in the resultset
 * 
 * @author vhalbert
 *
 */
public class TradesCacheSource extends SourceCacheAdapter {
	public static final String CACHE_NAME = "Trades";
	public static final int NUMLEGS = 10;
	public static final int NUMTRADES = 3;
	
	static {
		try {
			loadCoherence();
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * Load the cache with 3 trades and 10 legs for each trade.
	 * 
	 * @throws Exception
	 */
	public static void loadCoherence() throws Exception {
		NamedCache tradesCache = CacheFactory.getCache(CACHE_NAME);

		// populate the cache
		Map legsMap = new HashMap();
		
		Object trade = createObject("org.teiid.translator.coherence.Trade");
		for (int i = 1; i <= NUMTRADES; i++) {

			for (int j = 1; j <= NUMLEGS; j++) {
				Object leg = createObject("org.teiid.translator.coherence.Leg");
					//new Leg();
				if (leg == null) {
					throw new Exception("Unable to create leg");
				}
				setValue("Trade", "LegId", leg, j, long.class);
				setValue("Trade", "Notational", leg, j, double.class);
				setValue("Trade", "Name", leg, "LegName " + j, String.class);
				
				legsMap.put(j, leg);
			}
			
			setValue("Trade", "TradeId", trade, i, long.class);
			setValue("Trade", "Name", trade, "TradeName " + i, String.class);
			setValue("Trade", "Legs", trade, legsMap, Map.class);
			
			tradesCache.put(i, trade);
		}

		System.out.println("Loaded Coherence");

	}	
	
	public void addMetadata() throws TranslatorException {
		
		Table t = addTable("Trade");
		addColumn("Name", "Name", DataTypeManager.DefaultDataTypes.STRING, t); //$NON-NLS-1$
		addColumn("TradeId", "TradeId", DataTypeManager.DefaultDataTypes.LONG, t); //$NON-NLS-1$
		addColumn("LegId", "Legs.LegId", DataTypeManager.DefaultDataTypes.LONG, t); //$NON-NLS-1$
		addColumn("Notational", "Legs.Notational", DataTypeManager.DefaultDataTypes.DOUBLE, t); //$NON-NLS-1$
		addColumn("LegName", "Legs.Name", DataTypeManager.DefaultDataTypes.STRING, t); //$NON-NLS-1$
						
	}
	
}
