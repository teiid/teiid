package org.teiid.translator.coherence;


import java.util.HashMap;
import java.util.Map;

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
	
	
	/**
	 * Load the cache with 3 trades and 10 legs for each trade.
	 * 
	 * @throws Exception
	 */
	private static void loadCoherence() throws Exception {
		NamedCache tradesCache = CacheFactory.getCache(CACHE_NAME);
		TradesCacheSource translator = new TradesCacheSource();
		
		for (int i = 1; i <= NUMTRADES; i++) {

			Trade trade = (Trade) translator.createObjectFromMetadata("org.teiid.translator.coherence.Trade");
//			execFactory.getCacheTranslator().createObject("org.teiid.translator.coherence.Trade");
			
			Map legsMap = new HashMap();
			for (int j = 1; j <= NUMLEGS; j++) {
	
				Object leg = translator.createObjectFromMetadata("org.teiid.translator.coherence.Leg");
					//createObject("org.teiid.translator.coherence.Leg");
					//new Leg();
				if (leg == null) {
					throw new Exception("Unable to create leg");
				}
				translator.setValue("Trade", "LegId", leg, j, long.class);
				translator.setValue("Trade", "Notational", leg, j, double.class);
				translator.setValue("Trade", "Name", leg, "LegName " + j, String.class);
				
				legsMap.put(j, leg);
			}
			
			translator.setValue("Trade", "TradeId", trade, i, long.class);
			translator.setValue("Trade", "Name", trade, "TradeName " + i, String.class);
			translator.setValue("Trade", "Legs", trade, legsMap, Map.class);
			
			tradesCache.put(i, trade);
		}


	}

	@Override
	public void addMetadata() throws TranslatorException {
		// TODO Auto-generated method stub
		try {
			System.out.println("Loading Coherence");
			loadCoherence();
			System.out.println("Loaded Coherence");

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}
