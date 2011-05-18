package org.teiid.coherence.translator;

import java.io.Serializable;

import java.util.Map;



public class Trade extends BaseID implements Serializable {

	private static final long serialVersionUID = 8611785625511714561L;
	
private Map legs;

   public Trade() {
       super();
   }

   public Trade(long tradeId, Map legs) {
       super(tradeId);
       this.legs = legs;
   }


   public void setLegs(Map legs) {
       this.legs = legs;
   }

   public Map getLegs() {
       return legs;
   }
   
}
