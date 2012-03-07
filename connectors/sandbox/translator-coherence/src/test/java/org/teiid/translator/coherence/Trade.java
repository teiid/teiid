package org.teiid.translator.coherence;

import java.io.Serializable;

import java.util.HashMap;
import java.util.Map;



public class Trade  implements Serializable {

	private static final long serialVersionUID = 8611785625511714561L;
	
private Map legs;
private long id;
private String name;

   public Trade() {
   }

   public Trade(long tradeId, Map legs) {
       this.legs = legs;
   }
   
   public long getTradeId() {
	   return id;
   }
   
   public void setTradeId(long id) {
	   this.id = id;
   }

   public void setName(String name) {
	   this.name = name;
   }
   
   public String getName() {
	   return this.name;
   }

   public void setLegs(Map legs) {
       this.legs = legs;
   }

   public Map getLegs() {
	   if (legs == null) {
		   legs = new HashMap();
	   }
       return legs;
   }
   
   public String toString() {
	   
	   StringBuffer sb = new StringBuffer("Trade:");
	   sb.append(" id " + getTradeId());
	   sb.append(" numLegs " + getLegs().size());
	   return sb.toString();
   }
   
}
