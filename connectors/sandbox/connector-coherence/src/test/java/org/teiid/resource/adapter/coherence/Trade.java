package org.teiid.resource.adapter.coherence;

import java.io.Serializable;

import java.util.Map;



public class Trade extends BaseID implements Serializable {

	private static final long serialVersionUID = 8611785625511714561L;
	
private Map legs;
private String name;

   public Trade() {
       super();
   }

   public Trade(long tradeId, Map legs) {
       super(tradeId);
       this.legs = legs;
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
       return legs;
   }
   
   public String toString() {
	   
	   StringBuffer sb = new StringBuffer("Trade:");
	   sb.append(" id " + getId());
	   return sb.toString();
   }
   
}
