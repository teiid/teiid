package org.teiid.resource.adapter.coherence;

import java.io.Serializable;


public class Leg extends BaseID implements Serializable {

	private static final long serialVersionUID = 7683272638393477962L;
	
private double notional;

   public Leg() {
       super();
   }

   public Leg(long legId, double notional) {
       super(legId);
       this.notional = notional;
   }

   public void setNotional(double notional) {
       this.notional = notional;
   }

   public double getNotional() {
       return notional;
   }
}
