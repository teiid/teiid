package org.teiid.translator.coherence;

import java.io.Serializable;


public class Leg  implements Serializable {

	private static final long serialVersionUID = 7683272638393477962L;
	
private double notational;
private long id;
private String name;

   public Leg() {
       super();
   }

   public Leg(long legId, double notional) {
       this.notational = notional;
   }
   
   public String getName() {
	   return this.name;
   }
   
   public void setName(String name) {
	   this.name = name;
   }
   
   public long getLegId() {
	   return id;
   }
   
   public void setLegId(long id) {
	   this.id = id;
   }


   public void setNotational(double notional) {
       this.notational = notional;
   }

   public double getNotational() {
       return notational;
   }
}
