package org.teiid.coherence.translator;


import java.io.Serializable;

public abstract class BaseID implements Serializable {

 private long id;

   public BaseID() {
       super();
   }

   public BaseID(long id) {
       super();
       this.id = id;
   }

   public void setId(long id) {
       this.id = id;
   }

   public long getId() {
       return id;
   }

}
