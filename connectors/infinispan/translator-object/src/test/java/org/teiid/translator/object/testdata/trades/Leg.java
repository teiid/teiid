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
package org.teiid.translator.object.testdata.trades;

import java.io.Serializable;
import java.util.Date;
import java.util.List;


public class Leg  implements Serializable {

	// 4 attributes, transactions is not selectable
	public static int NUM_ATTRIBUTES = 4;
	
	private static final long serialVersionUID = 7683272638393477962L;
	
private  double notational;
private  long id;
private  String legName;
private  Date createdDateTime;

private   List<Transaction> transactions = null;

   public Leg() {
       super();
   }

   public Leg(long legId, String name, double notional, Date date) {
       this.notational = notional;
       this.id=legId;
       this.legName=name;
       this.createdDateTime=date;
   }
   
    
   public String getLegName() {
	   return this.legName;
   }
   
   public void setLegName(String name) {
	   this.legName = name;
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
   
   public void setCreatedDateTime(Date ts) {
	   this.createdDateTime = ts;
   }
   
    
   public Date getCreatedDateTime() {
	   return this.createdDateTime;
   }
   
   public void setTransations(List<Transaction> transactions) {
	   this.transactions = transactions;
   }
   
     
   public List<Transaction> getTransactions() {
	   return this.transactions;
   }
   
   @Override
public String toString() {
	   StringBuffer sb = new StringBuffer("Leg:");
	   sb.append(" id " + getLegId());
	   sb.append(" name " + getLegName());
	   sb.append(" notational " + getNotational());
	   sb.append(" createdDate " + getCreatedDateTime());
	   sb.append(" numTransactions " + getTransactions());
			   
	
	   
	   return sb.toString();
   }
}
