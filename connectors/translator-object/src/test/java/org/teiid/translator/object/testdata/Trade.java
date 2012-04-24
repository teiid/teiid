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
package org.teiid.translator.object.testdata;

import java.io.Serializable;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Indexed;
import org.hibernate.search.annotations.IndexedEmbedded;
import org.hibernate.search.annotations.ProvidedId;


@Indexed @ProvidedId
public class Trade  implements Serializable {
	
	// 4 attributes, legs is not selectable
	public static int NUM_ATTRIBUTES = 4;

	private static final long serialVersionUID = 8611785625511714561L;
	
private @IndexedEmbedded Map<Object, Object> legs;
private @Field long tradeId;
private @Field String name;
private @Field Date tradeDate;
private @Field boolean settled;

   public Trade() {
   }

   public Trade(long tradeId, String name, Map<Object, Object> legs, Date tradeDate) {
       this.legs = legs;
       this.tradeId = tradeId;
       this.name = name;
       this.tradeDate=tradeDate;
   }
   
   public long getTradeId() {
	   return tradeId;
   }
   
   public void setTradeId(long id) {
	   this.tradeId = id;
   }

   public void setName(String name) {
	   this.name = name;
   }
   
   public String getName() {
	   return this.name;
   }
   
   public Date getTradeDate() {
	   return this.tradeDate;
   }
   
   public boolean isSettled() {
	   return this.settled;
   }

   public void setTradeDate(Date date) {
	   this.tradeDate = date;
   }
   
   public void setLegs(Map<Object, Object> legs) {
       this.legs = legs;
   }
   
   public void setSettled(boolean isSettled) {
	   this.settled = isSettled;
   }

   public Map<Object, Object> getLegs() {
	   if (legs == null) {
		   legs = new HashMap();
	   }
       return legs;
   }
   
   public String toString() {
	   
	   StringBuffer sb = new StringBuffer("Trade:");
	   sb.append(" id " + getTradeId());
	   sb.append(" name " + getName());
	   sb.append(" settled " + isSettled());
	   sb.append(" tradeDate " + getTradeDate());
	   sb.append(" numLegs " + getLegs().size());
	   
	   return sb.toString();
   }
   
}
