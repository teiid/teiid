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
package org.teiid.translator.object.testdata.annotated;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import org.hibernate.search.annotations.Analyze;
import org.hibernate.search.annotations.DateBridge;
import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Index;
import org.hibernate.search.annotations.Indexed;
import org.hibernate.search.annotations.IndexedEmbedded;
import org.hibernate.search.annotations.Resolution;
import org.hibernate.search.annotations.Store;
import org.teiid.translator.object.testdata.trades.MetaData;


@Indexed(index="Trade")
public class Trade  implements Serializable {
	
	// 4 attributes, legs is not selectable
	public static int NUM_ATTRIBUTES = 4;

	private static final long serialVersionUID = 8611785625511714561L;
	

protected @IndexedEmbedded List<Leg> legs = new ArrayList<Leg>();


@Field(index=Index.YES)
protected  long tradeId;

@Field(index=Index.YES)
protected  String name;

@Field @DateBridge(resolution=Resolution.MINUTE)
protected  Date tradeDate;

@Field(index=Index.NO, store=Store.YES, analyze=Analyze.NO)
protected  String description;

protected boolean settled;

@Field(index=Index.YES)
protected   MetaData metaData;

   public Trade() {
   }

   public Trade(long tradeId, String name, List<Leg> legs, Date tradeDate) {
       this.legs = legs;
       this.tradeId = tradeId;
       this.name = name;
       this.tradeDate=tradeDate;
   }
   
   @Field 
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
   
   public String getDescription() {
	   return this.description;
   }
   
   @Field
   public Date getTradeDate() {
	   return this.tradeDate;
   }
   
   @Field(index=Index.YES)
   public boolean isSettled() {
	   return this.settled;
   }

   @IndexedEmbedded  
   public List<Leg> getLegs() {
	   if (legs == null) {
		   return Collections.emptyList();
	   }
       return legs;
   }

   public void setTradeDate(Date date) {
	   this.tradeDate = date;
   }
   
   public void setLegs(List<Leg> legs) {
       this.legs = legs;
   }
   
   public void setSettled(boolean isSettled) {
	   this.settled = isSettled;
   }
   
   public void setDescription(String desc) {
	   this.description = desc;
   }
   
   /**
 * @return metaData
 */
public MetaData getMetaData() {
	return metaData;
}

/**
 * @param metaData Sets metaData to the specified value.
 */
public void setMetaData(MetaData metaData) {
	this.metaData = metaData;
}
  
   @Override
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
