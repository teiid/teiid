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

package com.metamatrix.dqp.internal.process;

import java.util.List;

import com.metamatrix.core.util.ArgCheck;
import com.metamatrix.core.util.HashCodeUtil;
import com.metamatrix.core.util.LRUCache;
import com.metamatrix.query.analysis.AnalysisRecord;
import com.metamatrix.query.processor.ProcessorPlan;
import com.metamatrix.query.sql.lang.Command;

/**
 * This class is used to cahce plan and related objects for prepared statement
 */
public class PreparedPlanCache {
	public static final int DEFAULT_MAX_SIZE_TOTAL = 100;

	private LRUCache cache;
	
	PreparedPlanCache(){
		this(DEFAULT_MAX_SIZE_TOTAL);
	}
	
	PreparedPlanCache (int maxSize ){
		if(maxSize < 0){
			maxSize = DEFAULT_MAX_SIZE_TOTAL;
		}
		cache = new LRUCache(maxSize);
	}	
	
	/**
	 * Return the PreparedPlan for the given session and SQl query
	 * @param session ClientConnection
	 * @param sql SQL query string
	 * @return PreparedPlan for the given clientConn and SQl query. Null if not exist.
	 */
	public synchronized PreparedPlan getPreparedPlan(String sessionId, String sql, boolean isPreparedBatchUpdate){
		ArgCheck.isNotNull(sessionId);
		ArgCheck.isNotNull(sql);
		
		CacheID cID = new CacheID(sessionId, sql, isPreparedBatchUpdate);
		
		return (PreparedPlan)cache.get(cID);
	}
	
	/**
	 * Create PreparedPlan for the given clientConn and SQl query
	 */
	public synchronized PreparedPlan createPreparedPlan(String sessionId, String sql, boolean isPreparedBatchUpdate){
		ArgCheck.isNotNull(sessionId);
		ArgCheck.isNotNull(sql);
		
		CacheID cID = new CacheID(sessionId, sql, isPreparedBatchUpdate);
		PreparedPlan preparedPlan = (PreparedPlan)cache.get(cID);
		if(preparedPlan == null){
			preparedPlan = new PreparedPlan();
			cache.put(cID, preparedPlan);
		}
		return preparedPlan;
	}
	
	/**
	 * Clear the cahced plans for the given clientConn
	 * @param clientConn ClientConnection
	 */
	public synchronized void clear(String sessionId){
		ArgCheck.isNotNull(sessionId);
		//do not do anything
	}

	/**
	 * Clear all the cahced plans for all the clientConns
	 * @param clientConn ClientConnection
	 */
	public synchronized void clearAll(){
		cache.clear();
	}	
	
	static class CacheID{
		private String sessionId;
		private String sql;
		int hashCode;
		private boolean isPreparedBatchUpdate;
		
		CacheID(String sessionId, String sql, boolean isPreparedBatchUpdate){
			this.sessionId = sessionId;
			this.sql = sql;
			this.isPreparedBatchUpdate = isPreparedBatchUpdate;
			hashCode = HashCodeUtil.hashCode(HashCodeUtil.hashCode(0, sessionId), sql);
		}
		
		public boolean equals(Object obj){
	        if(obj == this) {
	            return true;
	        } else if(obj == null || ! (obj instanceof CacheID)) {
	            return false;
	        } else {
	        	CacheID that = (CacheID)obj;
	            return this.sessionId.equals(that.sessionId)
	            && this.isPreparedBatchUpdate == that.isPreparedBatchUpdate
					&& this.sql.equals(that.sql);
			}
		}
		
	    public int hashCode() {
	        return hashCode;
	    }
	}
	
	static class PreparedPlan{
		private ProcessorPlan plan;
		private Command command;
		private List refs;
		private AnalysisRecord analysisRecord;
		
		/**
		 * Return the ProcessorPlan.
		 */
		public ProcessorPlan getPlan(){
			return plan;
		}
		
		/**
		 * Return the plan description.
		 */
		public AnalysisRecord getAnalysisRecord(){
			return this.analysisRecord;
		}
		
		/**
		 * Return the Command .
		 */
		public Command getCommand(){
			return command;
		}
		
		/**
		 * Return the list of Reference.
		 */
		public List getReferences(){
			return refs;
		}
		
		/**
		 * Set the ProcessorPlan.
		 */
		public void setPlan(ProcessorPlan planValue){
			plan = planValue;
		}
		
		/**
		 * Set the plan description.
		 */
		public void setAnalysisRecord(AnalysisRecord analysisRecord){
            this.analysisRecord = analysisRecord;
		}
		
		/**
		 * Set the Command.
		 */
		public void setCommand(Command commandValue){
			command = commandValue;
		}
		
		/**
		 * Set the list of Reference.
		 */
		public void setReferences(List refsValue){
			refs = refsValue;
		}
					
	}

	//for testing purpose 
	int getSpaceUsed() {
		return cache.size();
	}
    int getSpaceAllowed() {
        return cache.getSpaceLimit();
    }
    
}
