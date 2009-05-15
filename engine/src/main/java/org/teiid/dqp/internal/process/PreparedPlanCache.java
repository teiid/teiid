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

package org.teiid.dqp.internal.process;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.metamatrix.core.util.HashCodeUtil;
import com.metamatrix.core.util.LRUCache;
import com.metamatrix.query.analysis.AnalysisRecord;
import com.metamatrix.query.parser.ParseInfo;
import com.metamatrix.query.processor.ProcessorPlan;
import com.metamatrix.query.sql.lang.Command;
import com.metamatrix.query.sql.symbol.Reference;
import com.metamatrix.vdb.runtime.VDBKey;

/**
 * This class is used to cache plan and related objects for prepared statement
 */
public class PreparedPlanCache {
	public static final int DEFAULT_MAX_SIZE_TOTAL = 250;

	private Map<CacheID, PreparedPlan> cache;
	private int maxSize;
	
	PreparedPlanCache(){
		this(DEFAULT_MAX_SIZE_TOTAL);
	}
	
	PreparedPlanCache (int maxSize ){
		if(maxSize < 0){
			maxSize = DEFAULT_MAX_SIZE_TOTAL;
		}
		this.maxSize = maxSize;
		cache = Collections.synchronizedMap(new LRUCache<CacheID, PreparedPlan>(maxSize));
	}	
	
	/**
	 * Return the PreparedPlan for the given session and SQl query
	 * @param sql SQL query string
	 * @param session ClientConnection
	 * @return PreparedPlan for the given clientConn and SQl query. Null if not exist.
	 */
	public PreparedPlan getPreparedPlan(CacheID id){
		id.setSessionId(id.originalSessionId);
		PreparedPlan result = cache.get(id);
		if (result == null) {
			id.setSessionId(null);
		}
		return cache.get(id);
	}
	
	/**
	 * Create PreparedPlan for the given clientConn and SQl query
	 */
	public void putPreparedPlan(CacheID id, boolean sessionSpecific, PreparedPlan plan){
		if (sessionSpecific) {
			id.setSessionId(id.originalSessionId);
		} else {
			id.setSessionId(null);
		}
		this.cache.put(id, plan);
	}
	
	/**
	 * Clear all the cached plans for all the clientConns
	 * @param clientConn ClientConnection
	 */
	public void clearAll(){
		cache.clear();
	}	
	
	static class CacheID{
		private String sql;
		private VDBKey vdbInfo;
		private ParseInfo pi;
		private String sessionId;
		private String originalSessionId;
		private int hashCode;
				
		CacheID(DQPWorkContext context, ParseInfo pi, String sql){
			this.sql = sql;
			this.vdbInfo = new VDBKey(context.getVdbName(), context.getVdbVersion());
			this.pi = pi;
			this.originalSessionId = context.getConnectionID();
		}
		
		private void setSessionId(String sessionId) {
			this.sessionId = sessionId;
			hashCode = HashCodeUtil.hashCode(HashCodeUtil.hashCode(HashCodeUtil.hashCode(HashCodeUtil.hashCode(0, vdbInfo), sql), pi), sessionId);
		}
						
		public boolean equals(Object obj){
	        if(obj == this) {
	            return true;
	        } 
	        if(! (obj instanceof CacheID)) {
	            return false;
	        } 
        	CacheID that = (CacheID)obj;
            return this.pi.equals(that.pi) && this.vdbInfo.equals(that.vdbInfo) && this.sql.equals(that.sql) 
            	&& ((this.sessionId == null && that.sessionId == null) || this.sessionId.equals(that.sessionId));
		}
		
	    public int hashCode() {
	        return hashCode;
	    }
	}
	
	static class PreparedPlan{
		private ProcessorPlan plan;
		private Command command;
		private Command rewritenCommand;
		private List<Reference> refs;
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
		public List<Reference> getReferences(){
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
		public void setReferences(List<Reference> refsValue){
			refs = refsValue;
		}
		
		public void setRewritenCommand(Command rewritenCommand) {
			this.rewritenCommand = rewritenCommand;
		}
		
		public Command getRewritenCommand() {
			return rewritenCommand;
		}
					
	}

	//for testing purpose 
	int getSpaceUsed() {
		return cache.size();
	}
    int getSpaceAllowed() {
        return maxSize;
    }
    
}
