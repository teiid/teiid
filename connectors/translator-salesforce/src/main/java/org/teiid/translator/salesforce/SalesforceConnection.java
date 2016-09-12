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
package org.teiid.translator.salesforce;

import java.util.Calendar;
import java.util.List;

import javax.resource.ResourceException;
import javax.resource.cci.Connection;

import org.teiid.translator.salesforce.execution.DataPayload;
import org.teiid.translator.salesforce.execution.DeletedResult;
import org.teiid.translator.salesforce.execution.UpdatedResult;

import com.sforce.async.BatchResult;
import com.sforce.async.JobInfo;
import com.sforce.async.OperationEnum;
import com.sforce.async.SObject;
import com.sforce.soap.partner.DescribeGlobalResult;
import com.sforce.soap.partner.DescribeSObjectResult;
import com.sforce.soap.partner.QueryResult;

public interface SalesforceConnection extends Connection {
	
	public static class BatchResultInfo {
		private String batchId;
		private String[] batchList;
		private int batchNum;

		public BatchResultInfo(String batchInfo) {
			this.batchId = batchInfo;
		}
		
		public String[] getBatchList() {
			return batchList;
		}
		
		public void setBatchList(String[] batchList) {
			this.batchList = batchList;
		}
		
		public int getAndIncrementBatchNum() {
			return batchNum++;
		}
		
		public String getBatchId() {
			return batchId;
		}
		
	}

	public QueryResult query(String queryString, int maxBatchSize, boolean queryAll) throws ResourceException;

	public QueryResult queryMore(String queryLocator, int batchSize) throws ResourceException;
	
	public boolean isValid();
	
	public int delete(String[] ids) throws ResourceException ;

	public int create(DataPayload data) throws ResourceException;

	public int update(List<DataPayload> updateDataList) throws ResourceException;

	public UpdatedResult getUpdated(String objectName, Calendar startCalendar, Calendar endCalendar) throws ResourceException;

	public DeletedResult getDeleted(String objectName, Calendar startCalendar, Calendar endCalendar) throws ResourceException;
	
	public com.sforce.soap.partner.sobject.SObject[] retrieve(String fieldList, String sObjectType, List<String> ids) throws ResourceException;
	
	public DescribeGlobalResult getObjects() throws ResourceException;
	
	public DescribeSObjectResult[] getObjectMetaData(String... objectName) throws ResourceException;
	
	public BatchResult[] getBulkResults(JobInfo job, List<String> ids) throws ResourceException;

	public void cancelBulkJob(JobInfo job) throws ResourceException;

	JobInfo closeJob(String jobId) throws ResourceException;

	String addBatch(List<SObject> payload, JobInfo job)
			throws ResourceException;

	JobInfo createBulkJob(String objectName, OperationEnum operation) throws ResourceException;

	Long getCardinality(String sobject) throws ResourceException;
	
	String getVersion();

	BatchResultInfo addBatch(String query, JobInfo job) throws ResourceException;

	List<List<String>> getBatchQueryResults(String id, BatchResultInfo info) throws ResourceException;

}
