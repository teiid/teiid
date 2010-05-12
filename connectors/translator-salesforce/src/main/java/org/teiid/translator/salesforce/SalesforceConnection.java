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

import java.util.List;

import javax.xml.datatype.XMLGregorianCalendar;

import org.teiid.translator.ConnectorException;
import org.teiid.translator.salesforce.execution.DataPayload;
import org.teiid.translator.salesforce.execution.DeletedResult;
import org.teiid.translator.salesforce.execution.UpdatedResult;

import com.sforce.soap.partner.DescribeGlobalResult;
import com.sforce.soap.partner.DescribeSObjectResult;
import com.sforce.soap.partner.QueryResult;

public interface SalesforceConnection {

	public QueryResult query(String queryString, int maxBatchSize, Boolean queryAll) throws ConnectorException;

	public QueryResult queryMore(String queryLocator, int batchSize) throws ConnectorException;
	
	public boolean isAlive();
	
	public int delete(String[] ids) throws ConnectorException ;

	public int create(DataPayload data) throws ConnectorException;

	public int update(List<DataPayload> updateDataList) throws ConnectorException;

	public UpdatedResult getUpdated(String objectName, XMLGregorianCalendar startCalendar, XMLGregorianCalendar endCalendar) throws ConnectorException;

	public DeletedResult getDeleted(String objectName, XMLGregorianCalendar startCalendar, XMLGregorianCalendar endCalendar) throws ConnectorException;
	
	public QueryResult retrieve(String fieldList, String sObjectType, List<String> ids) throws ConnectorException;
	
	public DescribeGlobalResult getObjects() throws ConnectorException;
	
	public DescribeSObjectResult getObjectMetaData(String objectName) throws ConnectorException;
}
