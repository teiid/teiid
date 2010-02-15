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
package org.teiid.test.client;

/**
 * The QueryTest represents a test to be executed.  This test can consist of 
 * one or more {@link QuerySQL SQL} queries required to perform the test.  
 * 
 * @author vanhalbert
 *
 */
public class QueryTest  {
    
    private QuerySQL[] queries;
    private String querySetID;
    private String queryID;
    private boolean isException=false;
    private String queryScenarioID;
    
    public QueryTest(String queryScenarioID, String queryID, String querySetID, QuerySQL[] queries, boolean isException) {
	this.queryID = queryID;
	this.queries = queries;
	this.isException = isException;
	this.querySetID = querySetID;
	this.queryScenarioID = queryScenarioID;
    }

    public QuerySQL[] getQueries() {
        return queries;
    }

    public String getQueryID() {
        return queryID;
    }

    public boolean isException() {
	return this.isException;
    }
    
    public String geQuerySetID() {
	return this.querySetID;
    }
    
    public String getQueryScenarioID() {
	return this.queryScenarioID;
    }

}
