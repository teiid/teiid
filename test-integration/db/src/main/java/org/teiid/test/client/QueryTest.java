/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
