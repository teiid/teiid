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
package org.teiid.test.client.results;

import java.io.Serializable;

import org.teiid.test.client.TestResult;

/**
 * ATestResultStat
 *
 * <p>A per-query set of result stats.</p>
 */
@SuppressWarnings("nls")
public class TestResultStat implements TestResult, Serializable {
  
    /**
	 * @since
	 */
	private static final long serialVersionUID = 6670189391506288744L;
	protected int resultStatus = -1;
    protected String queryID;
    protected String querySetID;
    protected String errorMsg;
    protected String query;
    protected Throwable error=null;
    
    private long beginTS;
    private long endTS;
    
    private String errorFile;

    
    public TestResultStat(final String querySetID, final String queryID, String query) {
    	this.querySetID = querySetID;
    	this.queryID = queryID;
        this.query = query;

    }

    public TestResultStat(final String querySetID, final String queryID, String query, final int resultStatus, long beginTS, long endTS, final Throwable error) {
    	this.querySetID = querySetID;
    	this.queryID = queryID;
        this.resultStatus = resultStatus;
        this.beginTS = beginTS;
        this.endTS = endTS;
        this.error = error;

    }
    
    public TestResultStat(final String querySetID, final String queryID, String query, final int resultStatus, long beginTS, long endTS, final Throwable error, String errorFile) {
    	this.querySetID = querySetID;
    	this.queryID = queryID;
        this.resultStatus = resultStatus;
        this.beginTS = beginTS;
        this.endTS = endTS;
        this.error = error;
        
        this.errorFile = errorFile;
    }

    public String getResultStatusString() {
        switch (resultStatus) {
            case RESULT_STATE.TEST_SUCCESS:
                return RESULT_STATE_STRING.PASS;
            case RESULT_STATE.TEST_EXCEPTION:
                return RESULT_STATE_STRING.FAIL;
            case RESULT_STATE.TEST_EXPECTED_EXCEPTION:
                return RESULT_STATE_STRING.FAIL_EXPECTED_EXCEPTION;
        }
        return RESULT_STATE_STRING.UNKNOWN;
    }

    

	@Override
	public String getQuerySetID() {
		// TODO Auto-generated method stub
		return this.querySetID;
	}

	public String getQueryID() {
        return queryID;
    }
    
    public String getQuery() {
        return query;
    }

    public int getStatus() {
        return resultStatus;
    }
    
    public void setStatus(int endStatus) {
	resultStatus = endStatus;
    }

    public String getExceptionMsg() {
        return (this.errorMsg != null ? this.errorMsg : ( error != null ? error.getMessage() : ""));
    }
    
    public void setException(Throwable error){
	this.error = error;
    }
    
    public  void setExceptionMessage(String errorMsg) {
	this.errorMsg = errorMsg;
	
    }
    
    public Throwable getException() {
	return this.error;
    }
    
    public long getBeginTS() {
    	return beginTS;
   	
    }
    
    public void setBeginTS(long beginTS) {
	this.beginTS = beginTS;
    }
    
    public long getEndTS() {
    	return endTS;
    }
    
    public void setEndTS(long endts) {
	this.endTS = endts;
    }
    
    /**
     * @return Returns the errorfile.
     */
    public String getErrorfile() {
        return errorFile;
    }
    
    public void setErrorFile(String errorfile) {
	this.errorFile = errorfile;
    }
    
    

    
    
}
