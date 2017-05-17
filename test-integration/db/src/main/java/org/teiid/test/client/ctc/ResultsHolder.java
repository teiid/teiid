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
package org.teiid.test.client.ctc;

import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;



/**
 * ResultsHolder
 * <p>
 * Data structure. Holder of expected resutls and metadata.
 * </p>
 */
public class ResultsHolder {

    // either TagNames.Elements.QUERY_RESULTS or TagNames.Elements.EXCEPTION
    private String resultType;

    // Identifier
    private String queryID;
    
    // The SQl query if available.
    private String query;

    // Query Results
    private List rows;
    private List types;
    private List identifiers;

    // Exception
    private String exceptionClassName;
    private String exceptionMsg;

    public ResultsHolder(final String type) {
        this.resultType = type;
    }

    public String getQueryID() {
        return queryID;
    }

    public void setQueryID(final String queryID) {
        this.queryID = queryID;
    }

    /**
     * @return Returns the query.
     */
    public String getQuery() {
        return query;
    }
    /**
     * @param query The query to set.
     */
    public void setQuery(String query) {
        this.query = query;
    }

    public boolean isResult() {
        return resultType.equals(TagNames.Elements.QUERY_RESULTS);
    }

    public boolean isException() {
        return resultType.equals(TagNames.Elements.EXCEPTION);
    }

    public String getResultType() {
        return resultType;
    }

    public void setResultType(final String resultType) {
        this.resultType = resultType;
    }

    public List getRows() {
        return (rows == null ? new ArrayList() : rows);
    }

    public void setRows(final List rows) {
        this.rows = rows;
    }

    public List getTypes() {
        return types;
    }

    public void setTypes(final List types) {
        this.types = types;
    }

    public List getIdentifiers() {
        return identifiers;
    }

    public void setIdentifiers(final List identifiers) {
        this.identifiers = identifiers;
    }

    public String getExceptionClassName() {
        return exceptionClassName;
    }

    public void setExceptionClassName(final String className) {
        this.exceptionClassName = className;
    }

    public String getExceptionMsg() {
        return exceptionMsg;
    }

    public void setExceptionMsg(final String msg) {
        this.exceptionMsg = msg;
    }

    public boolean hasRows() {
        return (rows != null && rows.size() > 0);
    }

    public String toString() {
        StringBuffer buf = new StringBuffer();
        buf.append("ResultsHolder... \n"); //$NON-NLS-1$
        if (isResult()) {
	        for (int i = 0; i < this.identifiers.size(); i++) {
	            buf.append("["); //$NON-NLS-1$
	            buf.append(this.identifiers.get(i));
	            buf.append(" - "); //$NON-NLS-1$
	            buf.append(this.types.get(i));
	            buf.append("] "); //$NON-NLS-1$
	        }
	        buf.append("\n"); //$NON-NLS-1$
	        Iterator rowItr = this.rows.iterator();
	        int i = 1;
	        while (rowItr.hasNext()) {
	            buf.append(i++);
	            buf.append(": "); //$NON-NLS-1$
	            buf.append(rowItr.next());
	            buf.append("\n"); //$NON-NLS-1$
	        }
        }
        else {
            buf.append(getExceptionClassName()).append(":").append(getExceptionMsg()); //$NON-NLS-1$            
        }
        return buf.toString();
    }    
}