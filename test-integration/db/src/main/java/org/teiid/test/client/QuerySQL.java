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
 * The QuerySQL represents a single sql statement to be executed for a given {@link QueryTest Test}.
 * The {@link #rowCnt} and {@link  #updateCnt}, when set, provide validation checks after the
 * execution of the query.  
 * @author vanhalbert
 *
 */
public class QuerySQL {
    
    private String sql = null;
    private Object[] parms;
    private int updateCnt=-1;
    private int rowCnt=-1;
    
    public int getRowCnt() {
        return rowCnt;
    }

    public void setRowCnt(int rowCnt) {
        this.rowCnt = rowCnt;
    }

    public int getUpdateCnt() {
        return updateCnt;
    }

    public void setUpdateCnt(int updateCnt) {
        this.updateCnt = updateCnt;
    }

    public QuerySQL(String sql, Object[] parms) {
	this.sql = sql;
	this.parms = parms;
    }
    
    public String getSql() {
        return sql;
    }

    public Object[] getParms() {
        return parms;
    }


}
