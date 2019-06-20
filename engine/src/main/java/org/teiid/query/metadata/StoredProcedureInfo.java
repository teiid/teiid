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

package org.teiid.query.metadata;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.teiid.client.metadata.ParameterInfo;
import org.teiid.metadata.Procedure;
import org.teiid.query.mapping.relational.QueryNode;
import org.teiid.query.sql.lang.SPParameter;

/**
* This class encapsulates everything needed to pass between runtime metadata
* and the QueryResolver via the facades
*/

public class StoredProcedureInfo implements Serializable {

    /** Constant identifying an IN parameter */
    public static final int IN = ParameterInfo.IN;

    /** Constant identifying an OUT parameter */
    public static final int OUT = ParameterInfo.OUT;

    /** Constant identifying an INOUT parameter */
    public static final int INOUT = ParameterInfo.INOUT;

    /** Constant identifying a RETURN parameter */
    public static final int RETURN_VALUE = ParameterInfo.RETURN_VALUE;

    /** Constant identifying a RESULT SET parameter */
    public static final int RESULT_SET = ParameterInfo.RESULT_SET;

    private Object modelID;
    private Object procedureID;
    private List<SPParameter> parameters = new ArrayList<SPParameter>();
    private String callableName;
    private QueryNode query;
    private int updateCount = Procedure.AUTO_UPDATECOUNT;

    public String getProcedureCallableName(){
        return this.callableName;
    }
    public void setProcedureCallableName(String callableName){
        this.callableName = callableName;
    }
    public Object getModelID(){
        return this.modelID;
    }
    public void setModelID(Object modelID){
        this.modelID = modelID;
    }
    public Object getProcedureID(){
        return this.procedureID;
    }
    public void setProcedureID(Object procedureID){
        this.procedureID = procedureID;
    }
    public List<SPParameter> getParameters(){
        return this.parameters;
    }
    public void setParameters(List<SPParameter> parameters){
        this.parameters = parameters;
    }

    public void addParameter(SPParameter parameter){
        this.parameters.add(parameter);
    }

    public QueryNode getQueryPlan(){
        return this.query;
    }
    public void setQueryPlan(QueryNode queryNode){
        this.query = queryNode;
    }

    public boolean returnsResultSet() {
        for (SPParameter parameter : parameters) {
            if (parameter.getParameterType() == ParameterInfo.RESULT_SET) {
                return true;
            }
        }
        return false;
    }

    public boolean returnsResultParameter() {
        for (SPParameter parameter : parameters) {
            if (parameter.getParameterType() == ParameterInfo.RETURN_VALUE) {
                return true;
            }
        }
        return false;
    }

    public int getUpdateCount() {
        return updateCount;
    }

    public void setUpdateCount(int updateCount) {
        this.updateCount = updateCount;
    }

}