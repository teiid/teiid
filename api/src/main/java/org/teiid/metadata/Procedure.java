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

package org.teiid.metadata;

import java.util.ArrayList;
import java.util.List;

import org.teiid.metadata.AbstractMetadataRecord.Modifiable;


/**
 * Represents Teiid and source procedures.  Can also represent a function with restrictions.
 * <br>
 * Parameter positions start with 1 for consistency with {@link BaseColumn}.
 * <br>
 * See also {@link FunctionMethod}
 */
public class Procedure extends AbstractMetadataRecord implements Modifiable {

    private static final long serialVersionUID = 7714869437683360834L;

    public enum Type {
        Function,
        UDF,
        StoredProc,
        StoredQuery
    }

    public static final int AUTO_UPDATECOUNT = -1;

    private boolean isFunction;
    private boolean isVirtual;
    private int updateCount = AUTO_UPDATECOUNT;
    private List<ProcedureParameter> parameters = new ArrayList<ProcedureParameter>(2);
    private ColumnSet<Procedure> resultSet;
    private volatile String queryPlan;

    private Schema parent;
    private volatile transient long lastModified;

    public void setParent(Schema parent) {
        this.parent = parent;
    }

    public boolean isFunction() {
        return isFunction;
    }

    public boolean isVirtual() {
        return this.isVirtual;
    }

    public Type getType() {
        if (isFunction()) {
            if (isVirtual()) {
                return Type.UDF;
            }
            return Type.Function;
        }
        if (isVirtual()) {
            return Type.StoredQuery;
        }
        return Type.StoredProc;
    }

    public int getUpdateCount() {
        return this.updateCount;
    }

    public List<ProcedureParameter> getParameters() {
        return parameters;
    }

    public ProcedureParameter getParameterByName(String param) {
        for(ProcedureParameter p: this.parameters) {
            if (p.getName().equals(param)) {
                return p;
            }
        }
        return null;
    }

    public void setParameters(List<ProcedureParameter> parameters) {
        this.parameters = parameters;
    }

    public String getQueryPlan() {
        return queryPlan;
    }

    public void setQueryPlan(String queryPlan) {
        this.queryPlan = queryPlan;
    }

    /**
     * @param b
     */
    public void setFunction(boolean b) {
        isFunction = b;
    }

    /**
     * @param b
     */
    public void setVirtual(boolean b) {
        isVirtual = b;
    }

    public void setUpdateCount(int count) {
        this.updateCount = count;
    }

    public void setResultSet(ColumnSet<Procedure> resultSet) {
        this.resultSet = resultSet;
        if (resultSet != null) {
            resultSet.setParent(this);
        }
    }

    public ColumnSet<Procedure> getResultSet() {
        return resultSet;
    }

    @Override
    public Schema getParent() {
        return parent;
    }

    public long getLastModified() {
        return lastModified;
    }

    public void setLastModified(long lastModified) {
        this.lastModified = lastModified;
    }

}