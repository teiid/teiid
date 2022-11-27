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
package org.teiid.olingo.service;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.olingo.commons.api.data.Parameter;
import org.apache.olingo.commons.api.edm.EdmOperation;
import org.apache.olingo.commons.api.edm.EdmParameter;
import org.apache.olingo.commons.api.edm.EdmReturnType;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.apache.olingo.server.api.deserializer.DeserializerException;
import org.apache.olingo.server.api.uri.UriParameter;
import org.apache.olingo.server.core.requests.ActionRequest;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.types.BlobImpl;
import org.teiid.core.types.BlobType;
import org.teiid.core.types.ClobImpl;
import org.teiid.core.types.ClobType;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.InputStreamFactory;
import org.teiid.core.types.JDBCSQLTypeInfo;
import org.teiid.core.types.SQLXMLImpl;
import org.teiid.core.types.XMLType;
import org.teiid.metadata.MetadataStore;
import org.teiid.metadata.Procedure;
import org.teiid.metadata.ProcedureParameter;
import org.teiid.metadata.Schema;
import org.teiid.odata.api.ProcedureReturnType;
import org.teiid.odata.api.SQLParameter;
import org.teiid.olingo.common.ODataTypeManager;
import org.teiid.olingo.service.TeiidServiceHandler.OperationParameterValueProvider;
import org.teiid.query.sql.visitor.SQLStringVisitor;

public class ProcedureSQLBuilder {
    final private List<SQLParameter> sqlParameters;
    private Procedure procedure;
    private ProcedureReturn procedureReturn;
    private OperationParameterValueProvider parameterValueProvider;

    static class ProcedureReturn implements ProcedureReturnType {
        private EdmReturnType type;
        private Integer sqlType = null;
        private boolean hasResultSet;

        public ProcedureReturn(EdmReturnType type, Integer sqlType, boolean hasResultSet) {
            this.type = type;
            this.sqlType = sqlType;
            this.hasResultSet = hasResultSet;
        }

        @Override
        public EdmReturnType getReturnType() {
            return type;
        }

        @Override
        public boolean hasResultSet() {
            return this.hasResultSet;
        }

        @Override
        public Integer getSqlType() {
            return sqlType;
        }
    }

    public ProcedureSQLBuilder(MetadataStore metadata, EdmOperation edmOperation,
            OperationParameterValueProvider parameterProvider, ArrayList<SQLParameter> params)
            throws TeiidProcessingException {
        FullQualifiedName fqn = edmOperation.getFullQualifiedName();
        String withoutVDB = fqn.getNamespace().substring(fqn.getNamespace().lastIndexOf('.')+1);
        Schema schema = metadata.getSchema(withoutVDB);

        this.procedure =  schema.getProcedure(edmOperation.getName());
        this.parameterValueProvider = parameterProvider;
        this.sqlParameters = params;
        visit(edmOperation);
    }

    /**
     *
     * @return the {@link ProcedureReturnType} or null if none is found
     */
    public ProcedureReturn getReturn() {
        return this.procedureReturn;
    }

    private void visit(EdmOperation edmOperation) throws TeiidProcessingException {
        if (edmOperation.getReturnType() != null) {
            visit(edmOperation.getReturnType());
        }

        for (String parameterName : edmOperation.getParameterNames()) {
            visit(edmOperation.getParameter(parameterName));
        }
    }

    private boolean hasResultSet() {
        return this.procedure.getResultSet() != null;
    }

    private void visit(EdmReturnType returnType) {
        if (hasResultSet()) {
            // this is complex type
            this.procedureReturn = new ProcedureReturn(returnType, null, true);
        }
        else {
            // must not be null
            ProcedureParameter parameter = getReturnParameter();
            Class<?> teiidType = DataTypeManager.getDataTypeClass(parameter.getRuntimeType());
            Integer sqlType = JDBCSQLTypeInfo.getSQLType(DataTypeManager.getDataTypeName(teiidType));
            this.procedureReturn = new ProcedureReturn(returnType, sqlType, false);
        }
    }

    private ProcedureParameter getReturnParameter() {
        for (ProcedureParameter parameter: this.procedure.getParameters()) {
            if (parameter.getType().equals(ProcedureParameter.Type.ReturnValue)) {
                return parameter;
            }
        }
        return null;
    }

    private void visit(EdmParameter edmParameter) throws TeiidProcessingException {
        Class<?> runtimeType = resolveParameterType(edmParameter.getName());
        Integer sqlType = JDBCSQLTypeInfo.getSQLType(DataTypeManager.getDataTypeName(runtimeType));
        Object value = this.parameterValueProvider.getValue(edmParameter, runtimeType);
        this.sqlParameters.add(new SQLParameter(edmParameter.getName(), value, sqlType));
    }

    public String buildProcedureSQL() {

        StringBuilder sql = new StringBuilder();

        if (procedureReturn == null || procedureReturn.hasResultSet()) {
            sql.append("{"); //$NON-NLS-1$
        }
        else {
            sql.append("{? = "); //$NON-NLS-1$
        }

        // fully qualify the procedure name
        sql.append("call ").append(SQLStringVisitor.escapeSinglePart(this.procedure.getFullName())); //$NON-NLS-1$
        sql.append("("); //$NON-NLS-1$

        boolean first = true;
        for (SQLParameter parameter:this.sqlParameters) {
            if (!first) {
                sql.append(","); //$NON-NLS-1$
            }
            first = false;
            sql.append(SQLStringVisitor.escapeSinglePart(parameter.getName())).append("=>?"); //$NON-NLS-1$

        }
        sql.append(")"); //$NON-NLS-1$
        sql.append("}"); //$NON-NLS-1$
        return sql.toString();
    }

    private Class<?> resolveParameterType(String parameterName) {
        for (ProcedureParameter pp : this.procedure.getParameters()) {
            if (pp.getName().equalsIgnoreCase(parameterName)) {
                return DataTypeManager.getDataTypeClass(pp.getRuntimeType());
            }
        }
        return null;
    }

    static class ActionParameterValueProvider implements OperationParameterValueProvider {
        private InputStream payload;
        private boolean alreadyConsumed;
        private ActionRequest actionRequest;
        private List<Parameter> parameters;

        public ActionParameterValueProvider(InputStream payload, ActionRequest actionRequest) {
            this.payload = payload;
            this.actionRequest = actionRequest;
        }

        @Override
        public Object getValue(EdmParameter edmParameter, Class<?> runtimeType)
                throws TeiidProcessingException {
            if (!this.alreadyConsumed) {
                this.alreadyConsumed = true;
                if (DataTypeManager.isLOB(runtimeType)) {
                    InputStreamFactory isf = new InputStreamFactory() {
                        @Override
                        public InputStream getInputStream() throws IOException {
                            return payload;
                        }
                    };
                    if (runtimeType.isAssignableFrom(XMLType.class)) {
                        return new SQLXMLImpl(isf);
                    } else if (runtimeType.isAssignableFrom(ClobType.class)) {
                        return new ClobImpl(isf, -1);
                    } else if (runtimeType.isAssignableFrom(BlobType.class)) {
                        return new BlobImpl(isf);
                    }
                } else {
                    try {
                        this.parameters = this.actionRequest.getParameters();
                    } catch (DeserializerException e) {
                        throw new TeiidProcessingException(e);
                    }
                }
            }

            if (this.parameters != null && !this.parameters.isEmpty()) {
                for (Parameter parameter : this.parameters) {
                    if (parameter.getName().equals(edmParameter.getName())) {
                        // In Teiid one can only pass simple literal values, not complex
                        // types, no complex parsing required. And LOBs can not be inlined
                        // for Function
                        return parameter.getValue();
                    }
                }
            }
            return null;
        }
    }

    static class FunctionParameterValueProvider implements OperationParameterValueProvider {
        private List<UriParameter> parameters;

        public FunctionParameterValueProvider(List<UriParameter> parameters) {
            this.parameters = parameters;
        }

        @Override
        public Object getValue(EdmParameter edmParameter, Class<?> runtimeType)
                throws TeiidProcessingException {
            for (UriParameter parameter : this.parameters) {
                if (parameter.getName().equals(edmParameter.getName())) {
                    // In Teiid one can only pass simple literal values, not complex
                    // types, no complex parsing required. And LOBs can not be inlined
                    // for Function
                    return ODataTypeManager.parseLiteral(edmParameter,
                            runtimeType, parameter.getText());
                }
            }
            return null;
        }
    }
}
