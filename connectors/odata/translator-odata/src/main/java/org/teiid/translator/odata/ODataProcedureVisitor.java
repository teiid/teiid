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
package org.teiid.translator.odata;

import java.util.ArrayList;
import java.util.List;

import org.teiid.language.Argument;
import org.teiid.language.Argument.Direction;
import org.teiid.language.Call;
import org.teiid.language.SQLConstants.Tokens;
import org.teiid.language.visitor.HierarchyVisitor;
import org.teiid.metadata.Column;
import org.teiid.metadata.Procedure;
import org.teiid.metadata.ProcedureParameter;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.metadata.Schema;
import org.teiid.metadata.Table;
import org.teiid.translator.TranslatorException;
import org.teiid.util.WSUtil;

public class ODataProcedureVisitor extends HierarchyVisitor {
    protected ODataExecutionFactory executionFactory;
    protected RuntimeMetadata metadata;
    protected ArrayList<TranslatorException> exceptions = new ArrayList<TranslatorException>();
    private StringBuilder buffer = new StringBuilder();
    private String method = "GET"; //$NON-NLS-1$
    private String returnEntityTypeName;
    private boolean returnsTable;
    private Procedure procedure;
    private String returnType;
    private Class<?> returnTypeClass;
    private boolean isComplexReturnType;
    private Table entity;
    private List<Column> returnColumns;

    public ODataProcedureVisitor(ODataExecutionFactory executionFactory,
            RuntimeMetadata metadata) {
        this.executionFactory = executionFactory;
        this.metadata = metadata;
    }

    @Override
    public void visit(Call obj) {
        Procedure proc = obj.getMetadataObject();
        this.method = proc.getProperty(ODataMetadataProcessor.HTTP_METHOD, false);

        this.procedure = proc;
        this.buffer.append(obj.getProcedureName());
        final List<Argument> params = obj.getArguments();
        if (params != null && params.size() != 0) {
            this.buffer.append("?"); //$NON-NLS-1$
            Argument param = null;
            StringBuilder temp = new StringBuilder();
            for (int i = 0; i < params.size(); i++) {
                param = params.get(i);
                if (param.getDirection() == Direction.IN || param.getDirection() == Direction.INOUT) {
                    if (i != 0) {
                        this.buffer.append("&"); //$NON-NLS-1$
                    }
                    this.buffer.append(WSUtil.httpURLEncode(param.getMetadataObject().getName()));
                    this.buffer.append(Tokens.EQ);
                    this.executionFactory.convertToODataInput(param.getArgumentValue(), temp);
                    this.buffer.append(WSUtil.httpURLEncode(temp.toString()));
                    temp.setLength(0);
                }
            }
        }

        // this is collection based result
        if(proc.getResultSet() != null) {
            this.returnsTable = true;
            this.returnEntityTypeName = proc.getProperty(ODataMetadataProcessor.ENTITY_TYPE, false);
            this.entity = getTableWithEntityType(proc.getParent(), returnEntityTypeName);
               this.isComplexReturnType = ( this.entity == null);
               this.returnColumns = proc.getResultSet().getColumns();
        }
        else {
            for (ProcedureParameter param:proc.getParameters()) {
                if (param.getType().equals(ProcedureParameter.Type.ReturnValue)) {
                    this.returnType = param.getRuntimeType();
                    this.returnTypeClass = param.getJavaType();
                }
            }
        }
    }

    Table getTableWithEntityType(Schema schema, String entityType) {
        for (Table t:schema.getTables().values()) {
            if (entityType.equals(t.getProperty(ODataMetadataProcessor.ENTITY_TYPE, false))) {
                return t;
            }
        }
        return null;
    }

    public String buildURL() {
        return this.buffer.toString();
    }

    public String getMethod() {
        return this.method;
    }

    public String getReturnEntityTypeName() {
        return this.returnEntityTypeName;
    }

    public Table getTable() {
        return this.entity;
    }

    public boolean hasCollectionReturn() {
        return this.returnsTable;
    }

    public Column[] getReturnColumns() {
        return this.returnColumns.toArray(new Column[this.returnColumns.size()]);
    }

    public boolean isReturnComplexType() {
        return this.isComplexReturnType;
    }

    public Procedure getProcedure() {
        return this.procedure;
    }

    public String getReturnType() {
        return this.returnType;
    }

    public Class<?>getReturnTypeClass() {
        return this.returnTypeClass;
    }

}
