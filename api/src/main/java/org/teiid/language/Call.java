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

package org.teiid.language;

import java.util.List;

import org.teiid.language.visitor.LanguageObjectVisitor;
import org.teiid.metadata.Column;
import org.teiid.metadata.ColumnSet;
import org.teiid.metadata.Procedure;
import org.teiid.metadata.ProcedureParameter;
import org.teiid.metadata.ProcedureParameter.Type;

/**
 * Represents a procedural execution (such as a stored procedure).
 */
public class Call extends BaseLanguageObject implements Command, MetadataReference<Procedure>, TableReference {

    private String name;
    private List<Argument> arguments;
    private Procedure metadataObject;
    private Class<?> returnType;
    private boolean tableReference;

    public Call(String name, List<Argument> parameters, Procedure metadataObject) {
        this.name = name;
        this.arguments = parameters;
        this.metadataObject = metadataObject;
    }

    /**
     * Get the return type
     * @return the return parameter type or null if not expecting a return value
     */
    public Class<?> getReturnType() {
        return returnType;
    }

    public void setReturnType(Class<?> returnType) {
        this.returnType = returnType;
    }

    public String getProcedureName() {
        return this.name;
    }

    public List<Argument> getArguments() {
        return arguments;
    }

    public void acceptVisitor(LanguageObjectVisitor visitor) {
        visitor.visit(this);
    }

    public void setProcedureName(String name) {
        this.name = name;
    }

    public void setArguments(List<Argument> parameters) {
        this.arguments = parameters;
    }

    @Override
    public Procedure getMetadataObject() {
        return this.metadataObject;
    }

    public ProcedureParameter getReturnParameter() {
        for (ProcedureParameter param : this.metadataObject.getParameters()) {
            if (param.getType() == Type.ReturnValue) {
                return param;
            }
        }
        return null;
    }

    /**
     * @return the result set types or a zero length array if no result set is returned
     */
    public Class<?>[] getResultSetColumnTypes() {
        ColumnSet<Procedure> resultSet = this.metadataObject.getResultSet();
        if (resultSet == null) {
            return new Class[0];
        }
        List<Column> columnMetadata = resultSet.getColumns();
        int size = columnMetadata.size();
        Class<?>[] coulmnDTs = new Class[size];
        for(int i =0; i<size; i++ ){
            coulmnDTs[i] = columnMetadata.get(i).getJavaType();
        }
        return coulmnDTs;
    }

    public boolean isTableReference() {
        return tableReference;
    }

    public void setTableReference(boolean tableReference) {
        this.tableReference = tableReference;
    }

}
