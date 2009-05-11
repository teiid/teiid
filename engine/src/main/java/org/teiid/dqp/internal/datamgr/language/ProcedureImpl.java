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

package org.teiid.dqp.internal.datamgr.language;

import java.util.List;

import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.language.IParameter;
import org.teiid.connector.language.IProcedure;
import org.teiid.connector.language.IParameter.Direction;
import org.teiid.connector.metadata.runtime.Element;
import org.teiid.connector.metadata.runtime.Procedure;
import org.teiid.connector.visitor.framework.LanguageObjectVisitor;


public class ProcedureImpl extends BaseLanguageObject implements IProcedure {

    private String name;
    private List<IParameter> parameters;
    private Procedure metadataObject;
    
    public ProcedureImpl(String name, List<IParameter> parameters, Procedure metadataObject) {
        this.name = name;
        this.parameters = parameters;
        this.metadataObject = metadataObject;
    }
    
    /**
     * @see com.metamatrix.data.language.IExecute#getProcedureName()
     */
    public String getProcedureName() {
        return this.name;
    }

    /**
     * @see com.metamatrix.data.language.IExecute#getVariableValues()
     */
    public List<IParameter> getParameters() {
        return parameters;
    }

    /**
     * @see org.teiid.connector.language.ILanguageObject#acceptVisitor(com.metamatrix.data.visitor.LanguageObjectVisitor)
     */
    public void acceptVisitor(LanguageObjectVisitor visitor) {
        visitor.visit(this);
    }

    /* 
     * @see com.metamatrix.data.language.IExecute#setProcedureName(java.lang.String)
     */
    public void setProcedureName(String name) {
        this.name = name;
    }

    /* 
     * @see com.metamatrix.data.language.IExecute#setParameters(java.util.List)
     */
    public void setParameters(List<IParameter> parameters) {
        this.parameters = parameters;
    }

    @Override
    public Procedure getMetadataObject() {
    	return this.metadataObject;
    }

    public void setMetadataObject(Procedure metadataID) {
        this.metadataObject = metadataID;
    }
    
    public boolean equals(Object obj) {
        if(obj == this) {
            return true;
        }
        
        if(obj == null) {
            return false;
        }
        
        IProcedure proc = (IProcedure) obj;
        return getProcedureName().equalsIgnoreCase(proc.getProcedureName());
    }
    
    public Class<?>[] getResultSetColumnTypes() throws ConnectorException {
        for (IParameter param : parameters) {
            if(param.getDirection() == Direction.RESULT_SET){
                List<Element> columnMetadata = param.getMetadataObject().getChildren();

                int size = columnMetadata.size();
                Class<?>[] coulmnDTs = new Class[size];
                for(int i =0; i<size; i++ ){
                    coulmnDTs[i] = columnMetadata.get(i).getJavaType();
                }
                return coulmnDTs;
            }
        }
        return new Class[0];
    }

}
