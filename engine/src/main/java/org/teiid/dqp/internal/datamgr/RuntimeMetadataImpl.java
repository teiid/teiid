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

/*
 */
package org.teiid.dqp.internal.datamgr;

import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.util.ArgCheck;
import org.teiid.metadata.AbstractMetadataRecord;
import org.teiid.metadata.Column;
import org.teiid.metadata.Procedure;
import org.teiid.metadata.ProcedureParameter;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.metadata.Table;
import org.teiid.query.QueryPlugin;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.StoredProcedureInfo;
import org.teiid.query.metadata.TempMetadataAdapter;
import org.teiid.query.sql.lang.SPParameter;
import org.teiid.translator.TranslatorException;


/**
 */
public class RuntimeMetadataImpl implements RuntimeMetadata {
    private QueryMetadataInterface metadata;

    public RuntimeMetadataImpl(QueryMetadataInterface metadata){
        ArgCheck.isNotNull(metadata);
        this.metadata = metadata.getDesignTimeMetadata();
    }

    @Override
    public Column getColumn(String schema, String table, String name)
            throws TranslatorException {
        return getColumn(schema + AbstractMetadataRecord.NAME_DELIM_CHAR + table + AbstractMetadataRecord.NAME_DELIM_CHAR + name);
    }

    @Override
    public Procedure getProcedure(String schema, String name)
            throws TranslatorException {
        return getProcedure(schema + AbstractMetadataRecord.NAME_DELIM_CHAR + name);
    }

    @Override
    public Table getTable(String schema, String name)
            throws TranslatorException {
        return getTable(schema + AbstractMetadataRecord.NAME_DELIM_CHAR + name);
    }

    @Override
    public Column getColumn(String fullName) throws TranslatorException {
        try {
            Object metadataId = metadata.getElementID(fullName);
            return getElement(metadataId);
        } catch (QueryMetadataException e) {
             throw new TranslatorException(QueryPlugin.Event.TEIID30464, e);
        } catch (TeiidComponentException e) {
             throw new TranslatorException(QueryPlugin.Event.TEIID30465, e);
        }
    }

    public Column getElement(Object elementId) {
        if (elementId instanceof Column) {
            return (Column)elementId;
        }
        return null;
    }

    @Override
    public Table getTable(String fullName) throws TranslatorException {
        try {
            Object groupId = metadata.getGroupID(fullName);
            return getGroup(groupId);
        } catch (QueryMetadataException e) {
             throw new TranslatorException(QueryPlugin.Event.TEIID30466, e);
        } catch (TeiidComponentException e) {
             throw new TranslatorException(QueryPlugin.Event.TEIID30467, e);
        }
    }

    public Table getGroup(Object groupId) throws QueryMetadataException, TeiidComponentException {
        groupId = TempMetadataAdapter.getActualMetadataId(groupId);
        if (groupId instanceof Table && !metadata.isVirtualGroup(groupId)) {
            return (Table)groupId;
        }
        return null;
    }

    @Override
    public Procedure getProcedure(String fullName) throws TranslatorException {
        try {
            StoredProcedureInfo sp = metadata.getStoredProcedureInfoForProcedure(fullName);
            return getProcedure(sp);
        } catch (QueryMetadataException e) {
             throw new TranslatorException(QueryPlugin.Event.TEIID30468, e);
        } catch (TeiidComponentException e) {
             throw new TranslatorException(QueryPlugin.Event.TEIID30469, e);
        }
    }

    public Procedure getProcedure(StoredProcedureInfo sp) {
        if (sp.getProcedureID() instanceof Procedure) {
            return (Procedure)sp.getProcedureID();
        }
        return null;
    }

    public ProcedureParameter getParameter(SPParameter param) {
        if (param.getMetadataID() instanceof ProcedureParameter) {
            return (ProcedureParameter)param.getMetadataID();
        }
        return null;
    }

    public byte[] getBinaryVDBResource(String resourcePath) throws TranslatorException {
        try {
            return metadata.getBinaryVDBResource(resourcePath);
        } catch (QueryMetadataException e) {
             throw new TranslatorException(QueryPlugin.Event.TEIID30470, e);
        } catch (TeiidComponentException e) {
             throw new TranslatorException(QueryPlugin.Event.TEIID30471, e);
        }
    }

    public String getCharacterVDBResource(String resourcePath) throws TranslatorException {
        try {
            return metadata.getCharacterVDBResource(resourcePath);
        } catch (QueryMetadataException e) {
             throw new TranslatorException(QueryPlugin.Event.TEIID30472, e);
        } catch (TeiidComponentException e) {
             throw new TranslatorException(QueryPlugin.Event.TEIID30473, e);
        }
    }

    public String[] getVDBResourcePaths() throws TranslatorException {
        try {
            return metadata.getVDBResourcePaths();
        } catch (QueryMetadataException e) {
             throw new TranslatorException(QueryPlugin.Event.TEIID30474, e);
        } catch (TeiidComponentException e) {
             throw new TranslatorException(QueryPlugin.Event.TEIID30475, e);
        }
    }

    public QueryMetadataInterface getMetadata() {
        return metadata;
    }

}
