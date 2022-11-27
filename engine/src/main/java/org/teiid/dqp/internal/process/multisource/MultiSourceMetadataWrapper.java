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

package org.teiid.dqp.internal.process.multisource;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.metadata.AbstractMetadataRecord;
import org.teiid.metadata.BaseColumn.NullType;
import org.teiid.metadata.Column;
import org.teiid.metadata.Column.SearchType;
import org.teiid.metadata.Table;
import org.teiid.query.QueryPlugin;
import org.teiid.query.metadata.BasicQueryMetadataWrapper;
import org.teiid.query.metadata.QueryMetadataInterface;


/**
 * This class is a proxy to QueryMetadataInterface.
 */
public class MultiSourceMetadataWrapper extends BasicQueryMetadataWrapper {

    public static final String MULTISOURCE_COLUMN_NAME = "multisource.columnName"; //$NON-NLS-1$
    public static final String MULTISOURCE_PARTITIONED_PROPERTY = AbstractMetadataRecord.RELATIONAL_PREFIX + "multisource.partitioned"; //$NON-NLS-1$

    private static class MultiSourceGroup {
        Object multiSourceElement;
        List<?> columns;
    }

    private Map<String, String> multiSourceModels;
    private Map<Object, MultiSourceGroup> groups = new ConcurrentHashMap<Object, MultiSourceGroup>();

    public static Map<String, String> getMultiSourceModels(VDBMetaData vdb) {
        HashMap<String, String> result = new HashMap<String, String>();
        for (ModelMetaData mmd : vdb.getModelMetaDatas().values()) {
            if (!mmd.isSupportsMultiSourceBindings()) {
                continue;
            }
            String columnName = mmd.getPropertyValue(MULTISOURCE_COLUMN_NAME);
            if (columnName == null) {
                columnName = MultiSourceElement.DEFAULT_MULTI_SOURCE_ELEMENT_NAME;
            }
            result.put(mmd.getName(), columnName);
        }
        return result;
    }

    public MultiSourceMetadataWrapper(final QueryMetadataInterface actualMetadata, Map<String, String> multiSourceModels){
        super(actualMetadata);
        this.multiSourceModels = multiSourceModels;
    }

    public MultiSourceMetadataWrapper(QueryMetadataInterface metadata,
            Set<String> multiSourceModels) {
        this(metadata, new HashMap<String, String>());
        for (String string : multiSourceModels) {
            this.multiSourceModels.put(string, MultiSourceElement.DEFAULT_MULTI_SOURCE_ELEMENT_NAME);
        }
    }

    @Override
    public List<?> getElementIDsInGroupID(Object groupID)
            throws TeiidComponentException, QueryMetadataException {
        MultiSourceGroup msg = getMultiSourceGroup(groupID);
        if (msg != null) {
            return msg.columns;
        }
        return actualMetadata.getElementIDsInGroupID(groupID);
    }

    public MultiSourceGroup getMultiSourceGroup(Object groupID)
            throws TeiidComponentException, QueryMetadataException {
        MultiSourceGroup msg = groups.get(groupID);
        if (msg != null) {
            return msg;
        }
        if (isVirtualGroup(groupID)) {
            return null;
        }
        Object modelId = getModelID(groupID);
        String multiSourceElementName = this.multiSourceModels.get(getFullName(modelId));
        if (multiSourceElementName == null) {
            return null;
        }
        List<?> elements = actualMetadata.getElementIDsInGroupID(groupID);
        // Check whether a source_name column was modeled in the group already
        Object mse = null;
        for(int i = 0; i<elements.size() && mse == null; i++) {
            Object elemID = elements.get(i);
            if(actualMetadata.getName(elemID).equalsIgnoreCase(multiSourceElementName)) {
                if (!actualMetadata.getElementRuntimeTypeName(elemID).equalsIgnoreCase(DataTypeManager.DefaultDataTypes.STRING)) {
                    throw new QueryMetadataException(QueryPlugin.Event.TEIID31128, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31128, multiSourceElementName, groupID));
                }
                mse = elemID;
            }
        }

        if (mse == null) {
            List<Object> result = new ArrayList<Object>(elements);
            MultiSourceElement e = new MultiSourceElement();
            e.setName(multiSourceElementName);
            e.setParent((Table)groupID);
            e.setPosition(elements.size()+1);
            e.setRuntimeType(DataTypeManager.DefaultDataTypes.STRING);
            setMultiSourceElementMetadata(e);
            result.add(e);
            mse = e;
            elements = result;
        }
        msg = new MultiSourceGroup();
        msg.columns = elements;
        msg.multiSourceElement = mse;
        this.groups.put(groupID, msg);
        return msg;
    }

    public static void setMultiSourceElementMetadata(Column e) {
        e.setNullValues(0);
        e.setNullType(NullType.No_Nulls);
        e.setSearchType(SearchType.Searchable);
        e.setUpdatable(true);
        e.setLength(255);
    }

    @Override
    public Object getElementID(String elementName)
            throws TeiidComponentException, QueryMetadataException {
        try {
            return super.getElementID(elementName);
        } catch (QueryMetadataException e) {
            //could be pseudo-column
            int index = elementName.lastIndexOf('.');
            if(index <= 0 || elementName.length() <= index) {
                throw e;
            }
            String group = elementName.substring(0, index);
            elementName = elementName.substring(index + 1, elementName.length());
            MultiSourceGroup msg = getMultiSourceGroup(getGroupID(group));
            if (msg != null && elementName.equalsIgnoreCase(getName(msg.multiSourceElement))) {
                return msg.multiSourceElement;
            }
            throw e;
        }
    }

    @Override
    public boolean isMultiSource(Object modelId) throws QueryMetadataException, TeiidComponentException {
        return multiSourceModels.containsKey(getFullName(modelId));
    }

    @Override
    public boolean isMultiSourceElement(Object elementId) throws QueryMetadataException, TeiidComponentException {
        if (elementId instanceof MultiSourceElement) {
            return true;
        }
        Object gid = getGroupIDForElementID(elementId);
        if (isVirtualGroup(gid)) {
            return false;
        }
        Object modelID = this.getModelID(gid);
        String modelName = this.getFullName(modelID);
        String multiSourceColumnName = multiSourceModels.get(modelName);
        if(multiSourceColumnName == null) {
            return false;
        }
        return multiSourceColumnName.equalsIgnoreCase(getName(elementId));
    }

    @Override
    protected QueryMetadataInterface createDesignTimeMetadata() {
        return new MultiSourceMetadataWrapper(actualMetadata.getDesignTimeMetadata(), multiSourceModels);
    }

    @Override
    public boolean isPseudo(Object elementId) {
        if (elementId instanceof MultiSourceElement) {
            return true;
        }
        return actualMetadata.isPseudo(elementId);
    }

}
