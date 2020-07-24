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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineFactory;
import javax.script.ScriptEngineManager;

import org.teiid.adminapi.impl.DataPolicyMetadata;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.types.BlobImpl;
import org.teiid.core.types.ClobImpl;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.DataTypeManager.DefaultDataClasses;
import org.teiid.core.types.InputStreamFactory;
import org.teiid.core.types.SQLXMLImpl;
import org.teiid.core.util.ArgCheck;
import org.teiid.core.util.LRUCache;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.core.util.StringUtil;
import org.teiid.dqp.internal.process.DQPWorkContext;
import org.teiid.metadata.AbstractMetadataRecord;
import org.teiid.metadata.BaseColumn.NullType;
import org.teiid.metadata.Column;
import org.teiid.metadata.Column.SearchType;
import org.teiid.metadata.ColumnSet;
import org.teiid.metadata.Datatype;
import org.teiid.metadata.ForeignKey;
import org.teiid.metadata.FunctionMethod;
import org.teiid.metadata.FunctionParameter;
import org.teiid.metadata.KeyRecord;
import org.teiid.metadata.Procedure;
import org.teiid.metadata.ProcedureParameter;
import org.teiid.metadata.ProcedureParameter.Type;
import org.teiid.metadata.Schema;
import org.teiid.metadata.Table;
import org.teiid.query.QueryPlugin;
import org.teiid.query.function.FunctionLibrary;
import org.teiid.query.function.FunctionTree;
import org.teiid.query.mapping.relational.QueryNode;
import org.teiid.query.sql.lang.ObjectTable;
import org.teiid.query.sql.lang.SPParameter;


/**
 * Teiid's implementation of the QueryMetadataInterface that reads columns, groups, models etc.
 * from the metadata object model.
 */
public class TransformationMetadata extends BasicQueryMetadata implements Serializable {

    public static final String ALLOWED_LANGUAGES = "allowed-languages"; //$NON-NLS-1$

    private static final class LiveQueryNode extends QueryNode {
        Procedure p;
        private LiveQueryNode(Procedure p) {
            super(null);
            this.p = p;
        }

        public String getQuery() {
            return p.getQueryPlan();
        }
    }

    private static final class LiveTableQueryNode extends QueryNode {
        Table t;
        private LiveTableQueryNode(Table t) {
            super(null);
            this.t = t;
        }

        public String getQuery() {
            return t.getSelectTransformation();
        }
    }

    private final class VirtualFileInputStreamFactory extends
            InputStreamFactory {
        private final VDBResources.Resource r;

        private VirtualFileInputStreamFactory(VDBResources.Resource r) {
            this.r = r;
        }

        @Override
        public InputStream getInputStream() throws IOException {
            return r.openStream();
        }

        @Override
        public long getLength() {
            return r.getSize();
        }

        @Override
        public StorageMode getStorageMode() {
            return StorageMode.PERSISTENT;
        }
    }

    private static final long serialVersionUID = 1058627332954475287L;

    /** Delimiter character used when specifying fully qualified entity names */
    public static final char DELIMITER_CHAR = '.';
    public static final String DELIMITER_STRING = String.valueOf(DELIMITER_CHAR);

    // error message cached to avoid i18n lookup each time
    public static String NOT_EXISTS_MESSAGE = StringUtil.Constants.SPACE+QueryPlugin.Util.getString("TransformationMetadata.does_not_exist._1"); //$NON-NLS-1$

    public static Properties EMPTY_PROPS = new Properties();

    private final CompositeMetadataStore store;
    private Map<String, VDBResources.Resource> vdbEntries;
    private FunctionLibrary functionLibrary;
    private FunctionLibrary visibleFunctionLibrary;
    private VDBMetaData vdbMetaData;
    private ScriptEngineManager scriptEngineManager;
    private Map<String, ScriptEngineFactory> scriptEngineFactories = Collections.synchronizedMap(new HashMap<String, ScriptEngineFactory>());
    private Set<String> importedModels;
    private Set<String> allowedLanguages;
    private Map<String, DataPolicyMetadata> policies = new TreeMap<String, DataPolicyMetadata>(String.CASE_INSENSITIVE_ORDER);
    private boolean useOutputNames = true;
    private boolean hiddenResolvable = true;
    private boolean designTime = false;
    private boolean hiddenQualified = true;

    /*
     * TODO: move caching to jboss cache structure
     */
    private Map<String, Object> metadataCache = Collections.synchronizedMap(new LRUCache<String, Object>(250));
    private Map<String, Object> groupInfoCache = Collections.synchronizedMap(new LRUCache<String, Object>(250));
    private Map<String, Collection<Table>> partialNameToFullNameCache = Collections.synchronizedMap(new LRUCache<String, Collection<Table>>(1000));
    private Map<String, Collection<StoredProcedureInfo>> procedureCache = Collections.synchronizedMap(new LRUCache<String, Collection<StoredProcedureInfo>>(200));

    private boolean widenComparisonToString = true;
    private boolean allowEnv = true;
    private boolean longRanks;
    /**
     * TransformationMetadata constructor
     */
    public TransformationMetadata(VDBMetaData vdbMetadata, final CompositeMetadataStore store, Map<String, VDBResources.Resource> vdbEntries, FunctionTree systemFunctions, Collection<FunctionTree> functionTrees) {
        ArgCheck.isNotNull(store);
        this.vdbMetaData = vdbMetadata;
        if (this.vdbMetaData !=null) {
            this.scriptEngineManager = vdbMetadata.getAttachment(ScriptEngineManager.class);
            this.importedModels = this.vdbMetaData.getImportedModels();
            this.allowedLanguages = StringUtil.valueOf(vdbMetadata.getPropertyValue(ALLOWED_LANGUAGES), Set.class);
            if (this.allowedLanguages == null) {
                this.allowedLanguages = Collections.emptySet();
            }
        } else {
            this.importedModels = Collections.emptySet();
        }
        if (store.getDatatypes().isEmpty()) {
            store.addDataTypes(SystemMetadata.getInstance().getRuntimeTypeMap());
        }
        this.store = store;
        if (vdbEntries == null) {
            this.vdbEntries = Collections.emptyMap();
        } else {
            this.vdbEntries = vdbEntries;
        }
        if (functionTrees == null) {
            this.functionLibrary = new FunctionLibrary(systemFunctions);
        } else {
            this.functionLibrary = new FunctionLibrary(systemFunctions, functionTrees.toArray(new FunctionTree[functionTrees.size()]));
        }
    }

    private TransformationMetadata(final CompositeMetadataStore store, FunctionLibrary functionLibrary) {
        this.store = store;
        this.vdbEntries = Collections.emptyMap();
        this.functionLibrary = functionLibrary;
    }

    //==================================================================================
    //                     I N T E R F A C E   M E T H O D S
    //==================================================================================

    public Column getElementID(final String elementName) throws TeiidComponentException, QueryMetadataException {
        int columnIndex = elementName.lastIndexOf(TransformationMetadata.DELIMITER_STRING);
        if (columnIndex == -1) {
             throw new QueryMetadataException(QueryPlugin.Event.TEIID30355, elementName+TransformationMetadata.NOT_EXISTS_MESSAGE);
        }
        Table table = this.store.findGroup(elementName.substring(0, columnIndex));
        String shortElementName = elementName.substring(columnIndex + 1);
        return getColumn(elementName, table, shortElementName);
    }

    public static Column getColumn(final String elementName, Table table,
            String shortElementName) throws QueryMetadataException {
        Column c = table.getColumnByName(shortElementName);
        if (c != null) {
            return c;
        }
         throw new QueryMetadataException(QueryPlugin.Event.TEIID30356, elementName+TransformationMetadata.NOT_EXISTS_MESSAGE);
    }

    public Table getGroupID(final String groupName) throws TeiidComponentException, QueryMetadataException {
        Table t = getMetadataStore().findGroup(groupName);
        if (!isResolvable(t.getParent())) {
            throw new QueryMetadataException(QueryPlugin.Event.TEIID30352, groupName+TransformationMetadata.NOT_EXISTS_MESSAGE);
        }
        return t;
    }

    public Collection<String> getGroupsForPartialName(final String partialGroupName)
        throws TeiidComponentException, QueryMetadataException {
        ArgCheck.isNotEmpty(partialGroupName);

        Collection<Table> matches = this.partialNameToFullNameCache.get(partialGroupName);

        if (matches == null) {
            matches = getMetadataStore().getGroupsForPartialName(partialGroupName);

            this.partialNameToFullNameCache.put(partialGroupName, matches);
        }

        if (matches.isEmpty()) {
            return Collections.emptyList();
        }

        Collection<String> filteredResult = new ArrayList<String>(matches.size());
        for (Table table : matches) {
            if (!hiddenQualified || vdbMetaData == null || vdbMetaData.isVisible(table.getParent().getName())) {
                filteredResult.add(table.getFullName());
            }
        }
        return filteredResult;
    }

    public Object getModelID(final Object groupOrElementID) throws TeiidComponentException, QueryMetadataException {
        AbstractMetadataRecord metadataRecord = (AbstractMetadataRecord) groupOrElementID;
        AbstractMetadataRecord parent = metadataRecord.getParent();
        if (parent instanceof Schema) {
            return parent;
        }
        if (parent == null) {
            throw createInvalidRecordTypeException(groupOrElementID);
        }
        parent = parent.getParent();
        if (parent instanceof Schema) {
            return parent;
        }
        throw createInvalidRecordTypeException(groupOrElementID);
    }

    public String getFullName(final Object metadataID) throws TeiidComponentException, QueryMetadataException {
        AbstractMetadataRecord metadataRecord = (AbstractMetadataRecord) metadataID;
        if (metadataRecord instanceof Column) {
            Column c = (Column)metadataRecord;
            if (c.getParent() != null && c.getParent().getParent() instanceof Procedure) {
                return c.getParent().getParent().getFullName() + '.' + c.getName();
            }
        }
        return metadataRecord.getFullName();
    }

    @Override
    public String getName(Object metadataID) throws TeiidComponentException,
            QueryMetadataException {
        AbstractMetadataRecord metadataRecord = (AbstractMetadataRecord) metadataID;
        return metadataRecord.getName();
    }

    public List<Column> getElementIDsInGroupID(final Object groupID) throws TeiidComponentException, QueryMetadataException {
        List<Column> columns = ((Table)groupID).getColumns();
        if (columns == null || columns.isEmpty()) {
            throw new QueryMetadataException(QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31071, ((Table)groupID).getName()));
        }
        return columns;
    }

    public Object getGroupIDForElementID(final Object elementID) throws TeiidComponentException, QueryMetadataException {
        if(elementID instanceof Column) {
            Column columnRecord = (Column) elementID;
            AbstractMetadataRecord parent = columnRecord.getParent();
            if (parent instanceof Table) {
                return parent;
            }
            if (parent instanceof ColumnSet) {
                parent = ((ColumnSet<?>)parent).getParent();
                if (parent instanceof Procedure) {
                    return parent;
                }
            }
        }
        if(elementID instanceof ProcedureParameter) {
            ProcedureParameter columnRecord = (ProcedureParameter) elementID;
            return columnRecord.getParent();
        }
        throw createInvalidRecordTypeException(elementID);
    }

    public boolean hasProcedure(String name) throws TeiidComponentException {
        try {
            return getStoredProcInfoDirect(name) != null;
        } catch (QueryMetadataException e) {
            return true;
        }
    }

    public StoredProcedureInfo getStoredProcedureInfoForProcedure(final String name)
        throws TeiidComponentException, QueryMetadataException {
        StoredProcedureInfo result = getStoredProcInfoDirect(name);

        if (result == null || !isResolvable((Schema) result.getModelID())) {
             throw new QueryMetadataException(QueryPlugin.Event.TEIID30357, name+NOT_EXISTS_MESSAGE);
        }

        return result;
    }

    private StoredProcedureInfo getStoredProcInfoDirect(
            final String name)
            throws TeiidComponentException, QueryMetadataException {
        ArgCheck.isNotEmpty(name);
        String canonicalName = name.toUpperCase();
        Collection<StoredProcedureInfo> results = this.procedureCache.get(canonicalName);

        if (results == null) {
            Collection<Procedure> procRecords = getMetadataStore().getStoredProcedure(canonicalName);
            if (procRecords.isEmpty()) {
                return null;
            }
            results = new ArrayList<StoredProcedureInfo>(procRecords.size());
            for (Procedure procRecord : procRecords) {
                String procedureFullName = procRecord.getFullName();

                // create the storedProcedure info object that would hold procedure's metadata
                StoredProcedureInfo procInfo = new StoredProcedureInfo();
                procInfo.setProcedureCallableName(procedureFullName);
                procInfo.setProcedureID(procRecord);

                // modelID for the procedure
                procInfo.setModelID(procRecord.getParent());

                // get the parameter metadata info
                for (ProcedureParameter paramRecord : procRecord.getParameters()) {
                    String runtimeType = paramRecord.getRuntimeType();
                    int direction = this.convertParamRecordTypeToStoredProcedureType(paramRecord.getType());
                    // create a parameter and add it to the procedure object
                    SPParameter spParam = new SPParameter(paramRecord.getPosition(), direction, paramRecord.getFullName());
                    spParam.setMetadataID(paramRecord);
                    spParam.setClassType(DataTypeManager.getDataTypeClass(runtimeType));
                    if (paramRecord.isVarArg()) {
                        spParam.setVarArg(true);
                        spParam.setClassType(DataTypeManager.getArrayType(spParam.getClassType()));
                    }
                    procInfo.addParameter(spParam);
                }

                // if the procedure returns a resultSet, obtain resultSet metadata
                if(procRecord.getResultSet() != null) {
                    ColumnSet<Procedure> resultRecord = procRecord.getResultSet();
                    // resultSet is the last parameter in the procedure
                    int lastParamIndex = procInfo.getParameters().size() + 1;
                    SPParameter param = new SPParameter(lastParamIndex, SPParameter.RESULT_SET, resultRecord.getFullName());
                    param.setClassType(java.sql.ResultSet.class);
                    param.setMetadataID(resultRecord);

                    for (Column columnRecord : resultRecord.getColumns()) {
                        String colType = columnRecord.getRuntimeType();
                        param.addResultSetColumn(columnRecord.getFullName(), DataTypeManager.getDataTypeClass(colType), columnRecord);
                    }

                    procInfo.addParameter(param);
                }

                // if this is a virtual procedure get the procedure plan
                if(procRecord.isVirtual()) {
                    QueryNode queryNode = new LiveQueryNode(procRecord);
                    procInfo.setQueryPlan(queryNode);
                }

                procInfo.setUpdateCount(procRecord.getUpdateCount());
                results.add(procInfo);
            }
            this.procedureCache.put(canonicalName, results);
        }

        StoredProcedureInfo result = null;

        for (StoredProcedureInfo storedProcedureInfo : results) {
            Schema schema = (Schema)storedProcedureInfo.getModelID();
            if(name.equalsIgnoreCase(storedProcedureInfo.getProcedureCallableName()) || !hiddenQualified || vdbMetaData == null || vdbMetaData.isVisible(schema.getName())){
                if (result != null) {
                     throw new QueryMetadataException(QueryPlugin.Event.TEIID30358, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30358, name));
                }
                result = storedProcedureInfo;
            }
        }
        return result;
    }

    /**
     * Method to convert the parameter type returned from a ProcedureParameterRecord
     * to the parameter type expected by StoredProcedureInfo
     * @param parameterType
     * @return
     */
    private int convertParamRecordTypeToStoredProcedureType(final ProcedureParameter.Type parameterType) {
        switch (parameterType) {
            case In : return SPParameter.IN;
            case Out : return SPParameter.OUT;
            case InOut : return SPParameter.INOUT;
            case ReturnValue : return SPParameter.RETURN_VALUE;
            default :
                return -1;
        }
    }

    public String getElementRuntimeTypeName(final Object elementID) throws TeiidComponentException, QueryMetadataException {
        if(elementID instanceof Column) {
            return ((Column) elementID).getRuntimeType();
        } else if(elementID instanceof ProcedureParameter){
            return ((ProcedureParameter) elementID).getRuntimeType();
        } else {
            throw createInvalidRecordTypeException(elementID);
        }
    }

    public String getDefaultValue(final Object elementID) throws TeiidComponentException, QueryMetadataException {
        if(elementID instanceof Column) {
            return ((Column) elementID).getDefaultValue();
        } else if(elementID instanceof ProcedureParameter){
            return ((ProcedureParameter) elementID).getDefaultValue();
        } else {
            throw createInvalidRecordTypeException(elementID);
        }
    }

    public Object getMinimumValue(final Object elementID) throws TeiidComponentException, QueryMetadataException {
        if(elementID instanceof Column) {
            return ((Column) elementID).getMinimumValue();
        } else if(elementID instanceof ProcedureParameter){
            return null;
        } else {
            throw createInvalidRecordTypeException(elementID);
        }
    }

    public Object getMaximumValue(final Object elementID) throws TeiidComponentException, QueryMetadataException {
        if(elementID instanceof Column) {
            return ((Column) elementID).getMaximumValue();
        } else if(elementID instanceof ProcedureParameter){
            return null;
        } else {
            throw createInvalidRecordTypeException(elementID);
        }
    }

    public boolean isVirtualGroup(final Object groupID) throws TeiidComponentException, QueryMetadataException {
        if (groupID instanceof Table) {
            return ((Table) groupID).isVirtual();
        }
        if (groupID instanceof Procedure) {
            return ((Procedure) groupID).isVirtual();
        }
        throw createInvalidRecordTypeException(groupID);
    }

    public boolean isProcedure(final Object groupID) throws TeiidComponentException, QueryMetadataException {
        if(groupID instanceof Procedure) {
            return true;
        }
        if(groupID instanceof Table){
            return false;
        }
        throw createInvalidRecordTypeException(groupID);
    }

    public boolean isVirtualModel(final Object modelID) throws TeiidComponentException, QueryMetadataException {
        Schema modelRecord = (Schema) modelID;
        return !modelRecord.isPhysical();
    }

    public QueryNode getVirtualPlan(final Object groupID) throws TeiidComponentException, QueryMetadataException {
        Table tableRecord = (Table) groupID;
        if (!tableRecord.isVirtual()) {
             throw new QueryMetadataException(QueryPlugin.Event.TEIID30359, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30359, tableRecord.getFullName(), "Query")); //$NON-NLS-1$
        }
        LiveTableQueryNode queryNode = new LiveTableQueryNode(tableRecord);

        return queryNode;
    }

    public String getInsertPlan(final Object groupID) throws TeiidComponentException, QueryMetadataException {
        Table tableRecordImpl = (Table)groupID;
        if (!tableRecordImpl.isVirtual()) {
             throw new QueryMetadataException(QueryPlugin.Event.TEIID30359, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30359, tableRecordImpl.getFullName(), "Insert")); //$NON-NLS-1$
        }
        return tableRecordImpl.isInsertPlanEnabled()?tableRecordImpl.getInsertPlan():null;
    }

    public String getUpdatePlan(final Object groupID) throws TeiidComponentException, QueryMetadataException {
        Table tableRecordImpl = (Table)groupID;
        if (!tableRecordImpl.isVirtual()) {
            throw new QueryMetadataException(QueryPlugin.Event.TEIID30359, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30359, tableRecordImpl.getFullName(), "Update")); //$NON-NLS-1$
        }
        return tableRecordImpl.isUpdatePlanEnabled()?tableRecordImpl.getUpdatePlan():null;
    }

    public String getDeletePlan(final Object groupID) throws TeiidComponentException, QueryMetadataException {
        Table tableRecordImpl = (Table)groupID;
        if (!tableRecordImpl.isVirtual()) {
            throw new QueryMetadataException(QueryPlugin.Event.TEIID30359, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30359, tableRecordImpl.getFullName(), "Delete")); //$NON-NLS-1$
        }
        return tableRecordImpl.isDeletePlanEnabled()?tableRecordImpl.getDeletePlan():null;
    }

    public boolean modelSupports(final Object modelID, final int modelConstant)
        throws TeiidComponentException, QueryMetadataException {
        switch(modelConstant) {
            default:
                throw new UnsupportedOperationException(QueryPlugin.Util.getString("TransformationMetadata.Unknown_support_constant___12") + modelConstant); //$NON-NLS-1$
        }
    }

    public boolean groupSupports(final Object groupID, final int groupConstant)
        throws TeiidComponentException, QueryMetadataException {
        Table tableRecord = (Table) groupID;

        switch(groupConstant) {
            case SupportConstants.Group.UPDATE:
                return tableRecord.supportsUpdate();
            default:
                throw new UnsupportedOperationException(QueryPlugin.Util.getString("TransformationMetadata.Unknown_support_constant___12") + groupConstant); //$NON-NLS-1$
        }
    }

    public boolean elementSupports(final Object elementID, final int elementConstant)
        throws TeiidComponentException, QueryMetadataException {

        if(elementID instanceof Column) {
            Column columnRecord = (Column) elementID;
            switch(elementConstant) {
                case SupportConstants.Element.NULL:
                    return columnRecord.getNullType() == NullType.Nullable;
                case SupportConstants.Element.NULL_UNKNOWN:
                    return columnRecord.getNullType() == NullType.Unknown;
                case SupportConstants.Element.SEARCHABLE_COMPARE:
                    return (columnRecord.getSearchType() == SearchType.Searchable || columnRecord.getSearchType() == SearchType.All_Except_Like);
                case SupportConstants.Element.SEARCHABLE_LIKE:
                    return (columnRecord.getSearchType() == SearchType.Searchable || columnRecord.getSearchType() == SearchType.Like_Only);
                case SupportConstants.Element.SEARCHABLE_EQUALITY:
                    return (columnRecord.getSearchType() == SearchType.Equality_Only || columnRecord.getSearchType() == SearchType.Searchable || columnRecord.getSearchType() == SearchType.All_Except_Like);
                case SupportConstants.Element.SELECT:
                    return columnRecord.isSelectable();
                case SupportConstants.Element.UPDATE:
                    return columnRecord.isUpdatable();
                case SupportConstants.Element.DEFAULT_VALUE:
                    Object defaultValue = columnRecord.getDefaultValue();
                    if(defaultValue == null) {
                        return false;
                    }
                    return true;
                case SupportConstants.Element.AUTO_INCREMENT:
                    return columnRecord.isAutoIncremented();
                case SupportConstants.Element.CASE_SENSITIVE:
                    return columnRecord.isCaseSensitive();
                case SupportConstants.Element.SIGNED:
                    return columnRecord.isSigned();
                default:
                    throw new UnsupportedOperationException(QueryPlugin.Util.getString("TransformationMetadata.Unknown_support_constant___12") + elementConstant); //$NON-NLS-1$
            }
        } else if(elementID instanceof ProcedureParameter) {
            ProcedureParameter columnRecord = (ProcedureParameter) elementID;
            switch(elementConstant) {
                case SupportConstants.Element.NULL:
                    return columnRecord.getNullType() == NullType.Nullable;
                case SupportConstants.Element.NULL_UNKNOWN:
                    return columnRecord.getNullType() == NullType.Unknown;
                case SupportConstants.Element.SEARCHABLE_COMPARE:
                case SupportConstants.Element.SEARCHABLE_LIKE:
                    return false;
                case SupportConstants.Element.SELECT:
                    return columnRecord.getType() != Type.In;
                case SupportConstants.Element.UPDATE:
                    return false;
                case SupportConstants.Element.DEFAULT_VALUE:
                    Object defaultValue = columnRecord.getDefaultValue();
                    if(defaultValue == null) {
                        return false;
                    }
                    return true;
                case SupportConstants.Element.AUTO_INCREMENT:
                    return false;
                case SupportConstants.Element.CASE_SENSITIVE:
                    return false;
                case SupportConstants.Element.SIGNED:
                    return true;
                default:
                    throw new UnsupportedOperationException(QueryPlugin.Util.getString("TransformationMetadata.Unknown_support_constant___12") + elementConstant); //$NON-NLS-1$
            }

        } else {
            throw createInvalidRecordTypeException(elementID);
        }
    }

    private IllegalArgumentException createInvalidRecordTypeException(Object elementID) {
        return new IllegalArgumentException(QueryPlugin.Util.getString("TransformationMetadata.Invalid_type", elementID!=null?elementID.getClass().getName():null));         //$NON-NLS-1$
    }

    public int getMaxSetSize(final Object modelID) throws TeiidComponentException, QueryMetadataException {
        return 0;
    }

    public Collection<KeyRecord> getIndexesInGroup(final Object groupID) throws TeiidComponentException, QueryMetadataException {
        return ((Table)groupID).getIndexes();
    }

    public Collection<KeyRecord> getUniqueKeysInGroup(final Object groupID)
        throws TeiidComponentException, QueryMetadataException {
        Table tableRecordImpl = (Table)groupID;
        ArrayList<KeyRecord> result = new ArrayList<KeyRecord>(tableRecordImpl.getUniqueKeys());
        if (tableRecordImpl.getPrimaryKey() != null) {
            result.add(tableRecordImpl.getPrimaryKey());
        }
        for (KeyRecord key : tableRecordImpl.getIndexes()) {
            if (key.getType() == KeyRecord.Type.Unique) {
                result.add(key);
            }
        }
        return result;
    }

    public Collection<ForeignKey> getForeignKeysInGroup(final Object groupID)
        throws TeiidComponentException, QueryMetadataException {
        return ((Table)groupID).getForeignKeys();
    }

    public Object getPrimaryKeyIDForForeignKeyID(final Object foreignKeyID)
        throws TeiidComponentException, QueryMetadataException {
        ForeignKey fkRecord = (ForeignKey) foreignKeyID;
        return fkRecord.getPrimaryKey();
    }

    public Collection<KeyRecord> getAccessPatternsInGroup(final Object groupID)
        throws TeiidComponentException, QueryMetadataException {
        return ((Table)groupID).getAccessPatterns();
    }

    public List<Column> getElementIDsInIndex(final Object index) throws TeiidComponentException, QueryMetadataException {
        return ((ColumnSet<?>)index).getColumns();
    }

    public List<Column> getElementIDsInKey(final Object key) throws TeiidComponentException, QueryMetadataException {
        return ((ColumnSet<?>)key).getColumns();
    }

    public List<Column> getElementIDsInAccessPattern(final Object accessPattern)
        throws TeiidComponentException, QueryMetadataException {
        return ((ColumnSet<?>)accessPattern).getColumns();
    }

    /**
     * @see org.teiid.query.metadata.QueryMetadataInterface#hasMaterialization(java.lang.Object)
     * @since 4.2
     */
    public boolean hasMaterialization(final Object groupID) throws TeiidComponentException,
                                                      QueryMetadataException {
        Table tableRecord = (Table) groupID;
        return tableRecord.isMaterialized();
    }

    /**
     * @see org.teiid.query.metadata.QueryMetadataInterface#getMaterialization(java.lang.Object)
     * @since 4.2
     */
    public Object getMaterialization(final Object groupID) throws TeiidComponentException,
                                                    QueryMetadataException {
        Table tableRecord = (Table) groupID;
        if(tableRecord.isMaterialized()) {
            return tableRecord.getMaterializedTable();
        }
        return null;
    }

    /**
     * @see org.teiid.query.metadata.QueryMetadataInterface#getMaterializationStage(java.lang.Object)
     * @since 4.2
     */
    public Object getMaterializationStage(final Object groupID) throws TeiidComponentException,
                                                         QueryMetadataException {
        Table tableRecord = (Table) groupID;
        if(tableRecord.isMaterialized()) {
            return tableRecord.getMaterializedStageTable();
        }
        return null;
    }

    /**
     * @see org.teiid.query.metadata.QueryMetadataInterface#getVirtualDatabaseName()
     */
    public String getVirtualDatabaseName() throws TeiidComponentException, QueryMetadataException {
        if (vdbMetaData == null) {
            return null;
        }
        return vdbMetaData.getName();
    }

    public VDBMetaData getVdbMetaData() {
        return vdbMetaData;
    }

    @Override
    public float getCardinality(final Object groupID) throws TeiidComponentException, QueryMetadataException {
        return ((Table) groupID).getCardinalityAsFloat();
    }

    public String getNameInSource(final Object metadataID) throws TeiidComponentException, QueryMetadataException {
        return ((AbstractMetadataRecord) metadataID).getNameInSource();
    }

    public int getElementLength(final Object elementID) throws TeiidComponentException, QueryMetadataException {
        if(elementID instanceof Column) {
            return ((Column) elementID).getLength();
        } else if(elementID instanceof ProcedureParameter){
            return ((ProcedureParameter) elementID).getLength();
        } else {
            throw createInvalidRecordTypeException(elementID);
        }
    }

    public int getPosition(final Object elementID) throws TeiidComponentException, QueryMetadataException {
        if(elementID instanceof Column) {
            return ((Column) elementID).getPosition();
        } else if(elementID instanceof ProcedureParameter) {
            return ((ProcedureParameter) elementID).getPosition();
        } else {
            throw createInvalidRecordTypeException(elementID);
        }
    }

    public int getPrecision(final Object elementID) throws TeiidComponentException, QueryMetadataException {
        if(elementID instanceof Column) {
            return ((Column) elementID).getPrecision();
        } else if(elementID instanceof ProcedureParameter) {
            return ((ProcedureParameter) elementID).getPrecision();
        } else {
            throw createInvalidRecordTypeException(elementID);
        }
    }

    public int getRadix(final Object elementID) throws TeiidComponentException, QueryMetadataException {
        if(elementID instanceof Column) {
            return ((Column) elementID).getRadix();
        } else if(elementID instanceof ProcedureParameter) {
            return ((ProcedureParameter) elementID).getRadix();
        } else {
            throw createInvalidRecordTypeException(elementID);
        }
    }

    public String getFormat(Object elementID) throws TeiidComponentException, QueryMetadataException {
        if(elementID instanceof Column) {
            return ((Column) elementID).getFormat();
        }
        throw createInvalidRecordTypeException(elementID);
    }

    public int getScale(final Object elementID) throws TeiidComponentException, QueryMetadataException {
        if(elementID instanceof Column) {
            return ((Column) elementID).getScale();
        } else if(elementID instanceof ProcedureParameter) {
            return ((ProcedureParameter) elementID).getScale();
        } else {
            throw createInvalidRecordTypeException(elementID);
        }
    }

    @Override
    public float getDistinctValues(final Object elementID) throws TeiidComponentException, QueryMetadataException {
        if(elementID instanceof Column) {
            return ((Column) elementID).getDistinctValuesAsFloat();
        } else if(elementID instanceof ProcedureParameter) {
            return -1;
        } else {
            throw createInvalidRecordTypeException(elementID);
        }
    }

    @Override
    public float getNullValues(final Object elementID) throws TeiidComponentException, QueryMetadataException {
        if(elementID instanceof Column) {
            return ((Column) elementID).getNullValuesAsFloat();
        } else if(elementID instanceof ProcedureParameter) {
            return -1;
        } else {
            throw createInvalidRecordTypeException(elementID);
        }
    }

    public String getNativeType(final Object elementID) throws TeiidComponentException, QueryMetadataException {
        if(elementID instanceof Column) {
            return ((Column) elementID).getNativeType();
        } else if(elementID instanceof ProcedureParameter) {
            return null;
        } else {
            throw createInvalidRecordTypeException(elementID);
        }
    }

    public Properties getExtensionProperties(final Object metadataID) throws TeiidComponentException, QueryMetadataException {
        AbstractMetadataRecord metadataRecord = (AbstractMetadataRecord) metadataID;
        Map<String, String> result = metadataRecord.getProperties();
        if (result == null) {
            return EMPTY_PROPS;
        }
        Properties p = new Properties();
        p.putAll(result);
        return p;
    }

    @Override
    public String getExtensionProperty(Object metadataID, String key, boolean checkUnqualified) {
        return ((AbstractMetadataRecord)metadataID).getProperty(key, checkUnqualified);
    }

    /**
     * @see org.teiid.query.metadata.BasicQueryMetadata#getBinaryVDBResource(java.lang.String)
     * @since 4.3
     */
    public byte[] getBinaryVDBResource(String resourcePath) throws TeiidComponentException, QueryMetadataException {
        final VDBResources.Resource f = getFile(resourcePath);
        if (f == null) {
            return null;
        }
        try {
            return ObjectConverterUtil.convertToByteArray(f.openStream());
        } catch (IOException e) {
             throw new TeiidComponentException(QueryPlugin.Event.TEIID30365, e);
        }
    }

    public ClobImpl getVDBResourceAsClob(String resourcePath) {
        final VDBResources.Resource f = getFile(resourcePath);
        if (f == null) {
            return null;
        }
        return new ClobImpl(new VirtualFileInputStreamFactory(f), -1);
    }

    public SQLXMLImpl getVDBResourceAsSQLXML(String resourcePath) {
        final VDBResources.Resource f = getFile(resourcePath);
        if (f == null) {
            return null;
        }
        return new SQLXMLImpl(new VirtualFileInputStreamFactory(f));
    }

    public BlobImpl getVDBResourceAsBlob(String resourcePath) {
        final VDBResources.Resource f = getFile(resourcePath);
        if (f == null) {
            return null;
        }
        return new BlobImpl(new VirtualFileInputStreamFactory(f));
    }

    private VDBResources.Resource getFile(String resourcePath) {
        if (resourcePath == null) {
            return null;
        }
        return this.vdbEntries.get(resourcePath);
    }

    /**
     * @see org.teiid.query.metadata.BasicQueryMetadata#getCharacterVDBResource(java.lang.String)
     * @since 4.3
     */
    public String getCharacterVDBResource(String resourcePath) throws TeiidComponentException, QueryMetadataException {
        try {
            byte[] bytes = getBinaryVDBResource(resourcePath);
            if (bytes == null) {
                return null;
            }
            return ObjectConverterUtil.convertToString(new ByteArrayInputStream(bytes));
        } catch (IOException e) {
             throw new TeiidComponentException(QueryPlugin.Event.TEIID30366, e);
        }
    }

    public CompositeMetadataStore getMetadataStore() {
        return this.store;
    }

    /**
     * @see org.teiid.query.metadata.BasicQueryMetadata#getVDBResourcePaths()
     * @since 4.3
     */
    public String[] getVDBResourcePaths() throws TeiidComponentException, QueryMetadataException {
        LinkedList<String> paths = new LinkedList<String>();
        for (Map.Entry<String, VDBResources.Resource> entry : this.vdbEntries.entrySet()) {
            paths.add(entry.getKey());
        }
        return paths.toArray(new String[paths.size()]);
    }

    @Override
    public Object addToMetadataCache(Object metadataID, String key, Object value) {
        boolean groupInfo = key.startsWith(GroupInfo.CACHE_PREFIX);
        key = getCacheKey(key, (AbstractMetadataRecord)metadataID);
        if (groupInfo) {
            return this.groupInfoCache.put(key, value);
        }
        return this.metadataCache.put(key, value);
    }

    @Override
    public Object getFromMetadataCache(Object metadataID, String key)
            throws TeiidComponentException, QueryMetadataException {
        boolean groupInfo = key.startsWith(GroupInfo.CACHE_PREFIX);
        key = getCacheKey(key, (AbstractMetadataRecord)metadataID);
        if (groupInfo) {
            return this.groupInfoCache.get(key);
        }
        return this.metadataCache.get(key);
    }

    private String getCacheKey(String key, AbstractMetadataRecord record) {
        return record.getUUID() + "/" + key; //$NON-NLS-1$
    }

    @Override
    public FunctionLibrary getFunctionLibrary() {
        if (!isHiddenResolvableInternal()) {
            if (visibleFunctionLibrary == null && functionLibrary != null && vdbMetaData != null) {
                FunctionTree[] userFuncts = functionLibrary.getUserFunctions();
                List<FunctionTree> filtered = new ArrayList<>();
                if (userFuncts != null) {
                    for (FunctionTree tree : userFuncts) {
                        if (vdbMetaData.isVisible(tree.getSchemaName())) {
                            filtered.add(tree);
                        }
                    }
                }
                visibleFunctionLibrary = new FunctionLibrary(
                        functionLibrary.getSystemFunctions(),
                        filtered.toArray(new FunctionTree[filtered.size()]));
            }
            return visibleFunctionLibrary;
        }
        return this.functionLibrary;
    }

    @Override
    public Object getPrimaryKey(Object metadataID) {
        Table table = (Table)metadataID;
        return table.getPrimaryKey();
    }

    @Override
    public TransformationMetadata getDesignTimeMetadata() {
        if (this.designTime) {
            return this;
        }
        TransformationMetadata tm = new TransformationMetadata(store, functionLibrary);
        tm.groupInfoCache = this.groupInfoCache;
        tm.metadataCache = this.metadataCache;
        tm.partialNameToFullNameCache = this.partialNameToFullNameCache;
        tm.procedureCache = this.procedureCache;
        tm.scriptEngineManager = this.scriptEngineManager;
        tm.importedModels = this.importedModels;
        tm.allowedLanguages = this.allowedLanguages;
        tm.widenComparisonToString = this.widenComparisonToString;
        tm.longRanks = this.longRanks;
        tm.hiddenResolvable = true;
        tm.vdbEntries = this.vdbEntries;
        tm.vdbMetaData = this.vdbMetaData;
        tm.designTime = true;
        tm.hiddenQualified = (this.vdbMetaData != null && Boolean.valueOf(
                this.vdbMetaData.getPropertyValue("hidden-qualified"))); //$NON-NLS-1$
        return tm;
    }

    @Override
    public Set<String> getImportedModels() {
        return this.importedModels;
    }

    @Override
    public ScriptEngine getScriptEngineDirect(String language)
            throws TeiidProcessingException {
        if (this.scriptEngineManager == null) {
            this.scriptEngineManager = new ScriptEngineManager();
        }
        ScriptEngine engine = null;
        if (allowedLanguages == null || allowedLanguages.contains(language)) {
            /*
             * because of state caching in the engine, we'll return a new instance for each
             * usage.  we can pool if needed and add a returnEngine method
             */
            ScriptEngineFactory sef = this.scriptEngineFactories.get(language);
            if (sef != null) {
                try {
                    engine = sef.getScriptEngine();
                    engine.setBindings(scriptEngineManager.getBindings(), ScriptContext.ENGINE_SCOPE);
                } catch (Exception e) {
                    //just swallow the exception to mimic the jsr behavior
                }
            }
            engine = this.scriptEngineManager.getEngineByName(language);
        }
        if (engine == null) {
            Set<String> names = new LinkedHashSet<String>();
            for (ScriptEngineFactory factory : this.scriptEngineManager.getEngineFactories()) {
                names.addAll(factory.getNames());
            }
            if (allowedLanguages != null) {
                names.retainAll(allowedLanguages);
            }
            names.add(ObjectTable.DEFAULT_LANGUAGE);
            throw new TeiidProcessingException(QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31109, language, names));
        }
        this.scriptEngineFactories.put(language, engine.getFactory());
        return engine;
    }

    @Override
    public boolean isVariadic(Object metadataID) {
        if (metadataID instanceof ProcedureParameter) {
            return ((ProcedureParameter)metadataID).isVarArg();
        }
        if (metadataID instanceof FunctionParameter) {
            return ((FunctionParameter)metadataID).isVarArg();
        }
        return false;
    }

    @Override
    public Schema getModelID(String modelName) throws TeiidComponentException,
            QueryMetadataException {
        Schema s = this.getMetadataStore().getSchema(modelName);
        if (s == null || !isResolvable(s)) {
            throw new QueryMetadataException(QueryPlugin.Event.TEIID30352, modelName+TransformationMetadata.NOT_EXISTS_MESSAGE);
        }
        return s;
    }

    private final boolean isResolvable(Schema s) {
        return isHiddenResolvableInternal() || vdbMetaData == null || vdbMetaData.isVisible(s.getName());
    }

    @Override
    public List<Schema> getModelIDs() {
        if (!isHiddenResolvableInternal() && vdbMetaData != null) {
            //filter list
            return this.getMetadataStore().getSchemaList().stream()
                    .filter(s -> vdbMetaData.isVisible(s.getName()))
                    .collect(Collectors.toList());
        }
        return this.getMetadataStore().getSchemaList();
    }

    public void setPolicies(Map<String, DataPolicyMetadata> policies) {
        this.policies = policies;
    }

    public Map<String, DataPolicyMetadata> getPolicies() {
        return policies;
    }

    @Override
    public boolean useOutputName() {
        return useOutputNames;
    }

    public void setUseOutputNames(boolean useOutputNames) {
        this.useOutputNames = useOutputNames;
    }

    @Override
    public boolean widenComparisonToString() {
        return widenComparisonToString;
    }

    public void setWidenComparisonToString(boolean widenComparisonToString) {
        this.widenComparisonToString = widenComparisonToString;
    }

    boolean isHiddenResolvableInternal() {
        return hiddenResolvable || DQPWorkContext.getWorkContext().isAdmin();
    }

    public void setHiddenResolvable(boolean hiddenResolvable) {
        this.hiddenResolvable = hiddenResolvable;
    }

    @Override
    public Class<?> getDataTypeClass(String typeOrDomainName)
            throws QueryMetadataException {
        if (typeOrDomainName == null) {
            return DefaultDataClasses.NULL;
        }

        Datatype type = store.getDatatypes().get(typeOrDomainName);

        if (type != null) {
            return DataTypeManager.getDataTypeClass(type.getRuntimeTypeName());
        }
        if (DataTypeManager.isArrayType(typeOrDomainName)) {
            return DataTypeManager.getArrayType(getDataTypeClass(typeOrDomainName.substring(0, typeOrDomainName.length() - 2)));
        }
        throw new QueryMetadataException(QueryPlugin.Event.TEIID31254, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31254, typeOrDomainName));
    }

    @Override
    public boolean isEnvAllowed() {
        return this.allowEnv;
    }

    public void setAllowENV(boolean b) {
        this.allowEnv = b;
    }

    @Override
    public boolean isLongRanks() {
        return longRanks;
    }

    public void setLongRanks(boolean longRanks) {
        this.longRanks = longRanks;
    }

    @Override
    public FunctionMethod getPushdownFunction(Object modelID, String fullName) {
        if (vdbMetaData == null || !(modelID instanceof Schema)) {
            return null;
        }
        Schema schema = (Schema)modelID;
        ModelMetaData model = vdbMetaData.getModel(schema.getName());
        if (model == null) {
            return null;
        }
        PushdownFunctions functions = model.getAttachment(PushdownFunctions.class);
        if (functions == null) {
            return null;
        }
        return functions.get(fullName);
    }

}