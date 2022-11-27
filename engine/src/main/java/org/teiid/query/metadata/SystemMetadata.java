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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.DataTypeManager.DefaultDataTypes;
import org.teiid.core.util.Assertion;
import org.teiid.core.util.PropertiesUtils;
import org.teiid.metadata.Datatype;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.MetadataStore;
import org.teiid.metadata.Table;
import org.teiid.query.function.SystemFunctionManager;
import org.teiid.query.parser.QueryParser;
import org.teiid.query.validator.ValidatorReport;

public class SystemMetadata {

    private static SystemMetadata INSTANCE = new SystemMetadata();

    public static SystemMetadata getInstance() {
        return INSTANCE;
    }

    private List<Datatype> dataTypes = new ArrayList<Datatype>();
    private Map<String, Datatype> typeMap = new TreeMap<String, Datatype>(String.CASE_INSENSITIVE_ORDER);
    private MetadataStore systemStore;
    private final SystemFunctionManager systemFunctionManager;

    public SystemMetadata() {
        InputStream is = SystemMetadata.class.getClassLoader().getResourceAsStream("org/teiid/metadata/types.dat"); //$NON-NLS-1$
        try {
            InputStreamReader isr = new InputStreamReader(is, Charset.forName("UTF-8")); //$NON-NLS-1$
            BufferedReader br = new BufferedReader(isr);
            String s = br.readLine();
            String[] props = s.split("\\|"); //$NON-NLS-1$
            while ((s = br.readLine()) != null) {
                Datatype dt = new Datatype();
                String[] vals = s.split("\\|"); //$NON-NLS-1$
                Properties p = new Properties();
                for (int i = 0; i < props.length; i++) {
                    if (vals[i].length() != 0) {
                        p.setProperty(props[i], new String(vals[i]));
                    }
                }
                PropertiesUtils.setBeanProperties(dt, p, null);
                if ("string".equals(dt.getName())) { //$NON-NLS-1$
                    dt.setLength(DataTypeManager.MAX_STRING_LENGTH);
                } else if ("varbinary".equals(dt.getName())) { //$NON-NLS-1$
                    dt.setLength(DataTypeManager.MAX_VARBINARY_BYTES);
                }
                dataTypes.add(dt);
                if (dt.isBuiltin()) {
                    //we don't want to retain the conflicting designer names
                    if (!dt.getName().equals(dt.getRuntimeTypeName())) {
                        dt.setName(dt.getRuntimeTypeName());
                    }
                    typeMap.put(dt.getRuntimeTypeName(), dt);
                }
            }
            is.close();
        } catch (IOException e) {
            throw new TeiidRuntimeException(e);
        } finally {
            try {
                is.close();
            } catch (IOException e) {
                throw new TeiidRuntimeException(e);
            }
        }
        addAliasType(DataTypeManager.DataTypeAliases.BIGINT);
        addAliasType(DataTypeManager.DataTypeAliases.DECIMAL);
        addAliasType(DataTypeManager.DataTypeAliases.REAL);
        addAliasType(DataTypeManager.DataTypeAliases.SMALLINT);
        addAliasType(DataTypeManager.DataTypeAliases.TINYINT);
        addAliasType(DataTypeManager.DataTypeAliases.VARCHAR);
        for (String name : DataTypeManager.getAllDataTypeNames()) {
            if (!name.equals(DefaultDataTypes.NULL)) {
                Assertion.isNotNull(typeMap.get(name), name);
            }
        }

        VDBMetaData vdb = new VDBMetaData();
        vdb.setName("System");  //$NON-NLS-1$
        vdb.setVersion(1);
        Properties p = new Properties();
        this.systemFunctionManager = new SystemFunctionManager(typeMap);
        QueryParser parser = new QueryParser();
        systemStore = loadSchema(vdb, p, "SYS", parser).asMetadataStore(); //$NON-NLS-1$
        systemStore.addDataTypes(typeMap);
        loadSchema(vdb, p, "SYSADMIN", parser).mergeInto(systemStore); //$NON-NLS-1$
        TransformationMetadata tm = new TransformationMetadata(vdb, new CompositeMetadataStore(systemStore), null, systemFunctionManager.getSystemFunctions(), null);
        vdb.addAttachment(QueryMetadataInterface.class, tm);
        MetadataValidator validator = new MetadataValidator(this.typeMap, parser);
        ValidatorReport report = validator.validate(vdb, systemStore);
        if (report.hasItems()) {
            throw new TeiidRuntimeException(report.getFailureMessage());
        }
    }

    private MetadataFactory loadSchema(VDBMetaData vdb, Properties p, String name, QueryParser parser) {
        ModelMetaData mmd = new ModelMetaData();
        mmd.setName(name);
        vdb.addModel(mmd);
        InputStream is = SystemMetadata.class.getClassLoader().getResourceAsStream("org/teiid/metadata/"+name+".sql"); //$NON-NLS-1$ //$NON-NLS-2$
        try {
            MetadataFactory factory = new MetadataFactory(vdb.getName(), vdb.getVersion(), name, typeMap, p, null);
            parser.parseDDL(factory, new InputStreamReader(is, Charset.forName("UTF-8"))); //$NON-NLS-1$
            for (Table t : factory.getSchema().getTables().values()) {
                t.setSystem(true);
            }
            return factory;
        } finally {
            try {
                is.close();
            } catch (IOException e) {
                throw new TeiidRuntimeException(e);
            }
        }
    }

    private void addAliasType(String alias) {
        Class<?> typeClass = DataTypeManager.getDataTypeClass(alias);
        String primaryType = DataTypeManager.getDataTypeName(typeClass);
        Datatype dt = typeMap.get(primaryType);
        Assertion.isNotNull(dt, alias);
        typeMap.put(alias, dt);
    }

    /**
     * List of all "built-in" datatypes.  Note that the datatype names do not necessarily match the corresponding runtime type names i.e. int vs. integer
     * @return
     */
    public List<Datatype> getDataTypes() {
        return dataTypes;
    }

    /**
     * Map of runtime types and aliases to runtime datatypes
     * @return
     */
    public Map<String, Datatype> getRuntimeTypeMap() {
        return typeMap;
    }

    public MetadataStore getSystemStore() {
        return systemStore;
    }

    public SystemFunctionManager getSystemFunctionManager() {
        return systemFunctionManager;
    }
}
