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
package org.teiid.jboss;

import org.jboss.as.controller.SimpleAttributeDefinition;
import org.jboss.as.controller.SimpleAttributeDefinitionBuilder;
import org.jboss.dmr.ModelType;
import org.teiid.adminapi.Admin;

class OperationsConstants {
    public static final SimpleAttributeDefinition SESSION = new SimpleAttributeDefinition("session", ModelType.STRING, false); //$NON-NLS-1$
    public static final SimpleAttributeDefinition VDB_NAME = new SimpleAttributeDefinition("vdb-name", ModelType.STRING, false); //$NON-NLS-1$
    public static final SimpleAttributeDefinition VDB_VERSION = new SimpleAttributeDefinition("vdb-version", ModelType.STRING, false); //$NON-NLS-1$
    public static final SimpleAttributeDefinition EXECUTION_ID = new SimpleAttributeDefinition("execution-id", ModelType.STRING, false); //$NON-NLS-1$
    public static final SimpleAttributeDefinition CACHE_TYPE = new SimpleAttributeDefinitionBuilder("cache-type", ModelType.STRING) //$NON-NLS-1$
        .setAllowNull(false)
        .setAllowExpression(false)
        .setAllowedValues(Admin.Cache.PREPARED_PLAN_CACHE.name(), Admin.Cache.QUERY_SERVICE_RESULT_SET_CACHE.name())
        .build();
    public static final SimpleAttributeDefinition XID = new SimpleAttributeDefinition("xid", ModelType.STRING, false); //$NON-NLS-1$
    public static final SimpleAttributeDefinition DATA_ROLE = new SimpleAttributeDefinition("data-role", ModelType.STRING, false); //$NON-NLS-1$
    public static final SimpleAttributeDefinition MAPPED_ROLE = new SimpleAttributeDefinition("mapped-role", ModelType.STRING, false); //$NON-NLS-1$
    public static final SimpleAttributeDefinition CONNECTION_TYPE = new SimpleAttributeDefinition("connection-type", ModelType.STRING, false); //$NON-NLS-1$
    public static final SimpleAttributeDefinition MODEL_NAME = new SimpleAttributeDefinition("model-name", ModelType.STRING, false); //$NON-NLS-1$
    public static final SimpleAttributeDefinition SOURCE_NAME = new SimpleAttributeDefinition("source-name", ModelType.STRING, false); //$NON-NLS-1$
    public static final SimpleAttributeDefinition DS_NAME = new SimpleAttributeDefinition("ds-name", ModelType.STRING, false); //$NON-NLS-1$
    public static final SimpleAttributeDefinition RAR_NAME = new SimpleAttributeDefinition("rar-name", ModelType.STRING, false); //$NON-NLS-1$
    public static final SimpleAttributeDefinition MODEL_NAMES = new SimpleAttributeDefinition("model-names", ModelType.STRING, true); //$NON-NLS-1$
    public static final SimpleAttributeDefinition SOURCE_VDBNAME = new SimpleAttributeDefinition("source-vdb-name", ModelType.STRING, false); //$NON-NLS-1$
    public static final SimpleAttributeDefinition SOURCE_VDBVERSION = new SimpleAttributeDefinition("source-vdb-version", ModelType.STRING, false); //$NON-NLS-1$
    public static final SimpleAttributeDefinition TARGET_VDBNAME = new SimpleAttributeDefinition("target-vdb-name", ModelType.STRING, false); //$NON-NLS-1$
    public static final SimpleAttributeDefinition TARGET_VDBVERSION = new SimpleAttributeDefinition("target-vdb-version", ModelType.STRING, false); //$NON-NLS-1$
    public static final SimpleAttributeDefinition SQL_QUERY = new SimpleAttributeDefinition("sql-query", ModelType.STRING, false); //$NON-NLS-1$
    public static final SimpleAttributeDefinition TIMEOUT_IN_MILLI = new SimpleAttributeDefinition("timeout-in-milli", ModelType.STRING, false); //$NON-NLS-1$
    public static final SimpleAttributeDefinition TRANSLATOR_NAME = new SimpleAttributeDefinition("translator-name", ModelType.STRING, false); //$NON-NLS-1$
    public static final SimpleAttributeDefinition PROPERTY_TYPE = new SimpleAttributeDefinition("type", ModelType.STRING, false); //$NON-NLS-1$
    public static final SimpleAttributeDefinition ENTITY_TYPE = new SimpleAttributeDefinition("entity-type", ModelType.STRING, true); //$NON-NLS-1$
    public static final SimpleAttributeDefinition ENTITY_PATTERN = new SimpleAttributeDefinition("entity-pattern", ModelType.STRING, true); //$NON-NLS-1$
    public static final SimpleAttributeDefinition FORMAT = new SimpleAttributeDefinitionBuilder("format", ModelType.STRING) //$NON-NLS-1$
            .setAllowNull(true)
            .setAllowExpression(false)
            .build();
    public static final SimpleAttributeDefinition INCLUDE_SOURCE = new SimpleAttributeDefinition("include-source", ModelType.BOOLEAN, true); //$NON-NLS-1$

    public static final SimpleAttributeDefinition OPTIONAL_VDB_NAME = new SimpleAttributeDefinition("vdb-name", ModelType.STRING, true); //$NON-NLS-1$
    public static final SimpleAttributeDefinition OPTIONAL_VDB_VERSION = new SimpleAttributeDefinition("vdb-version", ModelType.STRING, true); //$NON-NLS-1$


    public static final SimpleAttributeDefinition DBNAME = new SimpleAttributeDefinition("vdb-name", ModelType.STRING, true); //$NON-NLS-1$
    public static final SimpleAttributeDefinition VERSION = new SimpleAttributeDefinition("vdb-version", ModelType.STRING, true); //$NON-NLS-1$
    public static final SimpleAttributeDefinition SCHEMA = new SimpleAttributeDefinition("schema", ModelType.STRING, true); //$NON-NLS-1$
    public static final SimpleAttributeDefinition DDL = new SimpleAttributeDefinition("ddl", ModelType.STRING, false); //$NON-NLS-1$
    public static final SimpleAttributeDefinition PERSIST = new SimpleAttributeDefinition("persist", ModelType.BOOLEAN, false); //$NON-NLS-1$

    public static final SimpleAttributeDefinition INCLUDE_SCHEMA = new SimpleAttributeDefinition("include-schema", ModelType.BOOLEAN, true); //$NON-NLS-1$
}
