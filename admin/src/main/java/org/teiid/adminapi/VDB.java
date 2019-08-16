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

package org.teiid.adminapi;

import java.util.List;

/**
 * Represents a Virtual Database in the Teiid System.
 * <br>A VDB has a name and a version.
 *
 * <p>The identifier pattern for a VDB is name.version,
 * where the name of the VDB and its version represent its unique identifier in the Teiid system.
 * There are no spaces allowed in a given VDB name, and VDB name must start with a letter.
 * A version number is automatically assigned to a VDB when it is deployed into
 * a system. A VDB is uniquely identified by name.version".
 * For example: "Accounts.1", "UnifiedSales.4", etc.
 *
 *
 * @since 4.3
 */
public interface VDB extends AdminObject, DomainAware {

    public enum Status{
        /**
         * Initial state waiting for metadata to load
         */
        LOADING,
        /**
         * In the vdb repository and querable, but not necessarily valid
         */
        ACTIVE,
        /**
         * A vdb that cannot be successfully loaded - and cannot later transition to active
         */
        FAILED,
        REMOVED
    };

    public enum ConnectionType {NONE, BY_VERSION, ANY}

    /**
     * @return Collection of  Teiid Models
     */
    public List<Model> getModels();

    /**
     * @return the status
     */
    public Status getStatus();

    /**
     * @return the connection status
     */
    public ConnectionType getConnectionType();

    /**
     * @return the VDB version.
     */
    public String getVersion();

    /**
     * Get the description of the VDB
     * @return
     */
    public String getDescription();

    /**
     * Shows any validity errors present in the VDB
     * @return
     */
    public List<String> getValidityErrors();

    /**
     * Shows if VDB is a valid entity
     * @return
     */
    public boolean isValid();

    /**
     * Get the data roles defined on this VDB
     * @return
     */
    public List<DataPolicy> getDataPolicies();

    /**
     * Get the list of translators defined in the VDB
     * @return
     */
    public List<Translator> getOverrideTranslators();

    /**
     * Get the list of vdb imports
     * @return
     */
    public List<? extends VDBImport> getVDBImports();

    /**
     * Get the list of other resources included in the VDB
     * @return
     */
    public List<? extends Entry> getEntries();

    /**
     * Whether the model is visible
     * @param modelName
     * @return
     */
    boolean isVisible(String modelName);

    /**
     * @return the name of the vdb.  If this vdb is using semantic versioning, that version will be included in the name.
     */
    @Override
    public String getName();
}
