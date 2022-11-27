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

public interface DataPolicy {

    public enum Context {
        CREATE,
        DROP,
        QUERY,
        INSERT,
        MERGE,
        UPDATE,
        DELETE,
        FUNCTION,
        ALTER,
        STORED_PROCEDURE,
        METADATA;
    }

    public enum PermissionType {CREATE, READ, UPDATE, DELETE, ALTER, EXECUTE, DROP, LANGUAGE};

    public enum ResourceType {DATABASE, SCHEMA, PROCEDURE, TABLE, FUNCTION, COLUMN, LANGUAGE};

    /**
     * Get the Name of the Data Policy
     * @return
     */
    String getName();

    /**
     * Get the description of the Data Policy
     * @return
     */
    String getDescription();

    /**
     * Get the List of Permissions for this Data Policy.
     * @return
     */
    List<DataPermission> getPermissions();

    /**
     * Mapped Container Role names for this Data Policy
     * @return
     */
    List<String> getMappedRoleNames();

    /**
     * If the policy applies to any authenticated user
     * @return
     */
    boolean isAnyAuthenticated();

    /**
     * If the policy grants all permissions
     * @return
     */
    boolean isGrantAll();

    /**
     * If the policy allows for temporary table usage
     * @return
     */
    Boolean isAllowCreateTemporaryTables();

    interface DataPermission {
        /**
         * Get the Resource Name that the Data Permission represents
         * @return
         */
        String getResourceName();

        /**
         * Get the type of resource the Data Permission is represents
         * @return
         */
        ResourceType getResourceType();

        /**
         * Is "CREATE" allowed?
         * @return
         */
        Boolean getAllowCreate();

        /**
         * Is "SELECT" allowed?
         * @return
         */
        Boolean getAllowRead();

        /**
         * Is "INSERT/UPDATE" allowed?
         * @return
         */
        Boolean getAllowUpdate();

        /**
         * Is "DELETE" allowed?
         * @return
         */
        Boolean getAllowDelete();

        /**
         * Is "ALTER" allowed?
         * @return
         */
        Boolean getAllowAlter();

        /**
         * Is "EXECUTE" allowed?
         * @return
         */
        Boolean getAllowExecute();

        /**
         * Is "LANGUAGE" allowed?
         * @return
         */
        Boolean getAllowLanguage();

        /**
         * The condition string
         */
        String getCondition();

        /**
         * The column mask string
         */
        String getMask();

        /**
         * The column mask order
         */
        Integer getOrder();

        /**
         * If the condition acts as a constraint.
         */
        Boolean getConstraint();

    }
}
