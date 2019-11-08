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

package org.teiid;

import java.util.Set;

import org.teiid.adminapi.DataPolicy.Context;
import org.teiid.adminapi.DataPolicy.PermissionType;
import org.teiid.metadata.AbstractMetadataRecord;

/**
 * A policy decider that reports authorization decisions for further action.
 * A decider may be called many times for a single user command.  Typically there will be 1 call for every
 * command/subquery/temp table access/function call.
 */
public interface PolicyDecider {

    /**
     * Called by the system hasRole function to determine role membership.
     * @param roleName
     * @param context
     * @return true if the user has the given role name, otherwise false
     */
    boolean hasRole(String roleName, CommandContext context);

    /**
     * Returns the set of resources not allowed to be accessed by the current user.
     * @param action if context is METADATA, then action execute means a procedure or function, and read some other resource
     * @param resources
     * @param context in which the action is performed.
     *   For example you can have a context of {@link Context#UPDATE} for a {@link PermissionType#READ} for columns used in an UPDATE condition.
     * @param commandContext
     * @return the set of inaccessible resources, never null
     */
    Set<AbstractMetadataRecord> getInaccessibleResources(PermissionType action,
            Set<AbstractMetadataRecord> resources, Context context,
            CommandContext commandContext);


    /**
     * Returns true if the given language is allowed
     * @param language
     * @param commandContext
     * @return
     */
    boolean isLanguageAllowed(String language, CommandContext commandContext);

    /**
     * Checks if the temp table are accessible.  Typically as long as temp tables can be created, all operations are allowed.
     * @param action
     * @param resource will be null for general temp access
     * @param context in which the action is performed.
     *   For example you can have a context of {@link Context#UPDATE} for a {@link PermissionType#READ} for columns used in an UPDATE condition.
     * @param commandContext
     * @return true if the access is allowed, otherwise false
     */
    boolean isTempAccessible(PermissionType action, AbstractMetadataRecord resource,
            Context context, CommandContext commandContext);

    /**
     * Determines if an authorization check should proceed
     * @param commandContext
     * @return
     */
    boolean validateCommand(CommandContext commandContext);

}
