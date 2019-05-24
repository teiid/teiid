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

package org.teiid.dqp.internal.process;

import org.teiid.api.exception.query.QueryValidatorException;
import org.teiid.core.TeiidComponentException;
import org.teiid.metadata.AbstractMetadataRecord;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.util.CommandContext;

/**
 * Defines a validator that checks for proper authorization.
 */
public interface AuthorizationValidator {

    enum CommandType {
        USER,
        PREPARED,
        CACHED
    }

    /**
     * Validates the given command.  If the command is not a {@link CommandType#USER} command, the command object should not be modified.
     * Any modification must be fully resolved using the associated {@link QueryMetadataInterface}.  Returning true for a
     *  {@link CommandType#PREPARED} or  {@link CommandType#CACHED} commands means that the matching prepared plan or cache entry
     *  will not be used.
     * @param originalSql array of commands will typically contain only a single string, but may have multiple for batched updates.
     * @param command the parsed and resolved command.
     * @param metadata
     * @param commandContext
     * @param commandType
     * @return true if the USER command was modified, or if the non-USER command should be modified.
     * @throws QueryValidatorException
     * @throws TeiidComponentException
     */
    boolean validate(String[] originalSql, Command command, QueryMetadataInterface metadata, CommandContext commandContext, CommandType commandType) throws QueryValidatorException, TeiidComponentException;

    /**
     * Uses the context or other information to determine if the current user has the given role name.
     * @param roleName
     * @param commandContext
     * @return true if the current user has the given role
     */
    boolean hasRole(String roleName, CommandContext commandContext);

    /**
     * Determines if the metadata record is accessible in system queries
     * @param record
     * @param commandContext
     * @return
     */
    boolean isAccessible(AbstractMetadataRecord record, CommandContext commandContext);
}