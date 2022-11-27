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

package org.teiid.query.resolver;

import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryResolverException;
import org.teiid.core.TeiidComponentException;
import org.teiid.query.metadata.TempMetadataAdapter;
import org.teiid.query.sql.lang.Command;


/**
 * This is the interface that is implemented for each type of command, telling how
 * to resolve that command.
 */
public interface CommandResolver {

    /**
     * Resolve the command using the metadata.
     * @param command The command to resolve
     * @param metadata Metadata
     * @param resolveNullLiterals true if the resolver should consider replacing null literals with more appropriate types
     * @throws QueryMetadataException If there is a metadata problem
     * @throws QueryResolverException If the query cannot be resolved
     * @throws TeiidComponentException If there is an internal error
     */
    void resolveCommand(Command command, TempMetadataAdapter metadata, boolean resolveNullLiterals)
    throws QueryMetadataException, QueryResolverException, TeiidComponentException;

}
