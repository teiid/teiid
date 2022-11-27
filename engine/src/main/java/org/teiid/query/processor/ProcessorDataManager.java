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

package org.teiid.query.processor;

import org.teiid.common.buffer.BlockedException;
import org.teiid.common.buffer.TupleSource;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.events.EventDistributor;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.util.CommandContext;


public interface ProcessorDataManager {

    TupleSource registerRequest(CommandContext context, Command command, String modelName, RegisterRequestParameter parameterObject)
        throws TeiidComponentException, TeiidProcessingException;

    /**
     * Lookup a value from a cached code table.  If the code table is not loaded, it will be
     * loaded on the first query.  Code tables should be cached based on a combination of
     * the codeTableName, returnElementName, and keyElementName.  If the table is not loaded,
     * a request will be made and the method should throw a BlockedException.
     */
    Object lookupCodeValue(CommandContext context,
                                           String codeTableName,
                                           String returnElementName,
                                           String keyElementName,
                                           Object keyValue) throws BlockedException,
                                                           TeiidComponentException, TeiidProcessingException;

    EventDistributor getEventDistributor();
}
