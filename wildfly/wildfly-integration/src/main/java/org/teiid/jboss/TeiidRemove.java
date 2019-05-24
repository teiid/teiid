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

import org.jboss.as.controller.AbstractRemoveStepHandler;
import org.jboss.as.controller.OperationContext;
import org.jboss.as.controller.OperationFailedException;
import org.jboss.dmr.ModelNode;

class TeiidRemove extends AbstractRemoveStepHandler {
    public static TeiidRemove INSTANCE = new TeiidRemove();

    @Override
    protected void performRuntime(OperationContext context,
            final ModelNode operation, final ModelNode model)
            throws OperationFailedException {

        context.removeService(TeiidServiceNames.PREPAREDPLAN_CACHE_FACTORY);
        context.removeService(TeiidServiceNames.RESULTSET_CACHE_FACTORY);
        context.removeService(TeiidServiceNames.AUTHORIZATION_VALIDATOR);
        context.removeService(TeiidServiceNames.PREPARSER);
        context.removeService(TeiidServiceNames.EVENT_DISTRIBUTOR_FACTORY);

        context.removeService(TeiidServiceNames.ENGINE);
        context.removeService(TeiidServiceNames.CACHE_PREPAREDPLAN);
        context.removeService(TeiidServiceNames.CACHE_RESULTSET);
        context.removeService(TeiidServiceNames.TUPLE_BUFFER);
        context.removeService(TeiidServiceNames.BUFFER_MGR);
        context.removeService(TeiidServiceNames.BUFFER_DIR);
        context.removeService(TeiidServiceNames.OBJECT_SERIALIZER);
        context.removeService(TeiidServiceNames.VDB_STATUS_CHECKER);
        context.removeService(TeiidServiceNames.VDB_REPO);
        context.removeService(TeiidServiceNames.TRANSLATOR_REPO);
    }
}
