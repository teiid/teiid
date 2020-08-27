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

package org.teiid.translator.salesforce.execution;

import org.teiid.language.Command;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.salesforce.SalesForceExecutionFactory;
import org.teiid.translator.salesforce.SalesforceConnection;

import com.sforce.async.OperationEnum;

public class DeleteExecutionImpl extends DeleteUpdateExecutionImpl {

    public DeleteExecutionImpl(SalesForceExecutionFactory ef, Command command,
            SalesforceConnection salesforceConnection,
            RuntimeMetadata metadata, ExecutionContext context) {
        super(ef, command, salesforceConnection, metadata, context);
    }

    @Override
    protected OperationEnum getOperation() {
        return (executionFactory.useHardDelete() || (context.getSourceHint() != null && context
                .getSourceHint().contains("hardDelete"))) //$NON-NLS-1$
                ? OperationEnum.hardDelete
                : OperationEnum.delete;
    }

    @Override
    protected int syncProcessIds(String[] ids) throws TranslatorException {
        return connection.delete(ids);
    }

}
