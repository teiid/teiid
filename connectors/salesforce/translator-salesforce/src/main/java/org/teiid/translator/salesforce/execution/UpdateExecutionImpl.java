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

import java.util.ArrayList;
import java.util.List;

import org.teiid.language.ColumnReference;
import org.teiid.language.Command;
import org.teiid.language.Literal;
import org.teiid.language.SetClause;
import org.teiid.language.Update;
import org.teiid.metadata.Column;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.salesforce.SalesForceExecutionFactory;
import org.teiid.translator.salesforce.SalesforceConnection;
import org.teiid.translator.salesforce.Util;
import org.teiid.translator.salesforce.execution.visitors.IQueryProvidingVisitor;
import org.teiid.translator.salesforce.execution.visitors.UpdateVisitor;


public class UpdateExecutionImpl extends AbstractUpdateExecution {

    public UpdateExecutionImpl(SalesForceExecutionFactory ef, Command command,
            SalesforceConnection salesforceConnection,
            RuntimeMetadata metadata, ExecutionContext context) {
        super(ef, command, salesforceConnection, metadata, context);
    }

    @Override
    public void execute() throws TranslatorException {
        UpdateVisitor visitor = new UpdateVisitor(getMetadata());
        visitor.visit((Update)command);
        execute(((Update)command).getWhere(), visitor);
    }

    @Override
    protected int processIds(String[] ids, IQueryProvidingVisitor visitor)
            throws TranslatorException {
        List<DataPayload> updateDataList = new ArrayList<DataPayload>();

        for (int i = 0; i < ids.length; i++) {
            DataPayload data = new DataPayload();

            for (SetClause clause : ((Update)command).getChanges()) {
                ColumnReference element = clause.getSymbol();
                Column column = element.getMetadataObject();
                Literal l = (Literal) clause.getValue();
                Object value = Util.toSalesforceObjectValue(l.getValue(),  l.getType());
                data.addField(column.getSourceName(), value);
            }

            data.setType(visitor.getTableName());
            data.setID(ids[i]);
            updateDataList.add(data);
        }

        return getConnection().update(updateDataList);
    }
}
