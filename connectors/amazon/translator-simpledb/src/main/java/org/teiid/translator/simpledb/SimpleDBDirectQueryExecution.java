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

/*
 */

package org.teiid.translator.simpledb;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.teiid.language.Argument;
import org.teiid.language.Command;
import org.teiid.language.Select;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ProcedureExecution;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.simpledb.api.SimpleDBConnection;

import com.amazonaws.services.simpledb.model.Item;


public class SimpleDBDirectQueryExecution extends SimpleDBQueryExecution implements ProcedureExecution {

    protected int columnCount;
    private List<Argument> arguments;
    protected int updateCount = -1;
    private String sourceSQL;

    public SimpleDBDirectQueryExecution(List<Argument> arguments,
            Command command, RuntimeMetadata metadata,
            SimpleDBConnection connection, ExecutionContext context)
            throws TranslatorException {

        super((Select)command, context, metadata, connection);
        this.arguments = arguments;
    }

    @Override
    public void execute() throws TranslatorException {
        this.sourceSQL = (String) this.arguments.get(0).getArgumentValue().getValue();
        List<Argument> parameters = this.arguments.subList(1, this.arguments.size());

        LogManager.logTrace(LogConstants.CTX_CONNECTOR, "Source sql", sourceSQL); //$NON-NLS-1$
        int paramCount = parameters.size();
        if (paramCount > 0){
            throw new TranslatorException(SimpleDBPlugin.Event.TEIID24003, SimpleDBPlugin.Util.gs(SimpleDBPlugin.Event.TEIID24003));
        }

        if (!sourceSQL.toLowerCase().startsWith("select")) { //$NON-NLS-1$
            throw new TranslatorException(SimpleDBPlugin.Event.TEIID24002, SimpleDBPlugin.Util.gs(SimpleDBPlugin.Event.TEIID24002));
        }

        executeDirect(getSQL(), null);
    }

    @Override
    protected String getSQL() {
        return this.sourceSQL;
    }

    @Override
    protected List<?> buildRow(Item item) throws TranslatorException {
        Map<String, List<String>> valueMap = createAttributeMap(item.getAttributes());
        List row = new ArrayList();
        Object[] results = new Object[valueMap.size()];
        int i = 0;
        for (String attributeName:valueMap.keySet()) {
            if (SimpleDBMetadataProcessor.isItemName(attributeName)) {
                results[i++] = item.getName();
                continue;
            }
            List<String> value = valueMap.get(attributeName);
            results[i++] = (value.size() == 1)?value.get(0):value.toString();
        }
        row.add(results);
        return row;
    }

    @Override
    public List<?> getOutputParameterValues() throws TranslatorException {
        return null;
    }
}
