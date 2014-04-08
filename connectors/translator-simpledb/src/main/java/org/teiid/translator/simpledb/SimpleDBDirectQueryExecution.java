/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
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
import org.teiid.resource.adpter.simpledb.SimpleDBConnection;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ProcedureExecution;
import org.teiid.translator.TranslatorException;

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
