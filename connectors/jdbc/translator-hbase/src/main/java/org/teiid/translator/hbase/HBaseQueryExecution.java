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
package org.teiid.translator.hbase;

import java.sql.Connection;

import org.teiid.language.Command;
import org.teiid.language.QueryExpression;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.jdbc.JDBCQueryExecution;
import org.teiid.translator.jdbc.TranslatedCommand;

public class HBaseQueryExecution extends JDBCQueryExecution {
    
    
    private TranslatedCommand translatedComm = null;
    

    public HBaseQueryExecution(QueryExpression command, 
                               ExecutionContext executionContext, 
                               RuntimeMetadata metadata,
                               Connection conn, 
                               HBaseExecutionFactory executionFactory) throws TranslatorException {
        super(command, conn, executionContext, executionFactory);
        
        translatedComm = translateCommand(command);
    }
    
    

    @Override
    protected TranslatedCommand translateCommand(Command command) throws TranslatorException {
        
        if(null == translatedComm) {
            translatedComm = super.translateCommand(command);
        }
        
        return translatedComm ;
    }

}
