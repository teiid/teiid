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
package org.teiid.translator.phoenix;

import java.sql.Connection;
import java.sql.SQLException;

import org.teiid.language.Command;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.jdbc.JDBCExecutionException;
import org.teiid.translator.jdbc.JDBCUpdateExecution;
import org.teiid.translator.jdbc.TranslatedCommand;

public class PhoenixUpdateExecution extends JDBCUpdateExecution {

    private TranslatedCommand translatedComm = null;

    public PhoenixUpdateExecution(Command command,
                                ExecutionContext executionContext,
                                RuntimeMetadata metadata,
                                Connection conn,
                                PhoenixExecutionFactory executionFactory) throws TranslatorException {
        super(command, conn, executionContext, executionFactory);

        setCommitMode(conn);

        translatedComm = translateCommand(command);

    }

    // By default, Phoenix Connection AutoCommit is false, doesn't like other vendors
    private void setCommitMode(Connection conn) throws JDBCExecutionException {
        try {
            if(!conn.getAutoCommit()) {
                conn.setAutoCommit(true);
            }
        } catch (SQLException e) {
            throw new JDBCExecutionException(PhoenixPlugin.Event.TEIID27003, e, command.toString());
        }
    }

    @Override
    protected TranslatedCommand translateCommand(Command command) throws TranslatorException {

        if(null == translatedComm) {
            translatedComm = super.translateCommand(command);
        }

        return translatedComm ;
    }

}
