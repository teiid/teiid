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

package org.teiid.translator.jdbc;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.teiid.language.Argument;
import org.teiid.language.Call;
import org.teiid.language.Command;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ProcedureExecution;
import org.teiid.translator.TranslatorException;

/**
 */
public class JDBCProcedureExecution extends JDBCQueryExecution implements ProcedureExecution {

    public JDBCProcedureExecution(Command command, Connection connection, ExecutionContext context, JDBCExecutionFactory env) {
        super(command, connection, context, env);
    }

    @Override
    public void execute() throws TranslatorException {
        Call procedure = (Call)command;
        columnDataTypes = procedure.getResultSetColumnTypes();

        //translate command
        TranslatedCommand translatedComm = translateCommand(procedure);

        //create statement or CallableStatement and execute
        String sql = translatedComm.getSql();
        try{
            //create parameter index map
            CallableStatement cstmt = getCallableStatement(sql);
            this.results = this.executionFactory.executeStoredProcedure(cstmt, translatedComm.getPreparedValues(), procedure.getReturnType());
            addStatementWarnings();
        }catch(SQLException e){
             throw new TranslatorException(JDBCPlugin.Event.TEIID11004, e, JDBCPlugin.Util.gs(JDBCPlugin.Event.TEIID11004, sql));
        }

    }

    @Override
    public List<?> next() throws TranslatorException, DataNotAvailableException {
        if (results == null) {
            return null;
        }
        return super.next();
    }

    @Override
    public List<?> getOutputParameterValues() throws TranslatorException {
        try {
            Call proc = (Call)this.command;
            List<Object> result = new ArrayList<Object>();
            int paramIndex = 1;
            if (proc.getReturnType() != null) {
                if (proc.getReturnParameter() != null) {
                    addParameterValue(result, paramIndex, proc.getReturnType());
                }
                paramIndex++;
            }
            for (Argument parameter : proc.getArguments()) {
                switch (parameter.getDirection()) {
                case IN:
                    paramIndex++;
                    break;
                case INOUT:
                case OUT:
                    addParameterValue(result, paramIndex++, parameter.getType());
                    break;
                }
            }
            return result;
        } catch (SQLException e) {
             throw new TranslatorException(JDBCPlugin.Event.TEIID11005, e);
        }
    }

    private void addParameterValue(List<Object> result, int paramIndex,
            Class<?> type) throws SQLException {
        Object value = this.executionFactory.retrieveValue((CallableStatement)this.statement, paramIndex, type);
        result.add(value);
    }

}
