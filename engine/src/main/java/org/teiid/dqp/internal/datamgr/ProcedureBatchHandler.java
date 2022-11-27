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

/**
 *
 */
package org.teiid.dqp.internal.datamgr;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.teiid.language.Argument;
import org.teiid.language.Call;
import org.teiid.language.Argument.Direction;
import org.teiid.query.QueryPlugin;
import org.teiid.translator.ProcedureExecution;
import org.teiid.translator.TranslatorException;


class ProcedureBatchHandler {
    private Call proc;
    private ProcedureExecution procExec;
    private int paramCols = 0;
    private int resultSetCols = 0;
    private List<?> filler;

    public ProcedureBatchHandler(Call proc, ProcedureExecution procExec) {
        this.proc = proc;
        this.procExec = procExec;
        List<Argument> params = proc.getArguments();
        resultSetCols = proc.getResultSetColumnTypes().length;
        if (proc.getReturnType() != null) {
            paramCols++;
        }
        if(params != null && !params.isEmpty()){
            for (Argument param : params) {
                if(param.getDirection() == Direction.OUT || param.getDirection() == Direction.INOUT){
                    paramCols += 1;
                }
            }
        }
        if (paramCols > 0) {
            filler = Collections.nCopies(paramCols, null);
        }
    }

    List<?> padRow(List<?> row) throws TranslatorException {
        if (row.size() != resultSetCols) {
             throw new TranslatorException(QueryPlugin.Event.TEIID30479, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30479, proc, new Integer(resultSetCols), new Integer(row.size())));
        }
        if (paramCols == 0) {
            return row;
        }
        List<Object> result = new ArrayList<Object>(resultSetCols + paramCols);
        result.addAll(row);
        result.addAll(filler);
        return result;
    }

    List<?> getParameterRow() throws TranslatorException {
        if (paramCols == 0) {
            return null;
        }
        List<Object> result = new ArrayList<Object>(Arrays.asList(new Object[resultSetCols]));
        result.addAll(procExec.getOutputParameterValues());
        return result;
    }

}
