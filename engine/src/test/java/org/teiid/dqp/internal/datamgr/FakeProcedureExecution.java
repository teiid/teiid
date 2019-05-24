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

package org.teiid.dqp.internal.datamgr;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.teiid.translator.TranslatorException;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ProcedureExecution;


final class FakeProcedureExecution implements ProcedureExecution {

    int resultSetSize;
    int rowNum;
    int paramSize;

    public FakeProcedureExecution(int resultSetSize, int paramSize) {
        this.resultSetSize = resultSetSize;
        this.paramSize = paramSize;
    }

    @Override
    public void execute() throws TranslatorException {

    }

    @Override
    public List<?> getOutputParameterValues() throws TranslatorException {
        List<Object> result = new ArrayList<Object>(paramSize);
        for (int i = 0; i < paramSize; i++) {
            result.add(i);
        }
        return result;
    }

    public void close() {
    }

    public void cancel() throws TranslatorException {
    }

    @Override
    public List next() throws TranslatorException, DataNotAvailableException {
        if (rowNum == 1) {
            return null;
        }
        rowNum++;
        return Arrays.asList(new Object[resultSetSize]);
    }

}