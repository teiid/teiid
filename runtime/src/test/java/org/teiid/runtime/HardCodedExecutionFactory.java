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

package org.teiid.runtime;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import org.teiid.language.Call;
import org.teiid.language.Command;
import org.teiid.language.QueryExpression;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.ProcedureExecution;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.Translator;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.UpdateExecution;

@Translator(name="hardcoded")
public class HardCodedExecutionFactory extends ExecutionFactory<Object, Object> {
    Map<String, List<? extends List<?>>> dataMap = new ConcurrentHashMap<String, List<? extends List<?>>>();
    Map<String, Object> updateMap = new ConcurrentHashMap<String, Object>();

    private List<Command> commands = new CopyOnWriteArrayList<Command>();

    public HardCodedExecutionFactory() {
        setSourceRequired(false);
    }

    @Override
    public ProcedureExecution createProcedureExecution(Call command,
            ExecutionContext executionContext, RuntimeMetadata metadata,
            Object connection) throws TranslatorException {
        this.commands.add(command);
        List<? extends List<?>> list = getData(command);
        if (list == null) {
            throw new RuntimeException(command.toString());
        }
        final Iterator<? extends List<?>> result = list.iterator();
        return new ProcedureExecution() {

            @Override
            public void execute() throws TranslatorException {

            }

            @Override
            public void close() {

            }

            @Override
            public void cancel() throws TranslatorException {

            }

            @Override
            public List<?> next() throws TranslatorException, DataNotAvailableException {
                if (result.hasNext()) {
                    return result.next();
                }
                return null;
            }

            @Override
            public List<?> getOutputParameterValues()
                    throws TranslatorException {
                return null;
            }
        };
    }

    @Override
    public ResultSetExecution createResultSetExecution(
            final QueryExpression command, ExecutionContext executionContext,
            RuntimeMetadata metadata, Object connection)
            throws TranslatorException {
        this.commands.add(command);
        List<? extends List<?>> list = getData(command);
        if (list == null) {
            throw new RuntimeException(command.toString());
        }
        final Iterator<? extends List<?>> result = list.iterator();
        return new ResultSetExecution() {

            @Override
            public void execute() throws TranslatorException {

            }

            @Override
            public void close() {

            }

            @Override
            public void cancel() throws TranslatorException {

            }

            @Override
            public List<?> next() throws TranslatorException, DataNotAvailableException {
                if (result.hasNext()) {
                    return result.next();
                }
                return null;
            }
        };
    }

    protected List<? extends List<?>> getData(final QueryExpression command) {
        return getData((Command)command);
    }

    protected List<? extends List<?>> getData(final Command command) {
        return dataMap.get(command.toString());
    }

    @Override
    public UpdateExecution createUpdateExecution(Command command,
            ExecutionContext executionContext, RuntimeMetadata metadata,
            Object connection) throws TranslatorException {
        this.commands.add(command);
        Object response = updateMap.get(command.toString());
        if (response == null) {
            throw new RuntimeException(command.toString());
        }
        if (response instanceof int[]) {
            final int[] result = (int[])response;
            return new UpdateExecution() {

                @Override
                public void execute() throws TranslatorException {

                }

                @Override
                public void close() {

                }

                @Override
                public void cancel() throws TranslatorException {

                }

                @Override
                public int[] getUpdateCounts() throws DataNotAvailableException,
                        TranslatorException {
                    return result;
                }

            };
        }
        throw (TranslatorException)response;
    }

    public void addData(String key, List<? extends List<?>> list) {
        this.dataMap.put(key, list);
    }

    public void addUpdate(String key, int[] counts) {
        this.updateMap.put(key, counts);
    }

    public void addUpdate(String key, TranslatorException exception) {
        this.updateMap.put(key, exception);
    }

    public List<Command> getCommands() {
        return commands;
    }

    public void clearData() {
        this.dataMap.clear();
    }

}