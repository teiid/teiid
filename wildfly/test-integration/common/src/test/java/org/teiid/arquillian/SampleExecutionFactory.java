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

package org.teiid.arquillian;

import java.util.Arrays;
import java.util.List;

import org.teiid.language.Command;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.metadata.Table;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.Execution;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.loopback.LoopbackExecution;
import org.teiid.translator.loopback.LoopbackExecutionFactory;

@org.teiid.translator.Translator(name = "loopy")
@SuppressWarnings("nls")
public class SampleExecutionFactory extends LoopbackExecutionFactory {
    static int metadataloaded = 0; // use of static is bad, but we instantiate a separate translator for each vdb load

    public SampleExecutionFactory() {
        setSupportsSelectDistinct(true);
        setWaitTime(10);
        setRowCount(200);
        setSourceRequiredForMetadata(false);
        setSourceRequired(false);
    }

    @Override
    public void getMetadata(MetadataFactory metadataFactory, Object conn) throws TranslatorException {
        super.getMetadata(metadataFactory, conn);
        metadataloaded++;

        Table t = metadataFactory.addTable("Matadata");
        metadataFactory.addColumn("execCount", "integer", t);
    }

    @Override
    public boolean isSourceRequired() {
        return false;
    }

    @Override
    public Execution createExecution(Command command, ExecutionContext executionContext, RuntimeMetadata metadata, Object connection)
            throws TranslatorException {
        if (command.toString().equals("SELECT g_0.execCount FROM Matadata AS g_0")) { //$NON-NLS-1$
            return new ResultSetExecution() {
                boolean served = false;
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
                    if (!served) {
                        served = true;
                        return Arrays.asList(metadataloaded);
                    }
                    return null;
                }
            };
        }
        return new LoopbackExecution(command, this, executionContext);
    }

}