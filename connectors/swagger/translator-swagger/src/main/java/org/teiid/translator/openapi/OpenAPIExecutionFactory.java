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

package org.teiid.translator.openapi;

import java.util.Collections;
import java.util.List;

import org.teiid.language.Call;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.resource.api.ConnectionFactory;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.MetadataProcessor;
import org.teiid.translator.ProcedureExecution;
import org.teiid.translator.Translator;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.swagger.SwaggerProcedureExecution;
import org.teiid.translator.ws.WSConnection;

@Translator(name="openapi", description="A translator for making openapi based data service call")
public class OpenAPIExecutionFactory extends ExecutionFactory<ConnectionFactory, WSConnection> {

    public OpenAPIExecutionFactory() {
        setSourceRequiredForMetadata(true);
        setSupportsOrderBy(false);
        setSupportsSelectDistinct(false);
        setSupportsInnerJoins(false);
        setSupportsFullOuterJoins(false);
        setSupportsOuterJoins(false);
    }

    @Override
    public ProcedureExecution createProcedureExecution(Call command,
            ExecutionContext executionContext, RuntimeMetadata metadata,
            WSConnection connection) throws TranslatorException {
        return new SwaggerProcedureExecution(command, this, executionContext, metadata, connection);
    }

    @Override
    public final List<String> getSupportedFunctions() {
        return Collections.emptyList();
    }

    @Override
    public MetadataProcessor<WSConnection> getMetadataProcessor() {
        return new OpenAPIMetadataProcessor(this);
    }
}
