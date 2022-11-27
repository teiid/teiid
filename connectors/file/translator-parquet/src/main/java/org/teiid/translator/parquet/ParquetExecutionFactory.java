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

package org.teiid.translator.parquet;

import org.teiid.file.VirtualFileConnection;
import org.teiid.language.QueryExpression;
import org.teiid.language.Select;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.resource.api.ConnectionFactory;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.MetadataProcessor;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.Translator;
import org.teiid.translator.TranslatorException;

@Translator(name="parquet", description="Parquet file translator")
public class ParquetExecutionFactory extends ExecutionFactory<ConnectionFactory, VirtualFileConnection> {

    public ParquetExecutionFactory() {
        setSourceRequiredForMetadata(true);
        setTransactionSupport(TransactionSupport.NONE);
    }

    @Override
    public ResultSetExecution createResultSetExecution(QueryExpression command, ExecutionContext executionContext, RuntimeMetadata metadata, VirtualFileConnection connection)
            throws TranslatorException {
        ParquetExecution ex = new ParquetExecution((Select)command, executionContext, metadata, connection, this.isImmutable());
        return ex;
    }


    @Override
    public MetadataProcessor<VirtualFileConnection> getMetadataProcessor(){
        return new ParquetMetadataProcessor();
    }

    @Override
    public boolean supportsCompareCriteriaEquals() {
        return true;
    }

    @Override
    public boolean supportsOnlyLiteralComparison() {
        return true;
    }

    @Override
    public boolean supportsCompareCriteriaOrdered() {
        return true;
    }

    @Override
    public boolean supportsNotCriteria() {
        return true;
    }

    @Override
    public boolean supportsOrCriteria() {
        return true;
    }

    @Override
    public boolean supportsIsNullCriteria() {
        return true;
    }
}
