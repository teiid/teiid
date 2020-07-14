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

package org.teiid.translator.excel;

import org.apache.poi.ss.usermodel.DataFormatter;
import org.teiid.file.VirtualFileConnection;
import org.teiid.language.Command;
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
import org.teiid.translator.TranslatorProperty;

@Translator(name="excel", description="Excel file translator")
public class ExcelExecutionFactory extends ExecutionFactory<ConnectionFactory, VirtualFileConnection> {

    private boolean formatStrings;

    public ExcelExecutionFactory() {
        setSourceRequiredForMetadata(true);
        setTransactionSupport(TransactionSupport.NONE);
    }

    @Override
    public ResultSetExecution createResultSetExecution(QueryExpression command, ExecutionContext executionContext, RuntimeMetadata metadata, VirtualFileConnection connection)
            throws TranslatorException {
        ExcelExecution ex = new ExcelExecution((Select)command, executionContext, metadata, connection, this.isImmutable());
        if (formatStrings) {
            ex.setDataFormatter(new DataFormatter()); //assume default locale
        }
        return ex;
    }

    @Override
    public ExcelUpdateExecution createUpdateExecution(Command command,
            ExecutionContext executionContext, RuntimeMetadata metadata,
            VirtualFileConnection connection) throws TranslatorException {
        ExcelUpdateExecution ex = new ExcelUpdateExecution(command, executionContext, metadata, connection, this.isImmutable());
        if (formatStrings) {
            ex.setDataFormatter(new DataFormatter()); //assume default locale
        }
        return ex;
    }

    @Override
    public MetadataProcessor<VirtualFileConnection> getMetadataProcessor(){
        return new ExcelMetadataProcessor();
    }

    @Override
    public boolean supportsCompareCriteriaEquals() {
        return true; // only on ROW_ID
    }

    @Override
    public boolean supportsCompareCriteriaOrdered() {
        return true; //Only on ROW_ID
    }

    @Override
    public boolean supportsOnlyLiteralComparison() {
        return true;
    }

    @Override
    public boolean supportsInCriteria() {
        return true;
    }

    @TranslatorProperty(display="Format Strings", description="Format non-string cell values in a string column according to the worksheet format.", advanced=true)
    public boolean isFormatStrings() {
        return formatStrings;
    }

    public void setFormatStrings(boolean formatStrings) {
        this.formatStrings = formatStrings;
    }
}
