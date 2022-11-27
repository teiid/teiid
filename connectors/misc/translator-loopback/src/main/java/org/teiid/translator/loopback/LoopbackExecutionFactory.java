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

package org.teiid.translator.loopback;

import org.teiid.core.util.ApplicationInfo;
import org.teiid.language.Command;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.BaseDelegatingExecutionFactory;
import org.teiid.translator.Execution;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.Translator;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TranslatorProperty;
import org.teiid.translator.jdbc.teiid.TeiidExecutionFactory;

/**
 * Loopback translator.
 */
@Translator(name="loopback", description="A translator for testing, that returns mock data")
public class LoopbackExecutionFactory extends BaseDelegatingExecutionFactory {

    private int waitTime = 0;
    private int rowCount = 1;
    private boolean throwError = false;
    private long pollIntervalInMilli = -1;
    private boolean incrementRows = false;
    private int charValueSize = 10;

    public LoopbackExecutionFactory() {
        TeiidExecutionFactory tef = new TeiidExecutionFactory();
        tef.setDatabaseVersion(ApplicationInfo.getInstance().getReleaseNumber());
        this.setDelegate(tef);
    }

    @Override
    public void start() throws TranslatorException {
        if (this.getDelegateName() == null && this.getDelegate() != null) {
            this.getDelegate().start();
        }
        super.start();
    }

    @TranslatorProperty(display="Size of values for CLOB, VARCHAR, etc.", advanced=true)
    public int getCharacterValuesSize() {
        return charValueSize;
    }

    public void setCharacterValuesSize(int charValSize){
        this.charValueSize = charValSize;
    }

    @TranslatorProperty(display="If set to true each value in each column is being incremented with each row", advanced=true)
    public boolean getIncrementRows() {
        return incrementRows;
    }

    public void setIncrementRows(boolean incrementRows) {
        this.incrementRows = incrementRows;
    }

    @Override
    public Object getConnection(Object factory) throws TranslatorException {
        return null;
    }

    @Override
    public Object getConnection(Object factory,
            ExecutionContext executionContext) throws TranslatorException {
        return null;
    }

    @TranslatorProperty(display="Max Random Wait Time", advanced=true)
    public int getWaitTime() {
        return waitTime;
    }

    public void setWaitTime(int waitTime) {
        this.waitTime = waitTime;
    }

    @TranslatorProperty(display="Rows Per Query", advanced=true)
    public int getRowCount() {
        return rowCount;
    }

    public void setRowCount(int rowCount) {
        this.rowCount = rowCount;
    }

    @TranslatorProperty(display="Always Throw Error")
    public boolean isThrowError() {
        return this.throwError;
    }

    public void setThrowError(boolean error) {
        this.throwError = error;
    }

    @TranslatorProperty(display="Poll interval if using a Asynchronous Connector")
    public long getPollIntervalInMilli() {
        return this.pollIntervalInMilli;
    }

    public void setPollIntervalInMilli(long intervel) {
        this.pollIntervalInMilli = intervel;
    }

    @Override
    public Execution createExecution(Command command, ExecutionContext executionContext, RuntimeMetadata metadata, Object connection)
            throws TranslatorException {
        return new LoopbackExecution(command, this, executionContext);
    }

    @Override
    public boolean isSourceRequired() {
        return false;
    }

    @Override
    public boolean isSourceRequiredForMetadata() {
        return false;
    }

    @Override
    public void getMetadata(MetadataFactory metadataFactory, Object conn)
            throws TranslatorException {
    }

    //override to set as non required
    @Override
    @TranslatorProperty(display = "Delegate name", required = false)
    public String getDelegateName() {
        return super.getDelegateName();
    }

    @Override
    public boolean returnsSingleUpdateCount() {
        return true;
    }
}
