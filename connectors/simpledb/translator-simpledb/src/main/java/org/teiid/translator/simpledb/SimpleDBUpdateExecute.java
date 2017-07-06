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

package org.teiid.translator.simpledb;

import org.teiid.language.Command;
import org.teiid.language.Update;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.UpdateExecution;
import org.teiid.translator.simpledb.api.SimpleDBConnection;

public class SimpleDBUpdateExecute implements UpdateExecution {
    private SimpleDBUpdateVisitor visitor;
    private SimpleDBConnection connection;
    private int updatedCount=0;

    public SimpleDBUpdateExecute(Command command, SimpleDBConnection connection) throws TranslatorException {
        this.connection = connection;
        this.visitor = new SimpleDBUpdateVisitor((Update)command);
        this.visitor.checkExceptions();        
    }

    @Override
    public void execute() throws TranslatorException {
        String domainName = SimpleDBMetadataProcessor.getName(this.visitor.getTable());
        this.updatedCount = this.connection.performUpdate(domainName, this.visitor.getAttributes(), buildSelect());
    }

    private String buildSelect() {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT ").append(SimpleDBMetadataProcessor.ITEM_NAME); //$NON-NLS-1$
        sb.append(" FROM ").append(SimpleDBMetadataProcessor.getName(this.visitor.getTable())); //$NON-NLS-1$
        if (this.visitor.getCriteria() != null) {
            sb.append(" WHERE ").append(this.visitor.getCriteria()); //$NON-NLS-1$
        }
        return sb.toString();
    }    
    
    @Override
    public int[] getUpdateCounts() throws DataNotAvailableException, TranslatorException {
        return new int[] { updatedCount };
    }
    
    @Override
    public void close() {
    }

    @Override
    public void cancel() throws TranslatorException {
    }
}
