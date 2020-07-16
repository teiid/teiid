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

import org.teiid.file.VirtualFile;
import org.teiid.file.VirtualFileConnection;
import org.teiid.language.Delete;
import org.teiid.language.Insert;
import org.teiid.language.LanguageObject;
import org.teiid.language.Update;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.UpdateExecution;

public class ParquetUpdateExecution extends BaseParquetExecution implements UpdateExecution {

    private LanguageObject command;
    private int result;
    private VirtualFile writeTo;
    private boolean modified;

    public ParquetUpdateExecution(LanguageObject command, ExecutionContext executionContext,
                                  RuntimeMetadata metadata, VirtualFileConnection connection, boolean immutable) throws TranslatorException {
        super(executionContext, metadata, connection, immutable);
        visit(command);
        this.command = command;
    }

    @Override
    public int[] getUpdateCounts()
            throws DataNotAvailableException, TranslatorException {
        return new int[] {result};
    }

    @Override
    public void execute() throws TranslatorException {
        super.execute();
        if (command instanceof Update) {
            handleUpdate();
        } else if (command instanceof Delete) {
            handleDelete();
        } else if (command instanceof Insert) {
            handleInsert();
        }
    }

    private void handleInsert() throws TranslatorException {
        return;
    }

    private void handleUpdate() throws TranslatorException {
        return;
    }

    private void handleDelete() throws TranslatorException {
        return;
    }

}
