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



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;
import org.teiid.file.VirtualFile;
import org.teiid.file.VirtualFileConnection;
import org.teiid.language.LanguageObject;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.Execution;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TranslatorException;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicInteger;


public class BaseParquetExecution implements Execution {
    @SuppressWarnings("unused")
    protected ExecutionContext executionContext;
    @SuppressWarnings("unused")
    protected RuntimeMetadata metadata;
    protected VirtualFileConnection connection;
    protected boolean immutable;

    // Execution state
    protected ParquetQueryVisitor visitor = new ParquetQueryVisitor();
    private VirtualFile[] parquetFiles;
    private AtomicInteger fileCount = new AtomicInteger();
    private ParquetFileReader reader;
    private MessageType schema;
    private RecordReader<Group> rowIterator;
    private MessageColumnIO columnIO;

    public BaseParquetExecution(ExecutionContext executionContext,
                                RuntimeMetadata metadata, VirtualFileConnection connection, boolean immutable) {
        this.executionContext = executionContext;
        this.metadata = metadata;
        this.connection = connection;
        this.immutable = immutable;
    }

    public void visit(LanguageObject command) throws TranslatorException {
        this.visitor.visitNode(command);

        if (!visitor.exceptions.isEmpty()) {
            throw visitor.exceptions.get(0);
        }
    }

    @Override
    public void execute() throws TranslatorException {
        this.parquetFiles = VirtualFileConnection.Util.getFiles(this.visitor.getParquetPath(), this.connection, true);
        this.rowIterator = readParquetFile(parquetFiles[fileCount.getAndIncrement()]);
    }

    private RecordReader<Group> readParquetFile(VirtualFile parquetFile) throws TranslatorException {
        try (InputStream parquetFileStream = parquetFile.openInputStream(!immutable)) {
           return readParquetFile(parquetFile, parquetFileStream);
        } catch (IOException e) {
            throw new TranslatorException(e);
        }
    }

    private RecordReader<Group> readParquetFile(VirtualFile parquetFile, InputStream parquetFileStream) throws TranslatorException {
       try {
           File localFile = createTempFile(parquetFileStream);
           Path path = new Path(localFile.toURI());
           String extension = getExtension(localFile.getName());
           if (extension != "parquet") {
               throw new TranslatorException(ParquetPlugin.Event.TEIID23000, ParquetPlugin.Util.gs(ParquetPlugin.Event.TEIID23000));
           }
           Configuration config = new Configuration();
           reader = ParquetFileReader.open(HadoopInputFile.fromPath(path, config));
           schema = reader.getFooter().getFileMetaData().getSchema();
           columnIO = new ColumnIOFactory().getColumnIO(schema);
           PageReadStore pages = reader.readNextRowGroup();
           return columnIO.getRecordReader(pages, new GroupRecordConverter(schema));
       } catch (IOException e){
           throw new TranslatorException(e);
       }
    }



    private String getExtension(String name) {
        String extension = "";
        int i = name.lastIndexOf('.');
        if (i > 0) {
            extension = name.substring(i+1);
        }
        return extension;
    }

    private File createTempFile(InputStream parquetFileStream) throws IOException {
        File tempFile = File.createTempFile(getClass().getSimpleName(), ".tmp");
        tempFile.deleteOnExit();
        try (FileOutputStream out = new FileOutputStream(tempFile)) {
            IOUtils.copyBytes(parquetFileStream, out, 131072);
        }
        return tempFile;
    }

    public Group nextRow() throws TranslatorException, DataNotAvailableException {
        while(true){
            try {
                Group row = nextRowInternal();
                if (row == null) {
                    return null;
                }
                return row;
            } catch (IOException e) {
                throw new TranslatorException(e);
            }
        }
    }

    private Group nextRowInternal() throws IOException, TranslatorException {
        Group row = rowIterator.read();
        if(row != null) {
            return row;
        }
        try {
            PageReadStore nextRowGroup = this.reader.readNextRowGroup();
            if(nextRowGroup != null){
                PageReadStore pages = reader.readNextRowGroup();
                this.rowIterator = columnIO.getRecordReader(pages, new GroupRecordConverter(schema));
                row = this.rowIterator.read();
                return row;
            }
            while(true) {
                this.rowIterator = null;
                VirtualFile nextParquetFile = getNextParquetFile();
                if (nextParquetFile == null) {
                    break;
                }
                this.rowIterator = readParquetFile(nextParquetFile);
                row = this.rowIterator.read();
                if(row != null){
                    break;
                }
            }
            return row;
        } catch (IOException e)
        {
            throw new TranslatorException(e);
        }
    }

    protected VirtualFile getNextParquetFile() throws TranslatorException {
        if (this.parquetFiles.length > this.fileCount.get()) {
            return this.parquetFiles[this.fileCount.getAndIncrement()];
        }
        return null;
    }

    @Override
    public void close() {

    }

    @Override
    public void cancel() throws TranslatorException {

    }
}
