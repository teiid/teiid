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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.Operators.Column;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.teiid.core.types.BinaryType;
import org.teiid.file.VirtualFile;
import org.teiid.file.VirtualFileConnection;
import org.teiid.language.Comparison;
import org.teiid.language.LanguageObject;
import org.teiid.language.Literal;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.Execution;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TranslatorException;

import com.google.common.collect.Multimap;


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
    private MessageType filteredSchema;
    private RecordReader<Group> rowIterator;
    private MessageColumnIO columnIO;
    private long pageRowCount;
    protected HashMap<String, String> partitionedColumnsValue;
    protected HashSet<String> partitionedColumnsHm = new HashSet<>();

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
        String path = this.visitor.getParquetPath();
        if(this.visitor.getTable().getProperty(ParquetMetadataProcessor.PARTITIONED_COLUMNS) != null) {
            path += getDirectoryPath(this.visitor.getColumnPredicates(), this.visitor.getTable().getProperty(ParquetMetadataProcessor.PARTITIONED_COLUMNS));
        }
        this.parquetFiles = VirtualFileConnection.Util.getFiles(path, this.connection, true, false);
        VirtualFile nextParquetFile = getNextParquetFile();
        if (nextParquetFile != null) {
            this.rowIterator = readParquetFile(nextParquetFile);
        }
    }

    private String getDirectoryPath(Multimap<String, Comparison> predicates, String partitionedColumns) throws TranslatorException {
        StringBuilder path = new StringBuilder(); // because we are only supporting the equality comparison as of now, otherwise we would return a list of paths to iterate over
        String[] partitionedColumnsArray = getPartitionedColumns(partitionedColumns);
        for (String s : partitionedColumnsArray) {
            partitionedColumnsHm.add(s);
            path.append("/").append(s.replaceAll("[*]", "[*][*]")).append("=");
            String value = "*";
            Collection<Comparison> columnPredicates = predicates.get(s);
            for(Comparison predicate: columnPredicates) {
                Literal l = (Literal) predicate.getRightExpression();
                if (predicate.getOperator() == Comparison.Operator.EQ) {
                    value = l.getValue().toString().replaceAll("[*]", "[*][*]");
                }
            }
            path.append(value);
            predicates.removeAll(s);
        }
        path.append("/*");
        return path.toString();
    }

    private FilterCompat.Filter getRowGroupFilter(Multimap<String, Comparison> columnPredicates) throws TranslatorException {
        if(columnPredicates.size() == 0){
            return FilterCompat.NOOP;
        }
        FilterPredicate combinedFilterPredicate = null, filterPredicate;
        for (Map.Entry<String, Collection<Comparison>> entry : columnPredicates.asMap().entrySet()) {
            String columnName = entry.getKey();
            Collection<Comparison> collection = entry.getValue();
            for (Comparison comparison :
                    collection) {
                Literal l = (Literal) comparison.getRightExpression();
                Column<?> column;
                Comparable<?> value;
                switch (comparison.getRightExpression().getType().getName()) {
                    case "java.lang.String":
                        column = FilterApi.binaryColumn(columnName);
                        value = Binary.fromString((String) l.getValue());
                        break;
                    case "org.teiid.core.types.BinaryType":
                        column = FilterApi.binaryColumn(columnName);
                        BinaryType binaryType = (BinaryType) l.getValue();
                        value = Binary.fromConstantByteArray(binaryType.getBytes());
                        break;
                    case "java.lang.Long":
                        column = FilterApi.longColumn(columnName);
                        value = (Long) l.getValue();
                        break;
                    case "java.lang.Integer":
                        column = FilterApi.intColumn(columnName);
                        value = (Integer) l.getValue();
                        break;
                    case "java.lang.Boolean":
                        column = FilterApi.booleanColumn(columnName);
                        value = (Boolean) l.getValue();
                        break;
                    case "java.lang.Double":
                        column = FilterApi.doubleColumn(columnName);
                        value = (Double) l.getValue();
                        break;
                    case "java.lang.Float":
                        column = FilterApi.floatColumn(columnName);
                        value = (Float) l.getValue();
                        break;
                    default:
                        throw new TranslatorException("The type " + comparison.getRightExpression().getType().getName() + " is not supported for comparison.");
                }
                try {
                    Method m = FilterApi.class.getMethod(getMethodName(comparison.getOperator()), Column.class, Comparable.class);
                    filterPredicate = (FilterPredicate) m.invoke(null, column, value);
                } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
                    throw new TranslatorException(e);
                }
                if (combinedFilterPredicate == null) {
                    combinedFilterPredicate = filterPredicate;
                } else {
                    combinedFilterPredicate = FilterApi.and(combinedFilterPredicate, filterPredicate);
                }
            }

        }
        assert combinedFilterPredicate != null;
        return FilterCompat.get(combinedFilterPredicate);
    }

    private String getMethodName(Comparison.Operator operator) throws TranslatorException {
        switch (operator){
            case EQ: return "eq";
            case GT: return "gt";
            case LT: return "lt";
            case GE: return "gtEq";
            case LE: return "ltEq";
            case NE: return "notEq";
            default: throw new TranslatorException("Unexpected value: " + operator);
        }
    }

    private RecordReader<Group> readParquetFile(VirtualFile parquetFile) throws TranslatorException {
        try (InputStream parquetFileStream = parquetFile.openInputStream(!immutable)) {
            if(this.visitor.getTable().getProperty(ParquetMetadataProcessor.PARTITIONED_COLUMNS) != null) {
                partitionedColumnsValue = getPartitionedColumnsValues(parquetFile);
            }
            File localFile = createTempFile(parquetFileStream);
            Path path = new Path(localFile.toURI());
            Configuration config = new Configuration();
            FilterCompat.Filter rowGroupFilter = getRowGroupFilter(this.visitor.getColumnPredicates());
            reader = ParquetFileReader.open(HadoopInputFile.fromPath(path, config), ParquetReadOptions.builder().withRecordFilter(rowGroupFilter).build());
            MessageType schema = reader.getFooter().getFileMetaData().getSchema();
            filteredSchema = getFilteredSchema(schema, this.visitor.getProjectedColumnNames());
            columnIO = new ColumnIOFactory().getColumnIO(filteredSchema);
            PageReadStore pages = reader.readNextRowGroup();
            pageRowCount = pages.getRowCount();
            return columnIO.getRecordReader(pages, new GroupRecordConverter(filteredSchema), rowGroupFilter);
        } catch (IOException e) {
            throw new TranslatorException(e);
        }
    }

    private HashMap<String, String> getPartitionedColumnsValues(VirtualFile parquetFile) {
        HashMap<String, String> hm = new HashMap<>();
        String path = parquetFile.getPath().substring(this.visitor.getParquetPath().length());
        String[] columns = path.split("/");
        for(int i = 0; i < columns.length; i++){
            if(columns[i].contains("=")){
                int indexOfEquals = columns[i].indexOf("=");
                hm.put(columns[i].substring(0, indexOfEquals), columns[i].substring(indexOfEquals+1));
            }
        }
        return hm;
    }

    private MessageType getFilteredSchema(MessageType schema, List<String> expectedColumnNames) {
        List<Type> allColumns = schema.getFields();
        List<Type> selectedColumns = new ArrayList<>();
        for(String columnName: expectedColumnNames) {
            if(!partitionedColumnsHm.contains(columnName)) {
                selectedColumns.add(allColumns.get(schema.getFieldIndex(columnName)));
            }
        }
        return new MessageType(schema.getName(), selectedColumns);
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
        try {
            while (rowIterator != null) {
                if (pageRowCount-- <= 0) {
                    this.rowIterator = null;
                    PageReadStore nextRowGroup = this.reader.readNextRowGroup();
                    if(nextRowGroup == null){
                        VirtualFile nextParquetFile = getNextParquetFile();
                        if (nextParquetFile == null) {
                            return null;
                        }
                        this.rowIterator = readParquetFile(nextParquetFile);
                        continue;
                    }
                    this.pageRowCount = nextRowGroup.getRowCount();
                    this.rowIterator = columnIO.getRecordReader(nextRowGroup, new GroupRecordConverter(filteredSchema));
                }
                Group presentRow = rowIterator.read();
                if(presentRow == null){
                    continue;
                }
                return presentRow;
            }

            return null;
        } catch (IOException e) {
            throw new TranslatorException(e);
        }
    }

    protected VirtualFile getNextParquetFile() {
        while (this.parquetFiles.length > this.fileCount.get()) {
            VirtualFile f = this.parquetFiles[this.fileCount.getAndIncrement()];
            if (f.getName().endsWith(".parquet")) {
                return f;
            }
        }
        return null;
    }

    private String[] getPartitionedColumns(String partitionedColumns) {
        String[] partitionedColumnList;
        partitionedColumnList = partitionedColumns.split(",");
        return partitionedColumnList;
    }

    @Override
    public void close() {

    }

    @Override
    public void cancel() throws TranslatorException {

    }
}
