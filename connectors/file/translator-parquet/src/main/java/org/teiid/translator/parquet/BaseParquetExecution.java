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
import java.util.HashMap;
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
import org.teiid.language.AndOr;
import org.teiid.language.ColumnReference;
import org.teiid.language.Comparison;
import org.teiid.language.Comparison.Operator;
import org.teiid.language.Condition;
import org.teiid.language.IsNull;
import org.teiid.language.LanguageObject;
import org.teiid.language.Literal;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.Execution;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TranslatorException;


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
    private FilterCompat.Filter rowGroupFilter;

    public BaseParquetExecution(ExecutionContext executionContext,
                                RuntimeMetadata metadata, VirtualFileConnection connection, boolean immutable) {
        this.executionContext = executionContext;
        this.metadata = metadata;
        this.connection = connection;
        this.immutable = immutable;
    }

    public void visit(LanguageObject command) throws TranslatorException {
        this.visitor.visitNode(command);
    }

    @Override
    public void execute() throws TranslatorException {
        String path = this.visitor.getParquetPath();
        if(!this.visitor.getPartitionedColumns().isEmpty()) {
            path += getDirectoryPath(this.visitor.getPartitionedComparisons());
        }
        rowGroupFilter = getRowGroupFilter(this.visitor.getNonPartionedConditions());
        this.parquetFiles = VirtualFileConnection.Util.getFiles(path, this.connection, true, false);
        VirtualFile nextParquetFile = getNextParquetFile();
        if (nextParquetFile != null) {
            readParquetFile(nextParquetFile);
        }
    }

    private String getDirectoryPath(Map<String, Comparison> predicates) {
        StringBuilder path = new StringBuilder(); // because we are only supporting the equality comparison as of now, otherwise we would return a list of paths to iterate over
        for (String s : this.visitor.getPartitionedColumns()) {
            path.append("/").append(s.replaceAll("[*]", "[*][*]")).append("=");
            String value = "*";
            Comparison predicate = predicates.get(s);
            if (predicate != null) {
                Literal l = (Literal) predicate.getRightExpression();
                if (predicate.getOperator() == Comparison.Operator.EQ) {
                    value = l.getValue().toString().replaceAll("[*]", "[*][*]");
                }
            }
            //TODO: handle the other comparisons in the execution
            path.append(value);
        }
        path.append("/*");
        return path.toString();
    }

    private FilterPredicate getRowGroupFilter(Condition condition) throws TranslatorException {
        FilterPredicate filterPredicate = null;
        if (condition instanceof AndOr) {
            AndOr andOr = (AndOr)condition;
            filterPredicate = getRowGroupFilter(andOr.getLeftCondition());
            if (andOr.getOperator() == org.teiid.language.AndOr.Operator.AND) {
                filterPredicate = FilterApi.and(filterPredicate, getRowGroupFilter(andOr.getRightCondition()));
            } else {
                filterPredicate = FilterApi.or(filterPredicate, getRowGroupFilter(andOr.getRightCondition()));
            }
            return filterPredicate;
        }
        if (condition instanceof IsNull) {
            //rewrite isnull as a comparison
            IsNull isNull = (IsNull)condition;
            condition = new Comparison(isNull.getExpression(), new Literal(null, isNull.getExpression().getType()), isNull.isNegated()?Operator.NE:Operator.EQ);
        }
        if (condition instanceof Comparison) {
            Comparison comparison = (Comparison)condition;
            String columnName = ((ColumnReference)comparison.getLeftExpression()).getMetadataObject().getSourceName();
            Literal l = (Literal) comparison.getRightExpression();
            Comparable<?> value = (Comparable<?>) l.getValue();
            Column<?> column;
            switch (comparison.getRightExpression().getType().getName()) {
                case "java.lang.String":
                    column = FilterApi.binaryColumn(columnName);
                    String stringValue = (String)value;
                    if (stringValue != null) {
                        value = Binary.fromString(stringValue);
                    }
                    break;
                case "org.teiid.core.types.BinaryType":
                    column = FilterApi.binaryColumn(columnName);
                    BinaryType binaryType = (BinaryType) value;
                    if (binaryType != null) {
                        value = Binary.fromConstantByteArray(binaryType.getBytes());
                    }
                    break;
                case "java.lang.Long":
                    column = FilterApi.longColumn(columnName);
                    break;
                case "java.lang.Integer":
                    column = FilterApi.intColumn(columnName);
                    break;
                case "java.lang.Boolean":
                    column = FilterApi.booleanColumn(columnName);
                    break;
                case "java.lang.Double":
                    column = FilterApi.doubleColumn(columnName);
                    break;
                case "java.lang.Float":
                    column = FilterApi.floatColumn(columnName);
                    break;
                default:
                    throw new TranslatorException("The type " + comparison.getRightExpression().getType().getName() + " is not supported for comparison.");
            }
            try {
                filterPredicate = toFilterPredicate(comparison, column, value);

                //the api implements java like null comparisons, so we need an additional null filter
                if (comparison.getOperator() == Operator.NE && value != null) {
                    filterPredicate = FilterApi.and(filterPredicate, toFilterPredicate(comparison, column, null));
                }
            } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
                throw new TranslatorException(e);
            }
        }
        return filterPredicate;
    }

    private FilterCompat.Filter getRowGroupFilter(List<Condition> columnPredicates) throws TranslatorException {
        if(columnPredicates.size() == 0){
            return FilterCompat.NOOP;
        }
        FilterPredicate combinedFilterPredicate = null, filterPredicate;
        for (Condition cond : columnPredicates) {
            filterPredicate = getRowGroupFilter(cond);
            if (combinedFilterPredicate == null) {
                combinedFilterPredicate = filterPredicate;
            } else {
                combinedFilterPredicate = FilterApi.and(combinedFilterPredicate, filterPredicate);
            }
        }
        assert combinedFilterPredicate != null;
        return FilterCompat.get(combinedFilterPredicate);
    }

    private FilterPredicate toFilterPredicate(Comparison comparison,
            Column<?> column, Comparable<?> value)
            throws NoSuchMethodException, TranslatorException,
            IllegalAccessException, InvocationTargetException {
        Method m = FilterApi.class.getMethod(getMethodName(comparison.getOperator()), Column.class, Comparable.class);
        return (FilterPredicate) m.invoke(null, column, value);
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

    private void readParquetFile(VirtualFile parquetFile) throws TranslatorException {
        try (InputStream parquetFileStream = parquetFile.openInputStream(!immutable)) {
            if(!this.visitor.getPartitionedColumns().isEmpty()) {
                partitionedColumnsValue = getPartitionedColumnsValues(parquetFile);
            }
            File localFile = createTempFile(parquetFileStream);
            Path path = new Path(localFile.toURI());
            Configuration config = new Configuration();
            reader = ParquetFileReader.open(HadoopInputFile.fromPath(path, config), ParquetReadOptions.builder().withRecordFilter(rowGroupFilter).build());
            MessageType schema = reader.getFooter().getFileMetaData().getSchema();
            filteredSchema = getFilteredSchema(schema, this.visitor.getProjectedColumnNames());
            columnIO = new ColumnIOFactory().getColumnIO(filteredSchema);
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
            if(!this.visitor.getPartitionedColumns().contains(columnName)) {
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
            while (columnIO != null) {
                if (this.rowIterator == null || pageRowCount-- <= 0) {
                    this.rowIterator = null;
                    PageReadStore nextRowGroup = this.reader.readNextRowGroup();
                    if(nextRowGroup == null){
                        VirtualFile nextParquetFile = getNextParquetFile();
                        if (nextParquetFile == null) {
                            columnIO = null;
                            return null; //terminal condition
                        }
                        readParquetFile(nextParquetFile);
                        continue; //try again on the next file
                    }
                    this.pageRowCount = nextRowGroup.getRowCount();
                    this.rowIterator = columnIO.getRecordReader(nextRowGroup, new GroupRecordConverter(filteredSchema), rowGroupFilter);
                    continue;
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

    @Override
    public void close() {

    }

    @Override
    public void cancel() throws TranslatorException {

    }
}
