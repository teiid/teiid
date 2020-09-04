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
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
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
import org.apache.parquet.filter2.predicate.FilterPredicate.Visitor;
import org.apache.parquet.filter2.predicate.Operators.And;
import org.apache.parquet.filter2.predicate.Operators.Column;
import org.apache.parquet.filter2.predicate.Operators.Eq;
import org.apache.parquet.filter2.predicate.Operators.Gt;
import org.apache.parquet.filter2.predicate.Operators.GtEq;
import org.apache.parquet.filter2.predicate.Operators.LogicalNotUserDefined;
import org.apache.parquet.filter2.predicate.Operators.Lt;
import org.apache.parquet.filter2.predicate.Operators.LtEq;
import org.apache.parquet.filter2.predicate.Operators.Not;
import org.apache.parquet.filter2.predicate.Operators.NotEq;
import org.apache.parquet.filter2.predicate.Operators.Or;
import org.apache.parquet.filter2.predicate.Operators.UserDefined;
import org.apache.parquet.filter2.predicate.Statistics;
import org.apache.parquet.filter2.predicate.UserDefinedPredicate;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.teiid.core.types.BinaryType;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.TransformationException;
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

    /**
     * Provides a simple visitation to evaluate the path filter
     */
    static Visitor<Boolean> FILE_PATH_VISITOR = new Visitor<Boolean>() {
        @Override
        public <T extends Comparable<T>> Boolean visit(
                Eq<T> eq) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T extends Comparable<T>> Boolean visit(
                NotEq<T> notEq) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T extends Comparable<T>> Boolean visit(
                Lt<T> lt) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T extends Comparable<T>> Boolean visit(
                LtEq<T> ltEq) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T extends Comparable<T>> Boolean visit(
                Gt<T> gt) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T extends Comparable<T>> Boolean visit(
                GtEq<T> gtEq) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Boolean visit(Not not) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T extends Comparable<T>, U extends UserDefinedPredicate<T>> Boolean visit(
                LogicalNotUserDefined<T, U> udp) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Boolean visit(And and) {
            return and.getLeft().accept(this) && and.getRight().accept(this);
        }

        @Override
        public Boolean visit(Or or) {
            return or.getLeft().accept(this) || or.getRight().accept(this);
        }

        @Override
        public <T extends Comparable<T>, U extends UserDefinedPredicate<T>> Boolean visit(
                UserDefined<T, U> udp) {
            return udp.getUserDefinedPredicate().keep(null);
        }
    };

    /**
     * Implements predicates against the file path
     */
    private class FilePathPredicate extends UserDefinedPredicate implements Serializable {

        private String columnName;
        private Comparison.Operator operator;
        private Comparable referenceValue;

        public FilePathPredicate(String columnName, Operator operator, Comparable referenceValue) {
            this.columnName = columnName;
            this.operator = operator;
            this.referenceValue = referenceValue;
        }

        @Override
        public boolean acceptsNullValue() {
            return true;
        }

        @Override
        public boolean keep(Comparable value) {
            Comparable currentValue = partitionedColumnsValue.get(columnName);
            if (currentValue == null) {
                throw new AssertionError("null partition value not expected"); //$NON-NLS-1$
            }
            if (referenceValue == null) {
                //effectively the is not null check
                return operator == Operator.NE;
            }
            int result = currentValue.compareTo(referenceValue);
            switch (operator) {
            case EQ:
                return result == 0;
            case GE:
                return result >= 0;
            case GT:
                return result > 0;
            case LE:
                return result <= 0;
            case LT:
                return result < 0;
            case NE:
                return result != 0;
            default: throw new AssertionError();
            }
        }

        @Override
        public boolean canDrop(Statistics statistics) {
            return !keep(null);
        }

        @Override
        public boolean inverseCanDrop(Statistics statistics) {
            return keep(null);
        }
    }

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
    protected HashMap<String, Comparable<?>> partitionedColumnsValue = new HashMap<>();
    private FilterCompat.Filter rowGroupFilter;
    private FilterPredicate filePathFilter;

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
            path = getDirectoryPath(path, this.visitor.getPartitionedComparisons());
        }
        FilterPredicate predicate = getRowGroupFilter(this.visitor.getNonPartionedConditions());
        if (predicate == null) {
            this.rowGroupFilter = FilterCompat.NOOP;
        } else {
            this.rowGroupFilter = FilterCompat.get(predicate);
        }
        filePathFilter = getRowGroupFilter(this.visitor.getPartitionedConditions());
        this.parquetFiles = VirtualFileConnection.Util.getFiles(path, this.connection, true, false);
        VirtualFile nextParquetFile = getNextParquetFile();
        if (nextParquetFile != null) {
            readParquetFile(nextParquetFile);
        }
    }

    private String getDirectoryPath(String root, Map<String, Comparison> predicates) {
        StringBuilder path = new StringBuilder(root); // because we are only supporting the equality comparison as of now, otherwise we would return a list of paths to iterate over
        if (!root.endsWith("/")) {
            path.append("/");
        }
        for (String s : this.visitor.getPartitionedColumns().keySet()) {
            path.append(s.replaceAll("[*]", "[*][*]")).append("=");
            String value = "*";
            Comparison predicate = predicates.get(s);
            if (predicate != null) {
                Literal l = (Literal) predicate.getRightExpression();
                assert predicate.getOperator() == Comparison.Operator.EQ;
                value = l.getValue().toString().replaceAll("[*]", "[*][*]");
            }
            path.append(value).append("/");
        }
        path.append("*");
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
            org.teiid.metadata.Column teiidCol = ((ColumnReference)comparison.getLeftExpression()).getMetadataObject();
            String columnName = teiidCol.getSourceName();
            if (!(comparison.getRightExpression() instanceof Literal)) {
                throw new TranslatorException("The value " + comparison.getRightExpression() + " is not supported for comparison.");
            }
            Literal l = (Literal) comparison.getRightExpression();
            Comparable<?> value = (Comparable<?>) l.getValue();

            if (this.visitor.getPartitionedColumns().containsKey(columnName)) {
                Column<?> referenceCol = this.visitor.getReferenceColumn();
                try {
                    return toUserDefinedPredicate(referenceCol, new FilePathPredicate(columnName, comparison.getOperator(), value));
                } catch (NoSuchMethodException | IllegalAccessException
                        | InvocationTargetException e) {
                    throw new TranslatorException(e);
                }
            }

            String typeName = comparison.getRightExpression().getType().getName();
            Column<?> column = createFilterColumn(columnName, typeName);

            try {
                if (value instanceof String) {
                    String stringValue = (String)value;
                    value = Binary.fromString(stringValue);
                } else if (value instanceof BinaryType) {
                    BinaryType binaryType = (BinaryType) value;
                    value = Binary.fromConstantByteArray(binaryType.getBytes());
                }
                filterPredicate = toFilterPredicate(comparison, column, value);

                //the api implements java like null comparisons, so we need an additional null filter
                if (comparison.getOperator() == Operator.NE && value != null) {
                    filterPredicate = FilterApi.and(filterPredicate, toFilterPredicate(comparison, column, null));
                }
            } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
                throw new TranslatorException(e);
            }
        } else {
            throw new AssertionError("unexpected predicate");
        }
        return filterPredicate;
    }

    /**
     * Create a column for the {@link FilterApi}
     * @param typeName the Java (not Teiid) type name
     */
    static Column<?> createFilterColumn(String columnName, String typeName)
            throws TranslatorException {
        Column<?> column;
        switch (typeName) {
            case "java.lang.String":
                column = FilterApi.binaryColumn(columnName);
                break;
            case "org.teiid.core.types.BinaryType":
                column = FilterApi.binaryColumn(columnName);
                break;
            case "java.lang.Long":
                column = FilterApi.longColumn(columnName);
                break;
            case "java.lang.Integer":
                column = FilterApi.intColumn(columnName);
                break;
            case "java.lang.Boolean":
                column = FilterApi.booleanColumn(columnName);
                //the filterapi does not support the concept of a comparable boolean
                //the engine should handle the appropriate conversion prior to this
                break;
            case "java.lang.Double":
                column = FilterApi.doubleColumn(columnName);
                break;
            case "java.lang.Float":
                column = FilterApi.floatColumn(columnName);
                break;
            default:
                throw new TranslatorException("The type " + typeName + " is not supported for comparison.");
        }
        return column;
    }

    private FilterPredicate getRowGroupFilter(List<Condition> columnPredicates) throws TranslatorException {
        if(columnPredicates.size() == 0){
            return null;
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
        return combinedFilterPredicate;
    }

    private FilterPredicate toFilterPredicate(Comparison comparison,
            Column<?> column, Comparable<?> value)
            throws NoSuchMethodException, TranslatorException,
            IllegalAccessException, InvocationTargetException {
        Method m = FilterApi.class.getMethod(getMethodName(comparison.getOperator()), Column.class, Comparable.class);
        return (FilterPredicate) m.invoke(null, column, value);
    }

    private FilterPredicate toUserDefinedPredicate(Column<?> column, UserDefinedPredicate<?> value)
            throws NoSuchMethodException,
            IllegalAccessException, InvocationTargetException {
        Method m = FilterApi.class.getMethod("userDefined", Column.class, UserDefinedPredicate.class);
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
            File localFile = createTempFile(parquetFileStream);
            Path path = new Path(localFile.toURI());
            Configuration config = new Configuration();
            reader = ParquetFileReader.open(HadoopInputFile.fromPath(path, config), ParquetReadOptions.builder().withRecordFilter(rowGroupFilter).build());
            MessageType schema = reader.getFooter().getFileMetaData().getSchema();
            filteredSchema = getFilteredSchema(schema, this.visitor.getAllColumns());
            columnIO = new ColumnIOFactory().getColumnIO(filteredSchema);
        } catch (IOException e) {
            throw new TranslatorException(e);
        }
    }

    private void parsePartitionedColumnsValues(VirtualFile parquetFile) throws TranslatorException {
        String path = parquetFile.getPath().substring(this.visitor.getParquetPath().length());
        String[] columns = path.split("/");
        for(int i = 0; i < columns.length; i++){
            int indexOfEquals = columns[i].indexOf("=");
            if(indexOfEquals == -1){
                continue;
            }
            String name = columns[i].substring(0, indexOfEquals);
            String stringValue = columns[i].substring(indexOfEquals+1);
            Comparable value = stringValue;
            org.teiid.metadata.Column partColumn = this.visitor.getPartitionedColumns().get(name);
            if (partColumn != null) {
                try {
                    value = (Comparable)DataTypeManager.transformValue(stringValue, partColumn.getJavaType());
                } catch (TransformationException e) {
                    throw new TranslatorException(e);
                }
            }
            partitionedColumnsValue.put(name, value);
        }
    }

    private MessageType getFilteredSchema(MessageType schema, LinkedHashSet<String> expectedColumnNames) {
        List<Type> allColumns = schema.getFields();
        List<Type> selectedColumns = new ArrayList<>();
        for(String columnName: expectedColumnNames) {
            if(!this.visitor.getPartitionedColumns().containsKey(columnName)) {
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

    protected VirtualFile getNextParquetFile() throws TranslatorException {
        while (this.parquetFiles.length > this.fileCount.get()) {
            VirtualFile f = this.parquetFiles[this.fileCount.getAndIncrement()];
            if (f.getName().endsWith(".parquet")) {
                if(!this.visitor.getPartitionedColumns().isEmpty()) {
                    parsePartitionedColumnsValues(f);
                    if (filePathFilter != null && !filePathFilter.accept(FILE_PATH_VISITOR)) {
                        //use the next file as the path does not match
                        continue;
                    }
                }
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
