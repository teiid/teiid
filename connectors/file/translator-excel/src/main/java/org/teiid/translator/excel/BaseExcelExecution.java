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


import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.SQLXML;
import java.sql.Timestamp;
import java.util.Date;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.FormulaEvaluator;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.teiid.core.types.BlobImpl;
import org.teiid.core.types.BlobType;
import org.teiid.core.types.ClobImpl;
import org.teiid.core.types.ClobType;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.InputStreamFactory;
import org.teiid.core.types.SQLXMLImpl;
import org.teiid.core.types.TransformationException;
import org.teiid.core.types.XMLType;
import org.teiid.core.util.TimestampWithTimezone;
import org.teiid.file.VirtualFile;
import org.teiid.file.VirtualFileConnection;
import org.teiid.language.LanguageObject;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.Execution;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TranslatorException;


public class BaseExcelExecution implements Execution {
    @SuppressWarnings("unused")
    protected ExecutionContext executionContext;
    @SuppressWarnings("unused")
    protected RuntimeMetadata metadata;
    protected VirtualFileConnection connection;
    protected boolean immutable;

    // Execution state
    protected Iterator<Row> rowIterator;
    private Row currentRow;
    private VirtualFile[] xlsFiles;
    private AtomicInteger fileCount = new AtomicInteger();
    protected ExcelQueryVisitor visitor = new ExcelQueryVisitor();
    protected FormulaEvaluator evaluator;
    private DataFormatter dataFormatter;
    protected Workbook workbook;

    public BaseExcelExecution(ExecutionContext executionContext,
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
        this.xlsFiles = VirtualFileConnection.Util.getFiles(this.visitor.getXlsPath(), this.connection, true, false);
        this.rowIterator = readXLSFile(xlsFiles[fileCount.getAndIncrement()]);
    }

    private Iterator<Row> readXLSFile(VirtualFile xlsFile) throws TranslatorException {
        try (InputStream xlsFileStream = xlsFile.openInputStream(!immutable)) {
            return readXLSFile(xlsFile, xlsFileStream);
        } catch (IOException e) {
            throw new TranslatorException(e);
        }
    }

    private Iterator<Row> readXLSFile(VirtualFile xlsFile,
            InputStream xlsFileStream) throws IOException, TranslatorException {
        String extension = ExcelMetadataProcessor.getFileExtension(xlsFile);
        if (extension.equalsIgnoreCase("xls")) { //$NON-NLS-1$
            workbook = new HSSFWorkbook(xlsFileStream);
        }
        else if (extension.equalsIgnoreCase("xlsx")) { //$NON-NLS-1$
            workbook = new XSSFWorkbook(xlsFileStream);
        }
        else {
            throw new TranslatorException(ExcelPlugin.Event.TEIID23000, ExcelPlugin.Util.gs(ExcelPlugin.Event.TEIID23000));
        }
        Sheet sheet = workbook.getSheet(this.visitor.getSheetName());
        this.evaluator = workbook.getCreationHelper().createFormulaEvaluator();
        Iterator<Row> rowIter = sheet.iterator();

        // skip up to the first data row
        if (this.visitor.getFirstDataRowNumber() > 0 && rowIter != null) {
            while(rowIter.hasNext()) {
                Row row = rowIter.next();
                if (row.getRowNum() >= this.visitor.getFirstDataRowNumber()) {
                    this.currentRow = row;
                    break;
                }
            }
        }
        return rowIter;
    }

    public Row nextRow() throws TranslatorException, DataNotAvailableException {
        while (true) {
            Row row = nextRowInternal();
            if (row == null) {
                return null;
            }

            // when the first cell number is -1, then it is empty row, skip it
            if (row.getFirstCellNum() == -1) {
                continue;
            }

            if (!this.visitor.allows(row.getRowNum())) {
                continue;
            }
            return row;
        }
    }

    private Row nextRowInternal() throws TranslatorException {
        Row row = null;
        if (this.currentRow != null) {
            row = this.currentRow;
            this.currentRow = null;
            return row;
        }

        boolean hasNext = false;
        if (this.rowIterator != null) {
            hasNext = this.rowIterator.hasNext();
        }

        while (!hasNext) {
            this.rowIterator = null;
            VirtualFile nextXlsFile = getNextXLSFile();
            if (nextXlsFile == null) {
                break;
            }
            this.rowIterator = readXLSFile(nextXlsFile);
            hasNext = this.rowIterator.hasNext();
        }
        if (hasNext) {
            row = this.rowIterator.next();
        }
        return row;
    }

    /**
     * @throws TranslatorException
     */
    protected VirtualFile getNextXLSFile() throws TranslatorException {
        if (this.xlsFiles.length > this.fileCount.get()) {
            return this.xlsFiles[this.fileCount.getAndIncrement()];
        }
        return null;
    }

    protected VirtualFile getCurrentXLSFile() {
        if (this.xlsFiles.length >= this.fileCount.get()) {
            return this.xlsFiles[this.fileCount.get()-1];
        }
        return null;
    }

    Object convertFromExcelType(final boolean value, final Class<?> expectedType) throws TranslatorException {
        if (expectedType.isAssignableFrom(Boolean.class)) {
            return value;
        }

        try {
            return DataTypeManager.transformValue(value, expectedType);
        } catch (TransformationException e) {
            throw new TranslatorException(e);
        }
    }

    Object convertFromExcelType(final Double value, Cell cell, final Class<?> expectedType) throws TranslatorException {
        if (value == null) {
            return null;
        }

        if (expectedType.isAssignableFrom(Double.class)) {
            return value;
        }
        else if (expectedType.isAssignableFrom(Timestamp.class)) {
            Date date = cell.getDateCellValue();
            return new Timestamp(date.getTime());
        }
        else if (expectedType.isAssignableFrom(java.sql.Date.class)) {
            Date date = cell.getDateCellValue();
            return TimestampWithTimezone.createDate(date);
        }
        else if (expectedType.isAssignableFrom(java.sql.Time.class)) {
            Date date = cell.getDateCellValue();
            return TimestampWithTimezone.createTime(date);
        }

        if (expectedType == String.class && dataFormatter != null) {
            return dataFormatter.formatCellValue(cell);
        }

        Object val = value;

        if (DateUtil.isCellDateFormatted(cell)) {
            Date date = cell.getDateCellValue();
            val = new java.sql.Timestamp(date.getTime());
        }

        try {
            return DataTypeManager.transformValue(val, expectedType);
        } catch (TransformationException e) {
            throw new TranslatorException(e);
        }
    }

    static Object convertFromExcelType(final String value, final Class<?> expectedType) throws TranslatorException {
        if (value == null) {
            return null;
        }

        if (expectedType.isAssignableFrom(String.class)) {
            return value;
        }

        if (expectedType.isAssignableFrom(Blob.class)) {
            return new BlobType(new BlobImpl(new InputStreamFactory() {
                @Override
                public InputStream getInputStream() throws IOException {
                    return new ByteArrayInputStream(value.getBytes());
                }

            }));
        } else if (expectedType.isAssignableFrom(Clob.class)) {
            return new ClobType(new ClobImpl(value));
        } else if (expectedType.isAssignableFrom(SQLXML.class)) {
            return new XMLType(new SQLXMLImpl(value.getBytes()));
        } else if (DataTypeManager.isTransformable(String.class, expectedType)) {
            try {
                return DataTypeManager.transformValue(value, expectedType);
            } catch (TransformationException e) {
                throw new TranslatorException(e);
            }
        } else {
            throw new TranslatorException(ExcelPlugin.Event.TEIID23003, ExcelPlugin.Util.gs(ExcelPlugin.Event.TEIID23003, expectedType.getName()));
        }
    }

    @Override
    public void close() {
    }

    @Override
    public void cancel() throws TranslatorException {

    }

    public void setDataFormatter(DataFormatter dataFormatter) {
        this.dataFormatter = dataFormatter;
    }
}
