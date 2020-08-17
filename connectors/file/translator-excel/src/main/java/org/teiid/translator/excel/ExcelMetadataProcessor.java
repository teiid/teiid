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

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.teiid.file.VirtualFile;
import org.teiid.file.VirtualFileConnection;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.Column;
import org.teiid.metadata.Column.SearchType;
import org.teiid.metadata.ExtensionMetadataProperty;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.Table;
import org.teiid.translator.MetadataProcessor;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TranslatorProperty;
import org.teiid.translator.TranslatorProperty.PropertyType;
import org.teiid.translator.TypeFacility;

public class ExcelMetadataProcessor implements MetadataProcessor<VirtualFileConnection> {

    @ExtensionMetadataProperty(applicable=Table.class, datatype=String.class, display="Excel File Name", description="Excel File name, use file name pattern if one than one file in the parent directory", required=true)
    public static final String FILE = MetadataFactory.EXCEL_PREFIX+"FILE"; //$NON-NLS-1$

    @ExtensionMetadataProperty(applicable=Column.class, datatype=Integer.class, display="Cell Number", description="Cell number, where the column information is defined. If column name is ROW_ID, define it as -1", required=true)
    public static final String CELL_NUMBER = MetadataFactory.EXCEL_PREFIX+"CELL_NUMBER"; //$NON-NLS-1$

    @ExtensionMetadataProperty(applicable=Table.class, datatype=Integer.class, display="First Data Number", description="First Row Number, where data rows start")
    public static final String FIRST_DATA_ROW_NUMBER = MetadataFactory.EXCEL_PREFIX+"FIRST_DATA_ROW_NUMBER"; //$NON-NLS-1$

    public static final String ROW_ID = "ROW_ID"; //$NON-NLS-1$

    private String excelFileName;
    private boolean ignoreEmptyCells = false;
    private int headerRowNumber = 0;
    private boolean hasHeader = false;
    private int dataRowNumber = 0;
    private boolean hasDataRowNumber = false;

    public void process(MetadataFactory mf, VirtualFileConnection conn) throws TranslatorException {
        if (this.excelFileName == null) {
            throw new TranslatorException(ExcelPlugin.Event.TEIID23004, ExcelPlugin.Util.gs(ExcelPlugin.Event.TEIID23004, "importer.ExcelFileName")); //$NON-NLS-1$
        }
        try {
            VirtualFile[] xlsFiles = conn.getFiles(this.excelFileName);
            if (xlsFiles == null || xlsFiles.length == 0) {
                throw new TranslatorException(ExcelPlugin.Event.TEIID23005, ExcelPlugin.Util.gs(ExcelPlugin.Event.TEIID23005, excelFileName));
            }

            VirtualFile xlsFile = xlsFiles[0];

            String extension = getFileExtension(xlsFile);
            InputStream xlsFileStream = xlsFile.openInputStream(true);
            try {
                Workbook workbook = null;
                if (extension.equalsIgnoreCase("xls")) { //$NON-NLS-1$
                    workbook = new HSSFWorkbook(xlsFileStream);
                }
                else if (extension.equalsIgnoreCase("xlsx")) { //$NON-NLS-1$
                    workbook = new XSSFWorkbook(xlsFileStream);
                } else {
                    throw new TranslatorException("unknown file extension"); //$NON-NLS-1$
                }
                int sheetCount = workbook.getNumberOfSheets();
                for (int i = 0; i < sheetCount; i++) {
                    Sheet sheet = workbook.getSheetAt(i);
                    addTable(mf, sheet, xlsFile.getName(), this.excelFileName);
                }
            } finally {
                xlsFileStream.close();
            }
        } catch (IOException e) {
            throw new TranslatorException(e);
        }
    }

    private void addTable(MetadataFactory mf, Sheet sheet, String xlsName, String originalName) {
        int firstRowNumber = sheet.getFirstRowNum();
        Row headerRow = null;
        int firstCellNumber = -1;
        if (this.hasHeader) {
            headerRow = sheet.getRow(this.headerRowNumber);
            if (headerRow != null) {
                firstRowNumber = this.headerRowNumber;
                firstCellNumber = headerRow.getFirstCellNum();
                if (firstCellNumber == -1) {
                    LogManager.logInfo(LogConstants.CTX_CONNECTOR, ExcelPlugin.Util.gs(ExcelPlugin.Event.TEIID23006, xlsName));
                    return;
                }
            }
        }

        if (headerRow == null) {
            while (firstCellNumber == -1) {
                headerRow = sheet.getRow(firstRowNumber++);
                // check if this is a empty sheet; the data must be present in first 10000 rows
                if (headerRow == null && firstRowNumber > 10000) {
                    return;
                }
                if (headerRow == null) {
                    continue;
                }
                firstCellNumber = headerRow.getFirstCellNum();
            }
        }

        // create a table for each sheet
        AtomicInteger columnCount = new AtomicInteger();
        Table table = mf.addTable(sheet.getSheetName());
        table.setNameInSource(sheet.getSheetName());
        table.setProperty(ExcelMetadataProcessor.FILE, originalName);

        // add implicit row_id column based on row number from excel sheet
        Column column = mf.addColumn(ROW_ID, TypeFacility.RUNTIME_NAMES.INTEGER, table);
        column.setSearchType(SearchType.All_Except_Like);
        column.setProperty(CELL_NUMBER, ROW_ID);
        mf.addPrimaryKey("PK0", Arrays.asList(ROW_ID), table); //$NON-NLS-1$
        column.setUpdatable(false);

        Row dataRow = null;
        int lastCellNumber = headerRow.getLastCellNum();

        // if getIgnoreEmptyHeaderCells() is false and we have a header row
        // then only count cells that have a non-empty value.
        if (this.hasHeader && !getIgnoreEmptyHeaderCells()) {
            int cellCounter = 0;
            for (int i = firstCellNumber; i < lastCellNumber; i++) {
                Cell headerCell = headerRow.getCell(i);
                if (isCellEmpty(headerCell)) {
                    // found a cell with no column name that will be the last cell.
                    break;
                }
                cellCounter++;
            }

            lastCellNumber = cellCounter + firstCellNumber;
        }

        if (this.hasDataRowNumber) {
            // adjust for zero index
            table.setProperty(ExcelMetadataProcessor.FIRST_DATA_ROW_NUMBER, String.valueOf(this.dataRowNumber+1));
            dataRow = sheet.getRow(this.dataRowNumber);
        }
        else if (this.hasHeader) {
            // +1 zero based, +1 to skip header
            table.setProperty(ExcelMetadataProcessor.FIRST_DATA_ROW_NUMBER, String.valueOf(firstRowNumber+2));
            dataRow = sheet.getRow(firstRowNumber+1);
        }
        else {
            //+1 already occurred because of the increment above
            table.setProperty(ExcelMetadataProcessor.FIRST_DATA_ROW_NUMBER, String.valueOf(firstRowNumber));
            dataRow = sheet.getRow(firstRowNumber);
        }

        if (firstCellNumber != -1) {
            for (int j = firstCellNumber; j < lastCellNumber; j++) {
                Cell headerCell = headerRow.getCell(j);

                // if the config is set to ignore empty header cells then validate the header
                // cell has a value, if not move on to the next column in the sheet.
                if (this.hasHeader && getIgnoreEmptyHeaderCells() && isCellEmpty(headerCell)) {
                    continue;
                }

                Cell dataCell = dataRow.getCell(j);
                // if the cell value is null; then advance the data row cursor to to find it
                if (dataCell == null) {
                    for (int rowNo = firstRowNumber+1; rowNo < firstRowNumber+10000; rowNo++) {
                        Row row = sheet.getRow(rowNo);
                        dataCell = row.getCell(j);
                        if (dataCell != null) {
                            break;
                        }
                    }
                }
                column = mf.addColumn(cellName(headerCell, columnCount), cellType(headerCell, dataCell), table);
                column.setSearchType(SearchType.Unsearchable);
                column.setProperty(ExcelMetadataProcessor.CELL_NUMBER, String.valueOf(j+1));
            }
        }
    }

    private boolean isCellEmpty(Cell headerCell) {
        if (headerCell == null)
            return true;

        String name = headerCell.getStringCellValue();
        return (name == null || name.isEmpty());
    }

    private String cellType(Cell headerCell, Cell dataCell) {
        if (this.hasHeader) {
            return getCellType(dataCell);
        }
        return getCellType(headerCell);
    }

    private String cellName(Cell headerCell, AtomicInteger count) {
        if (this.hasHeader) {
            return headerCell.getStringCellValue();
        }
        return "column"+count.incrementAndGet(); //$NON-NLS-1$
    }

    public void setExcelFileName(String fileName) {
        this.excelFileName = fileName;
    }

    static String getFileExtension(VirtualFile xlsFile) {
        int idx = xlsFile.getName().lastIndexOf('.');
        String extension = "xls"; //$NON-NLS-1$
        if (idx > 0) {
            extension = xlsFile.getName().substring(idx+1);
        }
        return extension;
    }

    private String getCellType(Cell cell) {
        if (cell == null) {
            return TypeFacility.RUNTIME_NAMES.STRING;
        }
        switch (cell.getCellType()) {
        case Cell.CELL_TYPE_STRING:
            return TypeFacility.RUNTIME_NAMES.STRING;
        case Cell.CELL_TYPE_BOOLEAN:
            return TypeFacility.RUNTIME_NAMES.BOOLEAN;
        default:
            if (DateUtil.isCellDateFormatted(cell)) {
                return TypeFacility.RUNTIME_NAMES.TIMESTAMP;
            }
            return TypeFacility.RUNTIME_NAMES.DOUBLE;
        }
    }

    @TranslatorProperty(display="Header Row Number", category=PropertyType.IMPORT,
            description="Row number that contains the header information")
    public int getHeaderRowNumber() {
        return headerRowNumber;
    }

    public void setHeaderRowNumber(int headerRowNumber) {
        //adjust for zero index
        this.hasHeader = true;
        this.headerRowNumber = headerRowNumber-1;
        if (this.headerRowNumber < 0) {
            this.headerRowNumber = 0;
        }
    }

    @TranslatorProperty(display = "Ignore Empty Header Cells", category = TranslatorProperty.PropertyType.IMPORT,
            description = "When true any cells with empty value for header row are ignored, otherwise an empty header row cell indicates end of columns.")
    public boolean getIgnoreEmptyHeaderCells() {
        return ignoreEmptyCells;
    }

    public void setIgnoreEmptyHeaderCells(boolean ignoreEmpty) {
        ignoreEmptyCells = ignoreEmpty;
    }

    @TranslatorProperty(display = "Data Row Number", category = PropertyType.IMPORT,
            description = "Row number from which data rows start from")
    public int getDataRowNumber() {
        return dataRowNumber;
    }

    public void setDataRowNumber(int dataRowNumber) {
        //adjust for zero index
        this.hasDataRowNumber = true;
        this.dataRowNumber = dataRowNumber-1;
        if (this.dataRowNumber < 0) {
            this.dataRowNumber = 0;
        }
    }

    @TranslatorProperty(display="Excel File", category=PropertyType.IMPORT,
            description="Name of the Excel file to read metadata from", required=true)
    public String getExcelFileName() {
        return excelFileName;
    }
}
