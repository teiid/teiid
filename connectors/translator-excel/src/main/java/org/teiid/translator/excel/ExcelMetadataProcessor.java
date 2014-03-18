/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
 */
package org.teiid.translator.excel;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

import javax.resource.ResourceException;

import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.Column;
import org.teiid.metadata.Column.SearchType;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.Table;
import org.teiid.translator.FileConnection;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TypeFacility;

public class ExcelMetadataProcessor {
	public static final String FILE = MetadataFactory.EXCEL_URI+"FILE"; //$NON-NLS-1$
	public static final String CELL_NUMBER = MetadataFactory.EXCEL_URI+"CELL_NUMBER"; //$NON-NLS-1$
	public static final String FIRST_DATA_ROW_NUMBER = MetadataFactory.EXCEL_URI+"FIRST_DATA_ROW_NUMBER"; //$NON-NLS-1$
	public static final String ROW_ID = "ROW_ID"; //$NON-NLS-1$
	
	private MetadataFactory mf;
	private FileConnection conn;
	private String excelFileName;
	private int headerRowNumber = 0;
	private boolean hasHeader = false;
	private int dataRowNumber = 0;
	private boolean hasDataRowNumber = false;

	public ExcelMetadataProcessor(MetadataFactory metadataFactory, FileConnection conn) {
		this.mf = metadataFactory;
		this.conn = conn;
	}

	public void processMetadata() throws TranslatorException {
		if (this.excelFileName == null) {
			throw new TranslatorException(ExcelPlugin.Event.TEIID23004, ExcelPlugin.Util.gs(ExcelPlugin.Event.TEIID23004, "importer.ExcelFileName")); //$NON-NLS-1$
		}
		try {
			File xlsFile = this.conn.getFile(this.excelFileName);
			if (xlsFile.isDirectory() || !xlsFile.exists()) {
				throw new TranslatorException(ExcelPlugin.Event.TEIID23005, ExcelPlugin.Util.gs(ExcelPlugin.Event.TEIID23005, xlsFile.getName()));
			}
			
			String extension = getFileExtension(xlsFile);
			FileInputStream xlsFileStream = new FileInputStream(xlsFile);
			try {
				Workbook workbook = null;
				if (extension.equalsIgnoreCase("xls")) { //$NON-NLS-1$
					workbook = new HSSFWorkbook(xlsFileStream);
				}
				else if (extension.equalsIgnoreCase("xlsx")) { //$NON-NLS-1$
					workbook = new XSSFWorkbook(xlsFileStream);
				}
				int sheetCount = workbook.getNumberOfSheets();
				for (int i = 0; i < sheetCount; i++) {
					Sheet sheet = workbook.getSheetAt(i);
					addTable(sheet, xlsFile.getName());
				}
			} finally {
				xlsFileStream.close();
			}
		} catch (ResourceException e) {
			throw new TranslatorException(e);
		} catch (IOException e) {
			throw new TranslatorException(e);
		}
	}

	private void addTable(Sheet sheet, String xlsName) {
		int firstRowNumber = sheet.getFirstRowNum();
		Row headerRow = null;
		int firstCellNumber = -1;
		if (this.hasHeader) {
			headerRow = sheet.getRow(this.headerRowNumber);
			firstRowNumber = this.headerRowNumber;
			firstCellNumber = headerRow.getFirstCellNum();
			if (firstCellNumber == -1) {
				LogManager.logInfo(LogConstants.CTX_CONNECTOR, ExcelPlugin.Util.gs(ExcelPlugin.Event.TEIID23006, xlsName));
				return;
			}
		}

		if (headerRow == null) {
			while (firstCellNumber == -1) {
				headerRow = sheet.getRow(firstRowNumber++);
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
		table.setProperty(ExcelMetadataProcessor.FILE, xlsName);
		
		// add implicit row_id column based on row number from excel sheet 
		Column column = mf.addColumn(ROW_ID, TypeFacility.RUNTIME_NAMES.INTEGER, table);
		column.setSearchType(SearchType.All_Except_Like);
		column.setProperty(CELL_NUMBER, ROW_ID);
		mf.addPrimaryKey("PK0", Arrays.asList(ROW_ID), table); //$NON-NLS-1$
		column.setUpdatable(false);
				
				
		Row dataRow = null;
		int lastCellNumber = headerRow.getLastCellNum();
		
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
				Cell dataCell = dataRow.getCell(j);
				column = mf.addColumn(cellName(headerCell, columnCount), cellType(headerCell, dataCell), table);
				column.setSearchType(SearchType.Unsearchable);
				column.setProperty(ExcelMetadataProcessor.CELL_NUMBER, String.valueOf(j+1));
			}
		}
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
	
	public void setHeaderRowNumber(String headerNumber) {
		//adjust for zero index
		this.headerRowNumber = Integer.parseInt(headerNumber)-1;
		this.hasHeader = true;
	}	
	
	public void setDataRowNumber(String dataNumber) {
		//adjust for zero index
		this.dataRowNumber = Integer.parseInt(dataNumber)-1;
		this.hasDataRowNumber = true;
	}	
	
	static String getFileExtension(File xlsFile) {
		int idx = xlsFile.getName().lastIndexOf('.');
		String extension = "xls"; //$NON-NLS-1$
		if (idx > 0) {
		    extension = xlsFile.getName().substring(idx+1);
		}
		return extension;
	} 	
	
	private String getCellType(Cell cell) {
		switch (cell.getCellType()) {
		case Cell.CELL_TYPE_STRING:
			return TypeFacility.RUNTIME_NAMES.STRING;
		case Cell.CELL_TYPE_BOOLEAN:
			return TypeFacility.RUNTIME_NAMES.BOOLEAN;
		default:
			return TypeFacility.RUNTIME_NAMES.DOUBLE;
		}
	}
}
