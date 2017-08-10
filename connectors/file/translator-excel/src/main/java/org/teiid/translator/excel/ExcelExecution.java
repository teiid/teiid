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
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.SQLXML;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import javax.resource.ResourceException;

import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.FormulaEvaluator;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
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
import org.teiid.language.Select;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.FileConnection;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.TranslatorException;


public class ExcelExecution implements ResultSetExecution {
	@SuppressWarnings("unused")
	private ExecutionContext executionContext; 
	@SuppressWarnings("unused")
	private RuntimeMetadata metadata; 
	private FileConnection connection;
    
    // Execution state
	private Iterator<Row> rowIterator;
	private Row currentRow;
	private File[] xlsFiles;
	private AtomicInteger fileCount = new AtomicInteger();
	private ExcelQueryVisitor visitor;
	private FormulaEvaluator evaluator;
	private FileInputStream xlsFileStream;
	private Class<?>[] expectedColumnTypes;
	private DataFormatter dataFormatter;

	public ExcelExecution(Select query, ExecutionContext executionContext,
			RuntimeMetadata metadata, FileConnection connection)
			throws TranslatorException {

		this.executionContext = executionContext;
        this.metadata = metadata;
        this.connection = connection;
        this.expectedColumnTypes = query.getColumnTypes();
        this.visitor = new ExcelQueryVisitor();
		this.visitor.visitNode(query);
		
		if (!visitor.exceptions.isEmpty()) {
			throw visitor.exceptions.get(0);
		}
    }
    
    @Override
    public void execute() throws TranslatorException {
    	try {
			this.xlsFiles = FileConnection.Util.getFiles(this.visitor.getXlsPath(), this.connection, true);
			this.rowIterator = readXLSFile(xlsFiles[fileCount.getAndIncrement()]);
		} catch (ResourceException e) {
			throw new TranslatorException(e);
		}    
    }

	private Iterator<Row> readXLSFile(File xlsFile) throws TranslatorException {
		try {
			this.xlsFileStream = new FileInputStream(xlsFile);
			Iterator<Row> rowIter = null;			
			String extension = ExcelMetadataProcessor.getFileExtension(xlsFile);
			if (extension.equalsIgnoreCase("xls")) { //$NON-NLS-1$
				HSSFWorkbook workbook = new HSSFWorkbook(this.xlsFileStream);
				HSSFSheet sheet = workbook.getSheet(this.visitor.getSheetName());
				this.evaluator = workbook.getCreationHelper().createFormulaEvaluator();
				rowIter = sheet.iterator();
				
			}
			else if (extension.equalsIgnoreCase("xlsx")) { //$NON-NLS-1$
				XSSFWorkbook workbook = new XSSFWorkbook(this.xlsFileStream);
				XSSFSheet sheet = workbook.getSheet(this.visitor.getSheetName());
				this.evaluator = workbook.getCreationHelper().createFormulaEvaluator();
				rowIter = sheet.iterator();
			}
			else {
				throw new TranslatorException(ExcelPlugin.Event.TEIID23000, ExcelPlugin.Util.gs(ExcelPlugin.Event.TEIID23000));
			}
			
			// skip up to the first data row
			if (this.visitor.getFirstDataRowNumber() > 0 && rowIter != null) {
				while(rowIter.hasNext()) {
					this.currentRow = rowIter.next();
					if (this.currentRow.getRowNum() >= this.visitor.getFirstDataRowNumber()) {
						break;
					}
				}
			}
			return rowIter;
		} catch (IOException e) {
			throw new TranslatorException(e);
		}
	}

    @Override
    public List<?> next() throws TranslatorException, DataNotAvailableException {
        while (hasNext()) {
        	Row row = nextRow();
        	// when the first cell number is -1, then it is empty row, skip it
        	if (row.getFirstCellNum() == -1) {
        		continue;
        	}
        	
        	if (!this.visitor.allows(row.getRowNum())) {
        		continue;
        	}
        	return projectRow(row);
        }
        return null;
    }
    
    private boolean hasNext() throws TranslatorException {
    	if (this.currentRow != null) {
    		return true;
    	}
    	
    	boolean hasNext = false;
    	if (this.rowIterator != null) {
    		hasNext = this.rowIterator.hasNext();
    	}
    	
    	if (!hasNext) {
    		this.rowIterator = null;
    		File nextXlsFile = getNextXLSFile();
    		if (nextXlsFile != null) {
    			this.rowIterator = readXLSFile(nextXlsFile);
    			hasNext = this.rowIterator.hasNext();
    		}
    	}
    	return hasNext;
    }
    
    private File getNextXLSFile() {
    	if (this.xlsFiles.length > this.fileCount.get()) {
    		try {
				this.xlsFileStream.close();
			} catch (IOException e) {
				// ignore
			}
    		return this.xlsFiles[this.fileCount.getAndIncrement()];
    	}
    	return null;
    }
    
    private Row nextRow() {
    	if (this.currentRow != null) {
    		Row row = this.currentRow;
    		this.currentRow = null;
    		return row;
    	}
    	Row row = null;
        if (this.rowIterator != null && this.rowIterator.hasNext()) {
        	row = this.rowIterator.next();
        }
    	return row;
    }

    /**
     * @param row
     * @param neededColumns
     */
    List<Object> projectRow(Row row) throws TranslatorException {
        ArrayList<Object> output = new ArrayList<Object>(this.visitor.getProjectedColumns().size());
        
        int id = row.getRowNum()+1;
        
        int i = -1;
        for (int index:this.visitor.getProjectedColumns()) {
        	
        	i++;
        	// check if the row is ROW_ID
        	if (index == -1) {
        		output.add(id);
        		continue;
        	}
        	
        	Cell cell = row.getCell(index-1, Row.RETURN_BLANK_AS_NULL);
        	if (cell == null) {
        		output.add(null);
        		continue;
        	}
        	switch (this.evaluator.evaluateInCell(cell).getCellType()) {
                case Cell.CELL_TYPE_NUMERIC:
                    output.add(convertFromExcelType(cell.getNumericCellValue(), cell, this.expectedColumnTypes[i]));
                    break;
                case Cell.CELL_TYPE_STRING:
                    output.add(convertFromExcelType(cell.getStringCellValue(), this.expectedColumnTypes[i]));
                    break;
                case Cell.CELL_TYPE_BOOLEAN:
                    output.add(convertFromExcelType(cell.getBooleanCellValue(), this.expectedColumnTypes[i]));
                    break;
                default:
                	output.add(null);
                    break;
            }   
        }
        
        return output;    
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
    	if (this.xlsFileStream != null) {
	    	try {
				this.xlsFileStream.close();
			} catch (IOException e) {
			}
    	}
    }

    @Override
    public void cancel() throws TranslatorException {

    }
    
    public void setDataFormatter(DataFormatter dataFormatter) {
        this.dataFormatter = dataFormatter;
    }
}
