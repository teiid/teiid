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
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.teiid.GeneratedKeys;
import org.teiid.file.VirtualFile;
import org.teiid.file.VirtualFileConnection;
import org.teiid.language.Delete;
import org.teiid.language.ExpressionValueSource;
import org.teiid.language.Insert;
import org.teiid.language.LanguageObject;
import org.teiid.language.Literal;
import org.teiid.language.SetClause;
import org.teiid.language.Update;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.UpdateExecution;

public class ExcelUpdateExecution extends BaseExcelExecution implements UpdateExecution {

    private LanguageObject command;
    private int result;
    private VirtualFile writeTo;
    private boolean modified;

    public ExcelUpdateExecution(LanguageObject command, ExecutionContext executionContext,
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
        Insert insert = (Insert)command;
        ExpressionValueSource evs = (ExpressionValueSource)insert.getValueSource();
        Row row = nextRow();
        Sheet sheet = null;
        if (row == null) {
            sheet = workbook.getSheet(this.visitor.getSheetName());
        } else {
            sheet = row.getSheet();
        }
        int last = sheet.getLastRowNum();
        Row newRow = sheet.createRow(last+1);
        List<Integer> cols = this.visitor.getProjectedColumns();
        for (int i = 0; i < cols.size(); i++) {
            int index = cols.get(i);
            setValue(newRow, index-1, ((Literal)evs.getValues().get(i)).getValue());
        }
        GeneratedKeys keys = executionContext.getCommandContext().returnGeneratedKeys(new String[] {ExcelMetadataProcessor.ROW_ID}, new Class<?>[] {TypeFacility.RUNTIME_TYPES.INTEGER});
        keys.addKey(Arrays.asList(last+1));
        result++;
        writeXLSFile();
    }

    private void handleUpdate() throws TranslatorException {
        Update update = (Update)command;
        List<SetClause> changes = update.getChanges();

        while (true) {
            Row row = nextRow();
            if (row == null) {
                break;
            }
            for (int i = 0; i < this.visitor.getProjectedColumns().size(); i++) {
                int index = this.visitor.getProjectedColumns().get(i);
                Object o = ((Literal)changes.get(i).getValue()).getValue();
                setValue(row, index-1, o);
            }
            modified = true;
            result++;
        }
    }

    private void setValue(Row row, int index, Object o) {
        Cell cell = row.getCell(index);
        if (cell == null) {
            cell = row.createCell(index);
        }
        if (o == null) {
            cell.setCellValue((String)null);
            return;
        }
        if (o instanceof Number) {
            cell.setCellValue(((Number)o).doubleValue());
        } else if (o instanceof Date) {
            cell.setCellValue((Date)o);
        } else if (o instanceof Boolean) {
            cell.setCellValue((Boolean)o);
        } else {
            cell.setCellValue(o.toString());
        }
    }

    private void handleDelete() throws TranslatorException {
        while (true) {
            Row row = nextRow();
            if (row == null) {
                break;
            }
            this.rowIterator = null;
            int start = row.getRowNum();
            Sheet sheet = row.getSheet();
            int end = sheet.getLastRowNum();
            //a different iteration style is needed, which will not perform as well for sparse documents
            for (int i = start; i <= end; i++) {
                row = sheet.getRow(i);
                if (row == null) {
                    continue;
                }
                if (row.getFirstCellNum() == -1) {
                    continue;
                }

                if (!this.visitor.allows(row.getRowNum())) {
                    continue;
                }
                sheet.removeRow(row);
                result++;
                modified = true;
            }
        }
    }

    @Override
    protected VirtualFile getNextXLSFile() throws TranslatorException {
        if (modified) {
            writeXLSFile();
        }
        return super.getNextXLSFile();
    }

    private void writeXLSFile() throws TranslatorException {
        try (OutputStream fos = (writeTo==null?getCurrentXLSFile():writeTo).openOutputStream(true)) {
            workbook.write(fos);
        } catch (IOException e) {
            throw new TranslatorException(e);
        }
    }

    public void setWriteTo(VirtualFile writeTo) {
        this.writeTo = writeTo;
    }

}
