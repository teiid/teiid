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


import java.util.ArrayList;
import java.util.List;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.teiid.file.VirtualFileConnection;
import org.teiid.language.Select;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.TranslatorException;


public class ExcelExecution extends BaseExcelExecution implements ResultSetExecution {

    private Class<?>[] expectedColumnTypes;

    public ExcelExecution(Select query, ExecutionContext executionContext,
            RuntimeMetadata metadata, VirtualFileConnection connection, boolean immutable)
            throws TranslatorException {
        super(executionContext, metadata, connection, immutable);
        this.expectedColumnTypes = query.getColumnTypes();
        visit(query);
    }

    @Override
    public List<?> next() throws TranslatorException, DataNotAvailableException {
        Row row = nextRow();
        if (row == null) {
            return null;
        }
        return projectRow(row);
    }

    /**
     * @param row
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

            Cell cell = row.getCell(index-1, Row.MissingCellPolicy.RETURN_BLANK_AS_NULL);
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

}
