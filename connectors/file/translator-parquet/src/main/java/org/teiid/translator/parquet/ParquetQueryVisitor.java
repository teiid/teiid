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

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

import org.teiid.language.visitor.HierarchyVisitor;
import org.teiid.metadata.Table;
import org.teiid.translator.TranslatorException;

public class ParquetQueryVisitor extends HierarchyVisitor {

    public static int ROW_ID_INDEX = -1;

    protected Stack<Object> onGoingExpression = new Stack<Object>();
    private List<Integer> projectedColumns = new ArrayList<Integer>();
    protected ArrayList<TranslatorException> exceptions = new ArrayList<TranslatorException>();

    private Table table;
    private String parquetPath;
    private int firstDataRowNumber;

    public List<Integer> getProjectedColumns() {
        return projectedColumns;
    }

    public int getFirstDataRowNumber() {
        return firstDataRowNumber;
    }

    public ArrayList<TranslatorException> getExceptions() {
        return exceptions;
    }

    public Table getTable() {
        return table;
    }

    public String getParquetPath() {
        return parquetPath;
    }

}
