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

package org.teiid.translator.google.api;

import java.util.List;
import java.util.Map;

import org.teiid.resource.api.Connection;
import org.teiid.translator.google.api.metadata.SpreadsheetInfo;
import org.teiid.translator.google.api.metadata.Worksheet;
import org.teiid.translator.google.api.result.RowsResult;
import org.teiid.translator.google.api.result.UpdateResult;


/**
 * Connection to GoogleSpreadsheet API.
 *
 */
public interface GoogleSpreadsheetConnection extends Connection {
    public RowsResult executeQuery(Worksheet worksheet, String query, Integer offset, Integer limit, int batchSize);
    public UpdateResult updateRows(Worksheet worksheet, String criteria, List<UpdateSet> set);
    public UpdateResult deleteRows(Worksheet worksheet, String criteria);
    public UpdateResult executeRowInsert(Worksheet worksheet, Map<String,Object> pair);
    /**
     * Returns information about existing Spreadsheets and worksheets.
     * @return
     */
    public SpreadsheetInfo getSpreadsheetInfo();
}
