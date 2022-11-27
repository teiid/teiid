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
package org.teiid.translator.accumulo;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.teiid.language.Select;
import org.teiid.language.visitor.SQLStringVisitor;
import org.teiid.metadata.Column;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.metadata.Table;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.TranslatorException;

public class AccumuloQueryExecution implements ResultSetExecution {
    private AccumuloConnection connection;
    private Iterator<Entry<Key,Value>> results;
    private Class<?>[] expectedColumnTypes;
    private AccumuloExecutionFactory aef;
    private AccumuloQueryVisitor visitor;
    private Entry<Key, Value> prevEntry;

    public AccumuloQueryExecution(AccumuloExecutionFactory aef, Select command,
            ExecutionContext executionContext,
            RuntimeMetadata metadata,
            AccumuloConnection connection) throws TranslatorException {
        this.aef = aef;
        this.connection = connection;
        this.expectedColumnTypes = command.getColumnTypes();
        this.visitor = new AccumuloQueryVisitor(this.aef);
        this.visitor.visitNode(command);

        if (!visitor.exceptions.isEmpty()) {
            throw visitor.exceptions.get(0);
        }
    }

    @Override
    public void execute() throws TranslatorException {
        try {
            Connector connector = this.connection.getInstance();
            List<Range> ranges = this.visitor.getRanges();
            Table scanTable = this.visitor.getScanTable();
            List<IteratorSetting> scanIterators = visitor.scanIterators();
            this.results = runQuery(this.aef, connector, this.connection.getAuthorizations(), ranges, scanTable, scanIterators);
        } catch (TableNotFoundException e) {
            // Teiid will not let the query come this far with out validating metadata for given table
            // so table in user's mind exists, it may be not be in the Accumulo, which should be treated as
            // now rows.
            this.results = null;
        }
    }

    static Iterator<Entry<Key, Value>> runQuery(AccumuloExecutionFactory aef,
            Connector connector, Authorizations auths, List<Range> ranges,
            Table scanTable, List<IteratorSetting> scanIterators)
            throws TableNotFoundException {

        if (ranges.size() <= 1) {
            Scanner scanner = connector.createScanner(SQLStringVisitor.getRecordName(scanTable), auths);
            if (!ranges.isEmpty()) {
                scanner.setRange(ranges.get(0));
            }
            if (scanIterators != null && !scanIterators.isEmpty()) {
                for (IteratorSetting it:scanIterators) {
                    scanner.addScanIterator(it);
                }
            }
            scanner.enableIsolation();
            return scanner.iterator();
        }


        // use batch scanner
        BatchScanner scanner = connector.createBatchScanner(SQLStringVisitor.getRecordName(scanTable), auths, aef.getQueryThreadsCount());
        scanner.setRanges(ranges);
        return scanner.iterator();
    }

    private SortedMap<Key, Value> readNextRow(){
        ByteSequence prevRowId = null;
        TreeMap<Key, Value> row = new TreeMap<Key, Value>();
        while(this.prevEntry != null || this.results != null && this.results.hasNext()) {
            Entry<Key, Value> entry = null;
            if (this.prevEntry != null) {
                entry = this.prevEntry;
                this.prevEntry = null;
            }
            else {
                entry = this.results.next();
            }
            ByteSequence rowId = entry.getKey().getRowData();
            if (prevRowId == null || prevRowId.equals(rowId)) {
                prevRowId= rowId;
                row.put(entry.getKey(), entry.getValue());
            }
            else {
                this.prevEntry = entry;
                return row;
            }
        }
        return row;
    }

    @Override
    public List<?> next() throws TranslatorException, DataNotAvailableException {
        SortedMap<Key, Value> rowItems = readNextRow();
        boolean rowIdAdded = false;
        LinkedHashMap<String, byte[]> values = new LinkedHashMap<String, byte[]>();

        for (Key key:rowItems.keySet()) {
            Text cf = key.getColumnFamily();
            Text cq = key.getColumnQualifier();
            Text rowid = key.getRow();
            Value value = rowItems.get(key);

            Column match = findMatchingColumn(cf, cq);
            if (!rowIdAdded) {
                values.put(AccumuloMetadataProcessor.ROWID, rowid.getBytes());
                rowIdAdded = true;
            }

            if (match != null) {
                String valueIn = match.getProperty(AccumuloMetadataProcessor.VALUE_IN, false);
                // failed to use isolated scanner, but this if check will accomplish the same in getting the
                // most top value
                if (values.get(match.getName()) == null) {
                    values.put(match.getName(), buildValue(valueIn, cq, value));
                }
            }
        }
        return nextRow(values);
    }

    private Column findMatchingColumn(Text rowCF, Text rowCQ) {
        String CF = new String(rowCF.getBytes());
        String CQ = new String(rowCQ.getBytes());
        Column column = this.visitor.lookupColumn(CF+"/"+CQ); //$NON-NLS-1$
        if (column == null) {
            // this means CQ is not defined; In this pattern CQ is used for value
            column = this.visitor.lookupColumn(CF);
        }
        return column;
    }

    private List<?> nextRow(Map<String, byte[]> values) {
        if (!values.isEmpty()) {
            ArrayList<Object> list = new ArrayList<Object>();
            for(int i = 0; i < this.visitor.projectedColumns().size(); i++) {
                Column column = this.visitor.projectedColumns().get(i);
                String colName = SQLStringVisitor.getRecordName(column);
                byte[] value = values.get(colName);
                list.add(AccumuloDataTypeManager.deserialize(value, this.expectedColumnTypes[i]));
            }
            return list;
        }
        return null;
    }

    private byte[] buildValue(String pattern, Text cq, Value value) {
        if (pattern == null) {
            return value.get();
        }
        pattern = pattern.substring(1, pattern.length()-1); // remove the curleys
        if (pattern.equals(AccumuloMetadataProcessor.ValueIn.VALUE.name())) {
            return value.get();
        }
        else if (pattern.equals(AccumuloMetadataProcessor.ValueIn.CQ.name())) {
            return cq.getBytes();
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
