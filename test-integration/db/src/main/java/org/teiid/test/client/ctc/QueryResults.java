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
package org.teiid.test.client.ctc;

import java.io.Externalizable;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.teiid.common.buffer.TupleBatch;
import org.teiid.core.types.DataTypeManager;
import org.teiid.query.sql.symbol.AliasSymbol;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.symbol.Symbol;



/**
 * This class encapsulates results associated with a query.
 * <p>
 * Results are conceptually organized as a table of columns and rows, where the columns are the data fields that were specified in
 * the query select statement, and the rows are individual records returned from the data set. The data values are arbitrary Java
 * objects in each field/record cell.
 * <p>
 * 
 * <pre>
 * 
 *  
 *            Record # |  Field1    Field2    Field3   ...    FieldN
 *           ----------|---------------------------------------------
 *              1      |  Value11   Value12   Value13         Value1N
 *              2      |  Value21   Value22   Value23         Value2N
 *              :      |     :         :         :               :
 *              M      |  ValueM1   ValueM2   ValueM3         ValueMN
 *   
 *  
 * </pre>
 * 
 * <p>
 * Methods are provided to access data by:
 * <p>
 * <ul>
 * <li>Cell value - specify field identifier and record number</li>
 * <li>Field values - specify field identifier</li>
 * <li>Record values - specify record number</li>
 * <li>Record - specify record number; returns field idents mapped to values</li>
 * </ul>
 * <p>
 * Results can be specified to be sorted based on a user-provided ordering. The ordering is a List of ElementSymbols, which should
 * match the identifiers for the results fields. This list will typically be in the order that the parameters were specified in
 * the query select statement. If no ordering list is specified, the order is the same as results fields are added to this object.
 * <p>
 */
@SuppressWarnings("nls")
public class QueryResults implements
                         Externalizable {

    /**
     * Serialization ID - this must be changed if this class is no longer serialization-compatible with old versions.
     */
    static final long serialVersionUID = 5397138282301824378L;

    /**
     * The fields in the results object: List of String
     */
    private List fields;

    /**
     * The column info for each field: Map of String --> ColumnInfo
     */
    private Map columnInfos;

    /**
     * The set of results. Each result is keyed off the variable identifier that was defined in the query's select clause. This
     * field will never be null.
     */
    private List records; // Rows of columns: List<List<Object>>

    // =========================================================================
    //                         C O N S T R U C T O R S
    // =========================================================================

    /**
     * Construct a default instance of this class.
     * <p>
     * The number of fields returned by the {@link #getFieldCount}method will be 0 after this constructor has completed. The
     * number of records returned by the {@link #getRecordCount}method will be 0 after this constructor has completed.
     * <p>
     */
    public QueryResults() {
    }

    /**
     * Construct an instance of this class, specifying the order that the elements should be inserted into the map. The number of
     * fields returned by the {@link #getFieldCount}method will be the same as the number of <code>fields</code> passed in
     * after this constructor has completed. The number of records returned by the {@link #getRecordCount}method will be 0 after
     * this constructor has completed.
     * <p>
     * 
     * @param fields
     *            The set of field identifiers that will be in the result set
     */
    public QueryResults(List fields) {
        this(fields, 0);
    }

    /**
     * Construct an instance of this class, specifying the fields and the number of records that the result set should hold. The
     * fields and number of records are used to pre-allocate memory for all the values that are expected to be indested into the
     * results set.
     * <p>
     * The number of records returned by the {@link #getRecordCount}method will be <code>numberOfRecords</code> after this
     * constructor has completed. The number of fields returned by the {@link #getFieldCount}will be the same as the size of the
     * list of fields passed in after this constructor has completed.
     * <p>
     * 
     * @param fields
     *            The ordered list of variables in select statement
     * @param numberOfRecords
     *            The number of blank records to create; records will all contain <code>null</code> values for all the fields
     * @see #addField
     */
    public QueryResults(List fields,
                        int numberOfRecords) {
        if (fields != null) {
            Iterator fieldIter = fields.iterator();
            while (fieldIter.hasNext()) {
                ColumnInfo info = (ColumnInfo)fieldIter.next();
                addField(info);
            }
            for (int k = 0; k < numberOfRecords; k++) {
                addRecord();
            }
        }
    }

    /**
     * Construct a QueryResults from a TupleBatch. Take all rows from the QueryBatch and put them into the QueryResults.
     * 
     * @param elements
     *            List of SingleElementSymbols
     * @param tupleBatch
     *            Batch of rows
     */
    public QueryResults(List elements,
                        TupleBatch tupleBatch) {
        // Add fields
        List columnInfos = createColumnInfos(elements);
        for (int i = 0; i < columnInfos.size(); i++) {
            ColumnInfo info = (ColumnInfo)columnInfos.get(i);
            addField(info);
        }

        // Add records in bulk -
        this.records = Arrays.asList(tupleBatch.getAllTuples());
    }

    // =========================================================================
    //                D A T A A C C E S S M E T H O D S
    // =========================================================================

    /**
     * Returns all the field identifiers. If the parameters in the query select statement have been provided, then the set of
     * field identifiers should be a subset of them, and ordered the same.
     * <p>
     * This method will never return <code>null</code>. The list of identifiers returned is not mutable -- changes made to this
     * list will not affect the QueryResults object.
     * 
     * @return The field identifiers
     */
    public List getFieldIdents() {
        return (fields != null) ? fields : new ArrayList();
    }

    /**
     * Get the column information given the column name.
     * 
     * @param columnName
     *            The name of the column.
     * @return Column information
     */
    public ColumnInfo getColumnInfo(String columnName) {
        if (columnInfos != null) {
            return (ColumnInfo)columnInfos.get(columnName);
        }
        return null;
    }

    /**
     * Returns the number of fields in the result set.
     * 
     * @return The number of fields
     */
    public int getFieldCount() {
        return (fields != null) ? fields.size() : 0;
    }

    /**
     * Returns the number of records in the result set.
     * <p>
     * 
     * @return The number of records
     */
    public int getRecordCount() {
        return (records != null) ? records.size() : 0;
    }

    /**
     * Get the value for the specified field and record.
     * <p>
     * The value returned is not mutable -- changes made to this value will not affect the QueryResults object.
     * <p>
     * <b>Note that results must be retrieved with the same type of data node identifier that was specified in the select
     * statement. </b>
     * <p>
     * 
     * @param columnName
     *            The unique data element identifier for the field
     * @param recordNumber
     *            The record number
     * @return The data value at the specified field and record
     * @exception IllegalArgumentException
     *                If field is not in result set
     * @exception IndexOutOfBoundsException
     *                If record is not in result set
     */
    public Object getValue(String columnName,
                           int recordNumber) throws IllegalArgumentException,
                                            IndexOutOfBoundsException {

        // This throws an IllegalArgumentException if field not in result set
        int columnNumber = getIndexOfField(columnName);

        return (records != null) ? ((List)records.get(recordNumber)).get(columnNumber) : null;
    }

    /**
     * Returns the values for the specified record. The values are ordered the same as the field identifiers in the result set,
     * which will be the same as the order of the query select parameters if they have been provided.
     * <p>
     * The list of values returned is not mutable -- changes made to this list will not affect the QueryResults object.
     * <p>
     * 
     * @param recordNumber
     *            The record number
     * @return A list containing the field values for the specified record, ordered according to the original select parameters,
     *         if defined
     */
    public List getRecordValues(int recordNumber) throws IndexOutOfBoundsException {

        if (records != null) {
            return (List)records.get(recordNumber);
        }
        throw new IndexOutOfBoundsException("Record number " + recordNumber + " is not valid.");
    }

    /**
     * Get the records contained in this result. The records are returned as a list of field values (a list of lists).
     * 
     * @return A list of lists contains the field values for each row.
     */
    public List getRecords() {
        return records;
    }

    /**
     * Returns true if the specified field is in the result set.
     * 
     * @param field
     *            Unique identifier for a data element specified in result set
     */
    public boolean containsField(String field) {
        if (fields != null && field != null) {
            Iterator iter = fields.iterator();
            while (iter.hasNext()) {
                if (((String)iter.next()).equalsIgnoreCase(field)) {
                    return true;
                }
            }
        }

        return false;
    }

    public List getTypes() {
        List typeNames = new ArrayList();

        int nFields = getFieldCount();
        for (int i = 0; i < nFields; i++) {
            String aField = (String)fields.get(i);
            typeNames.add(((ColumnInfo)columnInfos.get(aField)).getDataType());
        }
        return typeNames;
    }

    // =========================================================================
    //           D A T A M A N I P U L A T I O N M E T H O D S
    // =========================================================================

    /**
     * Add a new field into this result set. The field will be inserted in the order of the parameters in the select statement if
     * those parameters were specified upon construction of the result set; otherwise, the field will be appended to the result
     * set.
     * <p>
     * 
     * @param info
     *            The column information.
     */
    public void addField(ColumnInfo info) {
        // Add to ordered list of fields
        if (fields == null) {
            fields = new ArrayList();
        }
        fields.add(info.getName());

        // Save column information
        if (columnInfos == null) {
            columnInfos = new HashMap();
        }
        columnInfos.put(info.getName(), info);

        // Add new field to each record
        if (records != null) {
            for (int i = 0; i < records.size(); i++) {
                List record = (List)records.get(i);
                record.add(null);
            }
        }
    }

    /**
     * Add a set of fields into this result set. The fields will be inserted in the order of the parameters in the select
     * statement if those parameters were specified upon construction of the result set; otherwise, the field will be appended to
     * the result set.
     * <p>
     * 
     * @param fields
     *            The field identifiers.
     */
    public void addFields(Collection fields) {
        Iterator idents = fields.iterator();
        while (idents.hasNext()) {
            ColumnInfo ident = (ColumnInfo)idents.next();
            addField(ident);
        }
    }

    /**
     * Add a new record for all fields. The record is populated with all null values, which act as placeholders for subsequent
     * <code>setValue
     * </code> calls.
     * <p>
     * Before this method is called, the fields must already be defined.
     * <p>
     * 
     * @return The updated number of records
     */
    public int addRecord() {
        // Create a place-holder record
        int nField = getFieldCount();
        if (nField == 0) {
            throw new IllegalArgumentException("Cannot add record; no fields have been defined");
        }
        // Create a record with all null values, one for each field
        List record = new ArrayList(nField);
        for (int j = 0; j < nField; j++) {
            record.add(null);
        }
        return addRecord(record);
    }

    /**
     * Add a new record for all fields. The record must contain the same number of values as there are fields.
     * <p>
     * Before this method is called, the fields must already be defined.
     * <p>
     * 
     * @return The updated number of records
     */
    public int addRecord(List record) {
        if (record == null) {
            throw new IllegalArgumentException("Attempt to add null record.");
        }
        if (record.size() != getFieldCount()) {
            throw new IllegalArgumentException("Attempt to add record with " + record.size() + " values when " + getFieldCount() + " fields are defined.");
        }
        if (records == null) {
            records = new ArrayList();
        }
        records.add(record);
        return records.size();
    }

    /**
     * Set the value at a particular record for a field.
     * <p>
     * The specified field and record must already exist in the data set, or an exception will be thrown. The
     * {@link #addField(ColumnInfo)}method can be used to append values or new records for fields.
     * 
     * @param field
     *            The unique data element identifier for the field
     * @param recordNumber
     *            The record number
     * @exception IndexOutOfBoundsException
     *                If the specified record does not exist
     */
    public void setValue(String field,
                         int recordNumber,
                         Object value) throws IllegalArgumentException,
                                      IndexOutOfBoundsException {

        List record = (List)records.get(recordNumber);
        int fieldIndex = getIndexOfField(field);
        record.set(fieldIndex, value);
    }

    // =========================================================================
    //                     H E L P E R M E T H O D S
    // =========================================================================

    /**
     * Returns the index of the specified field is in the result set. An exception is thrown if the field is not in the set.
     * 
     * @param field
     *            Unique identifier for a data element specified in result set
     * @return The index of the field in the set of fields
     * @exception IllegalArgumentException
     *                If field is not in result set
     */
    public int getIndexOfField(String field) throws IllegalArgumentException {

        int index = -1;
        if (fields != null && field != null) {
            Iterator iter = fields.iterator();
            for (int i = 0; iter.hasNext(); i++) {
                if (field.equalsIgnoreCase((String)iter.next())) {
                    index = i;
                    break;
                }
            }
        }

        if (index == -1) {
            throw new IllegalArgumentException("Field with identifier " + field + " is not in result set");
        }
        return index;
    }

    /**
     * Convert a list of SingleElementSymbols to a List of ColumnInfo objects.
     * 
     * @param symbols
     *            List of SingleElementSymbols
     * @return List of ColumnInfos
     */
    public static List createColumnInfos(List symbols) {
        List infos = new ArrayList(symbols.size());
        Iterator iter = symbols.iterator();
        while (iter.hasNext()) {
            Expression symbol = (Expression)iter.next();
            String name = Symbol.getName(symbol);
            if (symbol instanceof AliasSymbol) {
                AliasSymbol alias = (AliasSymbol)symbol;
                symbol = alias.getSymbol();
            }
            if (symbol instanceof ElementSymbol) {
                ElementSymbol element = (ElementSymbol)symbol;
                GroupSymbol group = element.getGroupSymbol();
                Object groupID = null;
                if (group != null) {
                    groupID = group.getMetadataID();
                }
                infos.add(new ColumnInfo(name, DataTypeManager.getDataTypeName(element.getType()), element.getType(), groupID,
                                         element.getMetadataID()));
            } else { // ExpressionSymbol or AggregateSymbol
                // Expressions don't map to a single element or group, so don't save that info
                infos.add(new ColumnInfo(name, DataTypeManager.getDataTypeName(symbol.getType()), symbol.getType()));
            }
        }

        return infos;
    }

    // =========================================================================
    //          O V E R R I D D E N O B J E C T M E T H O D S
    // =========================================================================

    /** Compares with another result set */
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }

        if (!this.getClass().isInstance(object)) {
            return false;
        }

        QueryResults other = (QueryResults)object;

        // First compare fields
        if (!this.getFieldIdents().equals(other.getFieldIdents())) {
            return false;
        }

        List thisRecords = this.getRecords();
        List otherRecords = other.getRecords();

        if (thisRecords == null) {
            if (otherRecords == null) {
                return true;
            }
            return false;
        }
        if (otherRecords == null) {
            return false;
        }
        return thisRecords.equals(otherRecords);
    }

    /** Returns a string representation of an instance of this class. */
    public String toString() {
        StringBuffer buffer = new StringBuffer("Query Results...\n"); //$NON-NLS-1$
        buffer.append(printFieldIdentsAndTypes(this.getFieldIdents(), this.columnInfos));
        buffer.append("\n"); //$NON-NLS-1$
        for (int r = 0; r < this.getRecordCount(); r++) {
            buffer.append(r);
            buffer.append(": "); //$NON-NLS-1$

            List record = this.getRecordValues(r);
            for (int c = 0; c < this.getFieldCount(); c++) {
                buffer.append(record.get(c));
                if (c < this.getFieldCount() - 1) {
                    buffer.append(", "); //$NON-NLS-1$
                }
            }
            buffer.append("\n"); //$NON-NLS-1$
        }
        return buffer.toString();
    }

    private static String printFieldIdentsAndTypes(List fieldIdents,
                                                   Map columnInfos) {
        StringBuffer buf = new StringBuffer();
        Iterator fieldItr = fieldIdents.iterator();
        while (fieldItr.hasNext()) {
            String aField = (String)fieldItr.next();
            if (aField != null) {
                buf.append("["); //$NON-NLS-1$
                buf.append(aField);
                buf.append(" - ["); //$NON-NLS-1$
                ColumnInfo colInfo = (ColumnInfo)columnInfos.get(aField);
                buf.append(colInfo.getDataType());
                buf.append(", "); //$NON-NLS-1$
                buf.append(colInfo.getJavaClass());
                buf.append("]"); //$NON-NLS-1$
            }
            buf.append("] "); //$NON-NLS-1$
        }

        return buf.toString();
    }

    // =========================================================================
    //                      S E R I A L I Z A T I O N
    // =========================================================================

    /**
     * Implements Externalizable interface to read serialized form
     * 
     * @param s
     *            Input stream to serialize from
     */
    public void readExternal(java.io.ObjectInput s) throws ClassNotFoundException,
                                                   IOException {
        int numFields = s.readInt();
        if (numFields > 0) {
            fields = new ArrayList(numFields);
            columnInfos = new HashMap();
            for (int i = 0; i < numFields; i++) {
                String fieldName = s.readUTF();
                fields.add(fieldName);

                Object colInfo = s.readObject();
                columnInfos.put(fieldName, colInfo);
            }
        }

        int numRows = s.readInt();
        if (numRows > 0) {
            records = new ArrayList(numRows);
            for (int row = 0; row < numRows; row++) {
                List record = new ArrayList(numFields);
                for (int col = 0; col < numFields; col++) {
                    record.add(s.readObject());
                }
                records.add(record);
            }
        }
    }

    /**
     * Implements Externalizable interface to write serialized form
     * 
     * @param s
     *            Output stream to serialize to
     */
    public void writeExternal(java.io.ObjectOutput s) throws IOException {
        // Write column names and column information
        int numFields = 0;
        if (fields == null) {
            s.writeInt(0);
        } else {
            numFields = fields.size();
            s.writeInt(numFields);
            for (int i = 0; i < numFields; i++) {
                String fieldName = (String)fields.get(i);
                s.writeUTF(fieldName);
                s.writeObject(columnInfos.get(fieldName));
            }
        }

        // Write record data
        if (records == null) {
            s.writeInt(0);
        } else {
            int numRows = records.size();
            s.writeInt(numRows);
            for (int row = 0; row < numRows; row++) {
                List record = (List)records.get(row);
                for (int col = 0; col < numFields; col++) {
                    s.writeObject(record.get(col));
                }
            }
        }
    }

    // =========================================================================
    //                        I N N E R C L A S S E S
    // =========================================================================

    /**
     * Represents all information about a column.
     */
    public static class ColumnInfo implements
                                  Serializable {

        /**
         * Serialization ID - this must be changed if this class is no longer serialization-compatible with old versions.
         */
        static final long serialVersionUID = -7131157612965891051L;

        private String name;
        private String dataType;
        private Class javaClass;

        private Object groupID; // fully qualified group name
        private Object elementID; // short name

        public ColumnInfo(String name,
                          String dataType,
                          Class javaClass) {
            if (name == null) {
                throw new IllegalArgumentException("QueryResults column cannot have name==null");
            }
            this.name = name;
            this.dataType = dataType;
            this.javaClass = javaClass;
        }

        public ColumnInfo(String name,
                          String dataType,
                          Class javaClass,
                          Object groupID,
                          Object elementID) {
            this(name, dataType, javaClass);
            this.groupID = groupID;
            this.elementID = elementID;
        }

        public String getName() {
            return this.name;
        }

        public String getDataType() {
            return this.dataType;
        }

        public Class getJavaClass() {
            return this.javaClass;
        }

        /**
         * May be null
         */
        public Object getGroupID() {
            return this.groupID;
        }

        /**
         * May be null
         */
        public Object getElementID() {
            return this.elementID;
        }

        public String toString() {
            StringBuffer str = new StringBuffer("Column["); //$NON-NLS-1$
            str.append(this.name);
            str.append(", "); //$NON-NLS-1$
            str.append(this.dataType);
            if (this.groupID != null) {
                str.append(", "); //$NON-NLS-1$
                str.append(this.groupID);
                str.append("."); //$NON-NLS-1$
                str.append(this.elementID);
            }
            str.append("]"); //$NON-NLS-1$
            return str.toString();
        }
    }

} // END CLASS
