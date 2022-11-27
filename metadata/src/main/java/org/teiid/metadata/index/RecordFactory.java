/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 */

package org.teiid.metadata.index;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.teiid.core.index.IEntryResult;
import org.teiid.core.util.Assertion;
import org.teiid.internal.core.index.EntryResult;
import org.teiid.internal.core.index.IIndexConstants;
import org.teiid.metadata.*;
import org.teiid.metadata.BaseColumn.NullType;
import org.teiid.metadata.Column.SearchType;
import org.teiid.metadata.Datatype.Variety;
import org.teiid.metadata.KeyRecord.Type;


/**
 * RuntimeAdapter
 */
public class RecordFactory {
    public static final String PROTOCOL = "mmuuid"; //$NON-NLS-1$

    /** Delimiter used to separate the URI string from the URI fragment */
    public static final String URI_REFERENCE_DELIMITER = "#"; //$NON-NLS-1$

    public static final int INDEX_RECORD_BLOCK_SIZE = IIndexConstants.BLOCK_SIZE - 32;

    /**
     * The version number associated with any index records prior to the point
     * when version information was encoded in newly created records
     */
    public static final int NONVERSIONED_RECORD_INDEX_VERSION = 0;

    /**
     * The version number that is associated with the change made to change the list
     * delimiter.  Added 07/22/2004.
     * @since 4.1.1
     */
    public static final int DELIMITER_INDEX_VERSION = 1;

    /**
     * The version number that is associated with the change made to add materialization
     * property on tables. Added 08/18/2004.
     * @since 4.2
     */
    public static final int TABLE_MATERIALIZATION_INDEX_VERSION = 2;

    /**
     * The version number that is associated with the change made to add native type
     * property on columns. Added 08/24/2004.
     * @since 4.2
     */
    public static final int COLUMN_NATIVE_TYPE_INDEX_VERSION = 3;

    /**
     * The version number that is associated with the change made to add an input parameter
     * flag on columns.  The flag is used to indicate if an element for a virtual table
     * represents an input parameter.  This change was made to support the Procedural-Relational
     * Mapping project.   Added 09/29/2004.
     * @since 4.2
     */
    public static final int COLUMN_INPUT_PARAMETER_FLAG_INDEX_VERSION = 4;

    /**
     * The version number that is associated with the change made to remove property value
     * pairs from the annotation records any properties on annotations would now be indexed
     * as part of the properties index. Added 12/14/2004.
     * @since 4.2
     */
    public static final int ANNOTATION_TAGS_INDEX_VERSION = 5;

    /**
     * The version number that is associated with the change made to add uuid for the
     * transformation mapping root on the transformation records, uuids would now be indexed
     * as part of the transformation index. Added 1/13/2005.
     * @since 4.2
     */
    public static final int TRANSFORMATION_UUID_INDEX_VERSION = 6;

    /**
     * The version number that is associated with the change made to add count of null and
     * distinct values for columns on the column records 7/8/2005.
     * @since 4.2
     */
    public static final int COLUMN_NULL_DISTINCT_INDEX_VERSION = 7;

    /**
     * The version number that is associated with the change made to add
     * primitive type ID on datatype records 02/28/2006.
     * @since 5.0
     */
    public static final int PRIMITIVE_TYPE_ID_INDEX_VERSION = 8;

    /**
     * The version number that is associated with the change made to add
     * an update count to physical stored and XQuery procedures 04/29/2008.
     * @since 5.0
     */
    public static final int PROCEDURE_UPDATE_COUNT_VERSION = 9;

    public static final int NONZERO_UNKNOWN_CARDINALITY = 10;

    /**
     * The version number that is encoded with all newly created index records
     */
    public static final int CURRENT_INDEX_VERSION = PROCEDURE_UPDATE_COUNT_VERSION;

    private int version = NONVERSIONED_RECORD_INDEX_VERSION;

    protected String parentId;

    /**
     * Return a collection of {@link AbstractMetadataRecord}
     * instances for the result obtained from executing <code>queryEntriesMatching</code>
     * @param queryResult
     */
    public List<AbstractMetadataRecord> getMetadataRecord(final IEntryResult[] queryResult) {
        final List records = new ArrayList(queryResult.length);
        for (int i = 0; i < queryResult.length; i++) {
            final AbstractMetadataRecord record = getMetadataRecord(queryResult[i].getWord());
            if (record != null) {
                records.add(record);
            }
        }
        return records;
    }

    /**
     * Return the {@link AbstractMetadataRecord}
     * instances for specified IEntryResult.
     * @param record
     */
    protected AbstractMetadataRecord getMetadataRecord(final char[] record) {
        parentId = null;
        if (record == null || record.length == 0) {
            return null;
        }
        switch (record[0]) {
            case MetadataConstants.RECORD_TYPE.MODEL: return createModelRecord(record);
            case MetadataConstants.RECORD_TYPE.TABLE: return createTableRecord(record);
            case MetadataConstants.RECORD_TYPE.JOIN_DESCRIPTOR: return null;
            case MetadataConstants.RECORD_TYPE.CALLABLE: return createProcedureRecord(record);
            case MetadataConstants.RECORD_TYPE.CALLABLE_PARAMETER: return createProcedureParameterRecord(record);
            case MetadataConstants.RECORD_TYPE.COLUMN: return createColumnRecord(record);
            case MetadataConstants.RECORD_TYPE.ACCESS_PATTERN: return createColumnSetRecord(record, new KeyRecord(KeyRecord.Type.AccessPattern));
            case MetadataConstants.RECORD_TYPE.INDEX: return createColumnSetRecord(record, new KeyRecord(KeyRecord.Type.Index));
            case MetadataConstants.RECORD_TYPE.RESULT_SET: return createColumnSetRecord(record, new ColumnSet());
            case MetadataConstants.RECORD_TYPE.UNIQUE_KEY: return createColumnSetRecord(record, new KeyRecord(KeyRecord.Type.Unique));
            case MetadataConstants.RECORD_TYPE.PRIMARY_KEY: return createColumnSetRecord(record, new KeyRecord(KeyRecord.Type.Primary));
            case MetadataConstants.RECORD_TYPE.FOREIGN_KEY: return createForeignKeyRecord(record);
            case MetadataConstants.RECORD_TYPE.DATATYPE: return createDatatypeRecord(record);
            case MetadataConstants.RECORD_TYPE.SELECT_TRANSFORM:
            case MetadataConstants.RECORD_TYPE.INSERT_TRANSFORM:
            case MetadataConstants.RECORD_TYPE.UPDATE_TRANSFORM:
            case MetadataConstants.RECORD_TYPE.DELETE_TRANSFORM:
            case MetadataConstants.RECORD_TYPE.MAPPING_TRANSFORM:
            case MetadataConstants.RECORD_TYPE.PROC_TRANSFORM: return createTransformationRecord(record);
            default:
                return null;
        }
    }

    /**
     * Append the specified IEntryResult[] to the IEntryResult
     * to create a single result representing an index entry that
     * was split across multiple index records.
     * @param result
     * @param continuationResults
     * @param blockSize
     */
    public static IEntryResult joinEntryResults(final IEntryResult result,
                                                final IEntryResult[] continuationResults,
                                                final int blockSize) {
        // If the IEntryResult is not continued on another record, return the original
        char[] baseResult = result.getWord();
        if (baseResult.length < blockSize || baseResult[blockSize-1] != MetadataConstants.RECORD_TYPE.RECORD_CONTINUATION) {
            return result;
        }

        // Extract the UUID string from the original result
        String baseStr  = new String(baseResult);
        String objectID = extractUUIDString(result);

        // Create and initialize a StringBuffer to store the concatenated result
        StringBuffer sb = new StringBuffer();
        sb.append(baseStr.substring(0,blockSize-1));

        // Append the continuation results onto the original -
        // assumes the IEntryResult[] are in ascending order of segment number
        final IEntryResult[] sortedResults = sortContinuationResults(objectID,continuationResults);
        for (int i = 0; i < sortedResults.length; i++) {
            char[] continuation = sortedResults[i].getWord();
            int segNumber  = getContinuationSegmentNumber(objectID,sortedResults[i]);
            int beginIndex = objectID.length() + Integer.toString(segNumber).length() + 5;
            for (int j = beginIndex; j < continuation.length; j++) {
                if (j < blockSize-1) {
                    sb.append(continuation[j]);
                }
            }
        }

        return new EntryResult(sb.toString().toCharArray(),result.getFileReferences());
    }

    private static IEntryResult[] sortContinuationResults(final String objectID, final IEntryResult[] continuationResults) {
        // If the array length is less than 10, then we should be able to safely
        // assume the IEntryResult[] are in ascending order of segment count
        if (continuationResults.length < 10) {
            return continuationResults;
        }

        // If the number of continuation records is 10 or greater then we need to sort them
        // by segment count since continuation records 10-19 will appear before records 1-9
        // in the array
        final IEntryResult[] sortedResults = new IEntryResult[continuationResults.length];
        for (int i = 0; i < continuationResults.length; i++) {
            int segNumber = getContinuationSegmentNumber(objectID,continuationResults[i]);
            sortedResults[segNumber-1] = continuationResults[i];
        }
        return sortedResults;
    }

    public static int getContinuationSegmentNumber(final String objectID, final IEntryResult continuationResult) {
        // The header portion of the continuation record is of the form:
        // RECORD_CONTINUATION|objectID|segmentCount|
        char[] record  = continuationResult.getWord();

        int segNumber = -1;

        int index = objectID.length() + 4;
        if (record[index+1] == IndexConstants.RECORD_STRING.RECORD_DELIMITER) {
            // segment count < 10
            segNumber = Character.getNumericValue(record[index]);
        } else if (record[index+2] == IndexConstants.RECORD_STRING.RECORD_DELIMITER) {
            // 9 < segment count < 100
            char[] temp = new char[] {record[index], record[index+1]};
            String segCount = new String(temp);
            segNumber = Integer.parseInt(segCount);
        } else if (record[index+3] == IndexConstants.RECORD_STRING.RECORD_DELIMITER) {
            // 99 < segment count < 1000
            char[] temp = new char[] {record[index], record[index+1], record[index+2]};
            String segCount = new String(temp);
            segNumber = Integer.parseInt(segCount);
        }
        return segNumber;
    }

    /**
     * Extract the UUID string from the IEntryResult
     * @param result
     */
    public static String extractUUIDString(final IEntryResult result) {
        char[] word = result.getWord();
        String baseStr = new String(word);
        int beginIndex = baseStr.indexOf(PROTOCOL);
        int endIndex   = word.length;
        Assertion.assertTrue(beginIndex != -1);
        for (int i = beginIndex; i < word.length; i++) {
            if (word[i] == IndexConstants.RECORD_STRING.RECORD_DELIMITER) {
                endIndex = i;
                break;
            }
        }
        Assertion.assertTrue(beginIndex < endIndex);
        return new String(baseStr.substring(beginIndex,endIndex));
    }

    // ==================================================================================
    //                    P R O T E C T E D   M E T H O D S
    // ==================================================================================

    /**
     * Create a ModelRecord instance from the specified index record
     */
    public Schema createModelRecord(final char[] record) {
        final List<String> tokens = getStrings(record, IndexConstants.RECORD_STRING.RECORD_DELIMITER);
        final Schema model = new Schema();

        // The tokens are the standard header values
        int tokenIndex = 0;
        setRecordHeaderValues(model, tokens.get(tokenIndex++), tokens.get(tokenIndex++), tokens.get(tokenIndex++),
                             tokens.get(tokenIndex++), tokens.get(tokenIndex++),
                             tokens.get(tokenIndex++));

        // The next token is the max set size
        tokenIndex++;

        // The next token is the model type
        model.setPhysical(Integer.parseInt(tokens.get(tokenIndex++)) == 0);

        // The next token is the primary metamodel Uri
        model.setPrimaryMetamodelUri(getObjectValue(tokens.get(tokenIndex++)));

        // The next token are the supports flags
        tokens.get(tokenIndex++);

        // The next tokens are footer values - the footer will contain the version number for the index record
        setRecordFooterValues(model, tokens, tokenIndex);

        return model;
    }

    /**
     * Create a TransformationRecord instance from the specified index record
     */
    public TransformationRecordImpl createTransformationRecord(final char[] record) {
        final List<String> tokens = getStrings(record, IndexConstants.RECORD_STRING.RECORD_DELIMITER);
        final TransformationRecordImpl transform = new TransformationRecordImpl();

        // Extract the index version information from the record
        int indexVersion = getIndexVersion(record);

        // The tokens are the standard header values
        int tokenIndex = 2;

        // The next token is the UUID of the transformed object
        transform.setUUID(getObjectValue(tokens.get(tokenIndex++)));

        // The next token is the UUID of the transformation object
        if(includeTransformationUUID(indexVersion)) {
            tokenIndex++;
            //transform.setUUID(getObjectValue((tokens.get(tokenIndex++))));
        }

        // The next token is the transformation definition
        transform.setTransformation(getObjectValue(tokens.get(tokenIndex++)));

        // The next token are the list of bindings
        List bindings = getStrings(tokens.get(tokenIndex++), getListDelimiter(indexVersion));
        transform.setBindings(bindings);

        // The next token are the list of schemaPaths
        List schemaPaths = getStrings(tokens.get(tokenIndex++), getListDelimiter(indexVersion));
        transform.setSchemaPaths(schemaPaths);

        // The next tokens are footer values
        setRecordFooterValues(transform, tokens, tokenIndex);

        return transform;
    }

    protected static short getKeyTypeForRecordType(final char recordType) {
        switch (recordType) {
            case MetadataConstants.RECORD_TYPE.UNIQUE_KEY: return MetadataConstants.KEY_TYPES.UNIQUE_KEY;
            case MetadataConstants.RECORD_TYPE.INDEX: return MetadataConstants.KEY_TYPES.INDEX;
            case MetadataConstants.RECORD_TYPE.ACCESS_PATTERN: return MetadataConstants.KEY_TYPES.ACCESS_PATTERN;
            case MetadataConstants.RECORD_TYPE.PRIMARY_KEY: return MetadataConstants.KEY_TYPES.PRIMARY_KEY;
            case MetadataConstants.RECORD_TYPE.FOREIGN_KEY: return MetadataConstants.KEY_TYPES.FOREIGN_KEY;
            case MetadataConstants.RECORD_TYPE.RESULT_SET : return -1;
            default:
                throw new IllegalArgumentException("Invalid record type, for key" + recordType); //$NON-NLS-1$
        }
    }

    /**
     * Create a TableRecord instance from the specified index record
     */
    public Table createTableRecord(final char[] record) {
        final List<String> tokens = getStrings(record, IndexConstants.RECORD_STRING.RECORD_DELIMITER);
        final Table table = new Table();

        // Extract the index version information from the record
        int indexVersion = getIndexVersion(record);

        // The tokens are the standard header values
        int tokenIndex = 0;
        setRecordHeaderValues(table, tokens.get(tokenIndex++), tokens.get(tokenIndex++),
                             tokens.get(tokenIndex++), tokens.get(tokenIndex++),
                             tokens.get(tokenIndex++), tokens.get(tokenIndex++));

        // The next token is the cardinality
        int cardinality = Integer.parseInt(tokens.get(tokenIndex++));
        if (indexVersion < NONZERO_UNKNOWN_CARDINALITY && cardinality == 0) {
            cardinality = -1;
        }
        table.setCardinality(cardinality);

        // The next token is the tableType
        table.setTableType(Table.Type.values()[Integer.parseInt(tokens.get(tokenIndex++))]);

        // The next token are the supports flags
        char[] supportFlags = (tokens.get(tokenIndex++)).toCharArray();
        table.setVirtual(getBooleanValue(supportFlags[0]));
        table.setSystem(getBooleanValue(supportFlags[1]));
        table.setSupportsUpdate(getBooleanValue(supportFlags[2]));
        if(includeMaterializationFlag(indexVersion)) {
            table.setMaterialized(getBooleanValue(supportFlags[3]));
        }

        // The next token are the UUIDs for the column references (no longer stored on the record)
        tokenIndex++;

        // The next token is the UUID of the primary key
        String id = getObjectValue(tokens.get(tokenIndex++));
        if (id != null) {
            KeyRecord pk = new KeyRecord(KeyRecord.Type.Primary);
            pk.setUUID(id);
            table.setPrimaryKey(pk);
        }

        List<String> indexes = getStrings(tokens.get(++tokenIndex), getListDelimiter(indexVersion));
        if (!indexes.isEmpty()) {
            table.setIndexes(new ArrayList<KeyRecord>(indexes.size()));
            for (String string : indexes) {
                KeyRecord index = new KeyRecord(Type.Index);
                index.setUUID(string);
                table.getIndexes().add(index);
            }
        }
        tokenIndex+=3; //skip reading uuids for associated records

        if(includeMaterializationFlag(indexVersion)) {
            // The next token are the UUIDs for the materialized table ID
            Table matTable = new Table();
            matTable.setUUID(tokens.get(tokenIndex++));
            table.setMaterializedTable(matTable);
            // The next token are the UUID for the materialized stage table ID
            matTable = new Table();
            matTable.setUUID(tokens.get(tokenIndex++));
            table.setMaterializedStageTable(matTable);
        }

        // The next tokens are footer values
        setRecordFooterValues(table, tokens, tokenIndex);

        return table;
    }

    private static List<Column> createColumns(List<String> uuids) {
        List<Column> columns = new ArrayList<Column>(uuids.size());
        for (String uuid : uuids) {
            Column column = new Column();
            column.setUUID(uuid);
            columns.add(column);
        }
        return columns;
    }

    /**
     * Create a ColumnRecord instance from the specified index record
     */
    public Column createColumnRecord(final char[] record) {
        final List<String> tokens = getStrings(record, IndexConstants.RECORD_STRING.RECORD_DELIMITER);
        final Column column = new Column();

        // Extract the index version information from the record
        int indexVersion = getIndexVersion(record);

        // The tokens are the standard header values
        int tokenIndex = 0;
        setRecordHeaderValues(column, tokens.get(tokenIndex++), tokens.get(tokenIndex++),
                             tokens.get(tokenIndex++), tokens.get(tokenIndex++),
                             tokens.get(tokenIndex++), tokens.get(tokenIndex++));

        // The next token are the supports flags
        char[] supportFlags = (tokens.get(tokenIndex++)).toCharArray();
        column.setSelectable(getBooleanValue(supportFlags[0]));
        column.setUpdatable(getBooleanValue(supportFlags[1]));
        column.setAutoIncremented(getBooleanValue(supportFlags[2]));
        column.setCaseSensitive(getBooleanValue(supportFlags[3]));
        column.setSigned(getBooleanValue(supportFlags[4]));
        column.setCurrency(getBooleanValue(supportFlags[5]));
        column.setFixedLength(getBooleanValue(supportFlags[6]));

        // The next token is the search type
        column.setNullType(NullType.values()[Integer.parseInt(tokens.get(tokenIndex++))]);

        // The next token is the search type
        column.setSearchType(SearchType.values()[3 - Integer.parseInt(tokens.get(tokenIndex++))]);

        // The next token is the length
        column.setLength( Integer.parseInt(tokens.get(tokenIndex++)) );

        // The next token is the scale
        column.setScale( Integer.parseInt(tokens.get(tokenIndex++)) );

        // The next token is the precision
        column.setPrecision( Integer.parseInt(tokens.get(tokenIndex++)) );

        // The next token is the precision
        column.setPosition( Integer.parseInt(tokens.get(tokenIndex++)) );

        // The next token is the charOctetLength
        column.setCharOctetLength( Integer.parseInt(tokens.get(tokenIndex++)) );

        // The next token is the radix
        column.setRadix( Integer.parseInt(tokens.get(tokenIndex++)) );

        if (includeColumnNullDistinctValues(indexVersion)) {
            // The next token is the distinct value
            column.setDistinctValues(Integer.parseInt(tokens.get(tokenIndex++)) );
            // The next token is the null value
            column.setNullValues(Integer.parseInt(tokens.get(tokenIndex++)) );
        }

        // The next token is the min value
        column.setMinimumValue( getObjectValue(tokens.get(tokenIndex++)) );

        // The next token is the max value
        column.setMaximumValue( getObjectValue(tokens.get(tokenIndex++)) );

        // The next token is the format value
        column.setFormat( getObjectValue(tokens.get(tokenIndex++)) );

        // The next token is the runtime type
        column.setRuntimeType( getObjectValue(tokens.get(tokenIndex++)) );

        if(includeColumnNativeType(indexVersion)) {
            // The next token is the native type
            column.setNativeType( getObjectValue(tokens.get(tokenIndex++)) );
        }

        // The next token is the datatype ObjectID
        column.setDatatypeUUID( getObjectValue(tokens.get(tokenIndex++)) );

        // The next token is the default value
        column.setDefaultValue( getObjectValue(tokens.get(tokenIndex++)) );

        // The next tokens are footer values
        setRecordFooterValues(column, tokens, tokenIndex);

        return column;
    }

    /**
     * Create a ColumnSetRecord instance from the specified index record
     */
    public ColumnSet createColumnSetRecord(final char[] record, ColumnSet columnSet) {
        final List<String> tokens = getStrings(record, IndexConstants.RECORD_STRING.RECORD_DELIMITER);

        // Extract the index version information from the record
        int indexVersion = getIndexVersion(record);

        // The tokens are the standard header values
        int tokenIndex = 0;
        setRecordHeaderValues(columnSet, tokens.get(tokenIndex++), tokens.get(tokenIndex++),
                             tokens.get(tokenIndex++), tokens.get(tokenIndex++),
                             tokens.get(tokenIndex++), tokens.get(tokenIndex++));

        // The next token are the UUIDs for the column references
        List<String> uuids = getStrings(tokens.get(tokenIndex++), getListDelimiter(indexVersion));
        columnSet.setColumns(createColumns(uuids));

        if (record[0] == MetadataConstants.RECORD_TYPE.UNIQUE_KEY || record[0] == MetadataConstants.RECORD_TYPE.PRIMARY_KEY) {
            //read the values from the index to update the tokenindex, but we don't actually use them.
            tokenIndex++;
        }
        // The next tokens are footer values
        setRecordFooterValues(columnSet, tokens, tokenIndex);

        return columnSet;
    }

    /**
     * Create a ForeignKeyRecord instance from the specified index record
     */
    public ForeignKey createForeignKeyRecord(final char[] record) {
        final List<String> tokens = getStrings(record, IndexConstants.RECORD_STRING.RECORD_DELIMITER);
        final ForeignKey fkRecord = new ForeignKey();

        // Extract the index version information from the record
        int indexVersion = getIndexVersion(record);

        // The tokens are the standard header values
        int tokenIndex = 0;
        setRecordHeaderValues(fkRecord, tokens.get(tokenIndex++), tokens.get(tokenIndex++),
                             tokens.get(tokenIndex++), tokens.get(tokenIndex++),
                             tokens.get(tokenIndex++), tokens.get(tokenIndex++));

        // The next token are the UUIDs for the column references
        List<String> uuids = getStrings(tokens.get(tokenIndex++), getListDelimiter(indexVersion));
        fkRecord.setColumns(createColumns(uuids));

        // The next token is the UUID of the unique key
        fkRecord.setUniqueKeyID(getObjectValue(tokens.get(tokenIndex++)));

        // The next tokens are footer values
        setRecordFooterValues(fkRecord, tokens, tokenIndex);

        return fkRecord;
    }

    /**
     * Create a DatatypeRecord instance from the specified index record
     */
    public Datatype createDatatypeRecord(final char[] record) {
        final List<String> tokens = getStrings(record, IndexConstants.RECORD_STRING.RECORD_DELIMITER);
        final Datatype dt = new Datatype();

        // Extract the index version information from the record
        int indexVersion = getIndexVersion(record);

        // The tokens are the standard header values
        int tokenIndex = 0;

        // Set the record type
        tokenIndex++;

        // Set the datatype and basetype identifiers
        tokenIndex++;
        String basetypeID = getObjectValue(tokens.get(tokenIndex++));
        if ( basetypeID != null ) {
            final int i = basetypeID.lastIndexOf(URI_REFERENCE_DELIMITER);
            if ( i != -1 && basetypeID.length() > (i+1)) {
                basetypeID = basetypeID.substring(i+1);
            }
        }
        dt.setBasetypeName(basetypeID);

        // Set the fullName/objectID/nameInSource
        String fullName = tokens.get(tokenIndex++);
        int indx = fullName.lastIndexOf(URI_REFERENCE_DELIMITER);
        if (indx > -1) {
            fullName = new String(fullName.substring(indx+1));
        } else {
            indx = fullName.lastIndexOf(AbstractMetadataRecord.NAME_DELIM_CHAR);
            if (indx > -1) {
                fullName = new String(fullName.substring(indx+1));
            }
        }
        dt.setName(fullName);
        dt.setUUID(getObjectValue(tokens.get(tokenIndex++)));
        dt.setNameInSource(getObjectValue(tokens.get(tokenIndex++)));

        // Set the variety type and its properties
        dt.setVarietyType(Variety.values()[Short.parseShort(tokens.get(tokenIndex++))]);
        tokenIndex++;

        // Set the runtime and java class names
        dt.setRuntimeTypeName(getObjectValue(tokens.get(tokenIndex++)));
        dt.setJavaClassName(getObjectValue(tokens.get(tokenIndex++)));

        // Set the datatype type
        dt.setType(Datatype.Type.values()[Short.parseShort(tokens.get(tokenIndex++))]);

        // Set the search type
        dt.setSearchType(SearchType.values()[3 - Integer.parseInt(tokens.get(tokenIndex++))]);

        // Set the null type
        dt.setNullType(NullType.values()[Integer.parseInt(tokens.get(tokenIndex++))]);

        // Set the boolean flags
        char[] booleanValues = (tokens.get(tokenIndex++)).toCharArray();
        dt.setSigned(getBooleanValue(booleanValues[0]));
        dt.setAutoIncrement(getBooleanValue(booleanValues[1]));
        dt.setCaseSensitive(getBooleanValue(booleanValues[2]));

        // Append the length
        dt.setLength( Integer.parseInt(tokens.get(tokenIndex++)) );

        // Append the precision length
        dt.setPrecision( Integer.parseInt(tokens.get(tokenIndex++)) );

        // Append the scale
        dt.setScale( Integer.parseInt(tokens.get(tokenIndex++)) );

        // Append the radix
        dt.setRadix( Integer.parseInt(tokens.get(tokenIndex++)) );

        // Set the primitive type identifier
        if (includePrimitiveTypeIdValue(indexVersion)) {
            // The next token is the primitive type identifier
            tokenIndex++;
        }

        // The next tokens are footer values
        setRecordFooterValues(dt, tokens, tokenIndex);

        return dt;
    }

    /**
     * Create a ProcedureRecord instance from the specified index record
     */
    public Procedure createProcedureRecord(final char[] record) {
        final List<String> tokens = getStrings(record, IndexConstants.RECORD_STRING.RECORD_DELIMITER);
        final Procedure procRd = new Procedure();

        // Extract the index version information from the record
        int indexVersion = getIndexVersion(record);

        // The tokens are the standard header values
        int tokenIndex = 0;

        // Set the record type
        setRecordHeaderValues(procRd, tokens.get(tokenIndex++), tokens.get(tokenIndex++),
                             tokens.get(tokenIndex++), tokens.get(tokenIndex++),
                             tokens.get(tokenIndex++), tokens.get(tokenIndex++));

        // Set the boolean flags
        char[] booleanValues = (tokens.get(tokenIndex++)).toCharArray();
        // flag indicating if the procedure is a function
        procRd.setFunction(getBooleanValue(booleanValues[0]));
        // flag indicating if the procedure is virtual
        procRd.setVirtual(getBooleanValue(booleanValues[1]));

        // The next token are the UUIDs for the param references
        List<String> uuids = getStrings(tokens.get(tokenIndex++), getListDelimiter(indexVersion));
        List<ProcedureParameter> columns = new ArrayList<ProcedureParameter>(uuids.size());
        for (String uuid : uuids) {
            ProcedureParameter column = new ProcedureParameter();
            column.setUUID(uuid);
            columns.add(column);
        }
        procRd.setParameters(columns);

        // The next token is the UUID of the resultSet object
        String rsId = getObjectValue(tokens.get(tokenIndex++));
        if (rsId != null) {
            ColumnSet cs = new ColumnSet();
            cs.setUUID(rsId);
            procRd.setResultSet(cs);
        }

        if (includeProcedureUpdateCount(indexVersion)) {
            procRd.setUpdateCount(Integer.parseInt(tokens.get(tokenIndex++)) - 1);
        }

        // The next tokens are footer values
        setRecordFooterValues(procRd, tokens, tokenIndex);

        return procRd;
    }

    /**
     * Create a ProcedureParameterRecord instance from the specified index record
     * header|defaultValue|dataType|length|radix|scale|nullType|precision|paramType|footer|
     */
    public ProcedureParameter createProcedureParameterRecord(final char[] record) {

        final String str = new String(record);
        final List<String> tokens = getStrings(str, IndexConstants.RECORD_STRING.RECORD_DELIMITER);
        final ProcedureParameter paramRd = new ProcedureParameter();

        // The tokens are the standard header values
        int tokenIndex = 0;

        // Set the record type
        setRecordHeaderValues(paramRd, tokens.get(tokenIndex++), tokens.get(tokenIndex++),
                             tokens.get(tokenIndex++), tokens.get(tokenIndex++),
                             tokens.get(tokenIndex++), tokens.get(tokenIndex++));

        // The next token is the default value of the parameter
        paramRd.setDefaultValue(getObjectValue(tokens.get(tokenIndex++)) );

        // The next token is the runtime type
        paramRd.setRuntimeType(getObjectValue(tokens.get(tokenIndex++)) );

        // The next token is the uuid
        paramRd.setDatatypeUUID(getObjectValue(tokens.get(tokenIndex++)) );

        // The next token is the length
        paramRd.setLength(Integer.parseInt(tokens.get(tokenIndex++)) );

        // The next token is the radix
        paramRd.setRadix(Integer.parseInt(tokens.get(tokenIndex++)) );

        // The next token is the scale
        paramRd.setScale(Integer.parseInt(tokens.get(tokenIndex++)) );

        // The next token is the null type
        paramRd.setNullType(NullType.values()[Integer.parseInt(tokens.get(tokenIndex++))]);

        // The next token is the precision
        paramRd.setPrecision(Integer.parseInt(tokens.get(tokenIndex++)) );

        // The next token is the position
        paramRd.setPosition(Integer.parseInt(tokens.get(tokenIndex++)) );

        // The next token is parameter type
        ProcedureParameter.Type type = null;
        switch (Short.parseShort(tokens.get(tokenIndex++))) {
        case MetadataConstants.PARAMETER_TYPES.IN_PARM:
            type = ProcedureParameter.Type.In;
            break;
        case MetadataConstants.PARAMETER_TYPES.INOUT_PARM:
            type = ProcedureParameter.Type.InOut;
            break;
        case MetadataConstants.PARAMETER_TYPES.OUT_PARM:
            type = ProcedureParameter.Type.Out;
            break;
        case MetadataConstants.PARAMETER_TYPES.RETURN_VALUE:
            type = ProcedureParameter.Type.ReturnValue;
            break;
        default:
            throw new IllegalArgumentException("Invalid parameter type, please ensure all parameter types are valid in Designer."); //$NON-NLS-1$
        }
        paramRd.setType(type);

        // The next token is flag for parameter optional prop
        char[] flags = (tokens.get(tokenIndex++)).toCharArray();
        paramRd.setOptional(getBooleanValue(flags[0]));

        // The next tokens are footer values
        setRecordFooterValues(paramRd, tokens, tokenIndex);

        return paramRd;
    }

    /**
     * Search for and return the version number associated with this record.
     * If no version information is found encoded in the record then the
     * version number of NONVERSIONED_RECORD_INDEX_VERSION will be returned.
     * @param record
     * @since 4.2
     */
    int getIndexVersion(final char[] record) {
        if (version == NONVERSIONED_RECORD_INDEX_VERSION) {
            int endIndex   = record.length;
            int beginIndex = (endIndex - 6 > 0 ? endIndex - 6 : 1);
            for (int i = beginIndex; i < endIndex; i++) {
                if (record[i] == IndexConstants.RECORD_STRING.INDEX_VERSION_MARKER) {
                    char versionPart1 = record[i+1];
                    char versionPart2 = record[i+2];
                    if (Character.isDigit(versionPart1) && Character.isDigit(versionPart2)){
                        version = Character.digit(versionPart1, 10) * 10 + Character.digit(versionPart2, 10);
                    }
                }
            }
        }
        return version;
    }

    public String getObjectValue(final String str) {
        if (str != null && str.length() == 1 && str.charAt(0) == IndexConstants.RECORD_STRING.SPACE) {
            return null;
        }
        return str;
    }

    public boolean getBooleanValue(final char b) {
        if (b == IndexConstants.RECORD_STRING.TRUE) {
            return true;
        }
        return false;
    }

    public static List<String> getStrings(final String record, final char listDelimiter) {
        return getStrings(record.toCharArray(), listDelimiter);
    }

    public static List<String> getStrings(final char[] record, final char listDelimiter) {
        if (record == null || record.length == 0) {
            return Collections.emptyList();
        }
        if (record.length == 1 && record[0] == IndexConstants.RECORD_STRING.SPACE) {
            return Collections.emptyList();
        }
        List<String> result = new ArrayList<String>();
        int start = 0;
        for (int i = 0; i < record.length; i++) {
            if (record[i] == listDelimiter) {
                if (i != start) {
                    result.add(new String(record, start, i - start));
                }
                start = i+1;
            }
        }
        if (start < record.length) {
            result.add(new String(record, start, record.length - start));
        }
        return result;
    }

    public char getListDelimiter(final int indexVersionNumber) {
        if (indexVersionNumber < DELIMITER_INDEX_VERSION) {
            return IndexConstants.RECORD_STRING.LIST_DELIMITER_OLD;
        }
        return IndexConstants.RECORD_STRING.LIST_DELIMITER;
    }

    public boolean includeMaterializationFlag(final int indexVersionNumber) {
        if (indexVersionNumber < TABLE_MATERIALIZATION_INDEX_VERSION) {
            return false;
        }
        return true;
    }

    public boolean includeMaterializedTables(final int indexVersionNumber) {
        if (indexVersionNumber < TABLE_MATERIALIZATION_INDEX_VERSION) {
            return false;
        }
        return true;
    }

    public boolean includeColumnNativeType(final int indexVersionNumber) {
        if (indexVersionNumber < COLUMN_NATIVE_TYPE_INDEX_VERSION) {
            return false;
        }
        return true;
    }

    public boolean includeColumnNullDistinctValues(final int indexVersionNumber) {
        if (indexVersionNumber < COLUMN_NULL_DISTINCT_INDEX_VERSION) {
            return false;
        }
        return true;
    }

    public boolean includePrimitiveTypeIdValue(final int indexVersionNumber) {
        if (indexVersionNumber < PRIMITIVE_TYPE_ID_INDEX_VERSION) {
            return false;
        }
        return true;
    }

    public boolean includeInputParameterFlag(final int indexVersionNumber) {
        if (indexVersionNumber < COLUMN_INPUT_PARAMETER_FLAG_INDEX_VERSION) {
            return false;
        }
        return true;
    }

    public boolean includeAnnotationProperties(final int indexVersionNumber) {
        if (indexVersionNumber < ANNOTATION_TAGS_INDEX_VERSION) {
            return true;
        }
        return false;
    }

    public boolean includeTransformationUUID(final int indexVersionNumber) {
        if (indexVersionNumber < TRANSFORMATION_UUID_INDEX_VERSION) {
            return false;
        }
        return true;
    }

    private static boolean includeProcedureUpdateCount(final int indexVersionNumber) {
        return (indexVersionNumber >= PROCEDURE_UPDATE_COUNT_VERSION);
    }

    // ==================================================================================
    //                         P R I V A T E   M E T H O D S
    // ==================================================================================

    /**
     * Set the "header" values on the specified MetadataRecord.
     * All index file record headers are of the form:
     * recordType|upperFullName|objectID|fullName|nameInSource|parentObjectID
     * The order of the fields in the index file header must also
     * be the order of the arguments in method signature.
     */
    private void setRecordHeaderValues(final AbstractMetadataRecord record, final String recordType,
                                              final String upperName, final String objectID, String fullName,
                                              final String nameInSource,
                                              final String parentObjectID) {

        record.setUUID(getObjectValue(objectID));
        String parentName = fullName;
        if (fullName != null) {
            String name = fullName;
            if (record instanceof ProcedureParameter || record instanceof KeyRecord) { //take only the last part
                name = getShortName(fullName);
            } else { //remove model name
                int index = fullName.indexOf(IndexConstants.NAME_DELIM_CHAR);
                if (index > 0) {
                    name = new String(fullName.substring(index + 1));
                    parentName = new String(fullName.substring(0, index));
                }
            }
            record.setName(name);
        }
        if (parentName != null) {
            if (record instanceof Table) {
                Schema s = new Schema();
                s.setName(parentName);
                ((Table)record).setParent(s);
            } else if (record instanceof Procedure) {
                Schema s = new Schema();
                s.setName(parentName);
                ((Procedure)record).setParent(s);
            }
        }
        parentId = getObjectValue(parentObjectID);
        record.setNameInSource(getObjectValue(nameInSource));
    }

    static String getShortName(String fullName) {
        int index = fullName.lastIndexOf(IndexConstants.NAME_DELIM_CHAR);
        if (index > 0) {
            fullName = new String(fullName.substring(index + 1));
        }
        return fullName;
    }

    /**
     * Set the "footer" values on the specified MetadataRecord.
     * All index file record footers are of the form:
     * modelPath|name|indexVersion
     * The order of the fields in the index file header must also
     * be the order of the arguments in method signature.
     */
    private void setRecordFooterValues(final AbstractMetadataRecord record, final List<String> tokens, int tokenIndex) {
        if (record instanceof TransformationRecordImpl) {
            ((TransformationRecordImpl)record).setResourcePath(getOptionalToken(tokens, tokenIndex));
        }
        tokenIndex++;
        if (record.getName() == null) {
            record.setName(getOptionalToken(tokens, tokenIndex++));
        }
        //placeholder for index version
        getOptionalToken(tokens, tokenIndex++);
    }

    public String getOptionalToken( final List<String> tokens, int tokenIndex) {
        if(tokens.size() > tokenIndex) {
            return tokens.get(tokenIndex);
        }
        return null;
    }

}