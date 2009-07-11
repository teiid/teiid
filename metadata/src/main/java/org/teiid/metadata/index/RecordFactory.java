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

package org.teiid.metadata.index;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.teiid.connector.metadata.FileRecordImpl;
import org.teiid.connector.metadata.runtime.AbstractMetadataRecord;
import org.teiid.connector.metadata.runtime.AnnotationRecordImpl;
import org.teiid.connector.metadata.runtime.ColumnRecordImpl;
import org.teiid.connector.metadata.runtime.ColumnSetRecordImpl;
import org.teiid.connector.metadata.runtime.DatatypeRecordImpl;
import org.teiid.connector.metadata.runtime.ForeignKeyRecordImpl;
import org.teiid.connector.metadata.runtime.MetadataConstants;
import org.teiid.connector.metadata.runtime.ModelRecordImpl;
import org.teiid.connector.metadata.runtime.ProcedureParameterRecordImpl;
import org.teiid.connector.metadata.runtime.ProcedureRecordImpl;
import org.teiid.connector.metadata.runtime.PropertyRecordImpl;
import org.teiid.connector.metadata.runtime.TableRecordImpl;
import org.teiid.connector.metadata.runtime.TransformationRecordImpl;
import org.teiid.core.index.IEntryResult;
import org.teiid.internal.core.index.EntryResult;
import org.teiid.internal.core.index.IIndexConstants;

import com.metamatrix.core.id.UUID;
import com.metamatrix.core.util.Assertion;
import com.metamatrix.core.util.StringUtil;

/**
 * RuntimeAdapter
 */
public class RecordFactory {
    
    public static final int INDEX_RECORD_BLOCK_SIZE = IIndexConstants.BLOCK_SIZE - 32;
    
    /**
     * The version number associated with any index records prior to the point
     * when version information was encoded in newly created records 
     */
    public static final int NONVERSIONED_RECORD_INDEX_VERSION = 0;
    
    /**
     * The version number that is associated with the change made to change the list
     * delimiter from {@link com.metamatrix.modeler.core.index.IndexConstants.LIST_DELIMITER_OLD}
     * to {@link com.metamatrix.modeler.core.index.IndexConstants.LIST_DELIMITER} and also the
     * property delimiter was changed from {@link com.metamatrix.modeler.core.index.IndexConstants.PROP_DELIMITER_OLD}
     * to {@link com.metamatrix.modeler.core.index.IndexConstants.PROP_DELIMITER}.  Added 07/22/2004.
     * @release 4.1.1
     */
    public static final int DELIMITER_INDEX_VERSION = 1;
    
    /**
     * The version number that is associated with the change made to add materialization
     * property on tables. Added 08/18/2004.
     * @release 4.2
     */
    public static final int TABLE_MATERIALIZATION_INDEX_VERSION = 2;

    /**
     * The version number that is associated with the change made to add native type
     * property on columns. Added 08/24/2004.
     * @release 4.2
     */
    public static final int COLUMN_NATIVE_TYPE_INDEX_VERSION = 3;

    /**
     * The version number that is associated with the change made to add an input parameter
     * flag on columns.  The flag is used to indicate if an element for a virtual table
     * represents an input parameter.  This change was made to support the Procedural-Relational
     * Mapping project.   Added 09/29/2004.
     * @release 4.2
     */
    public static final int COLUMN_INPUT_PARAMETER_FLAG_INDEX_VERSION = 4;

    /**
     * The version number that is associated with the change made to remove property value
     * pairs from the annotation records any properties on annotations would now be indexed
     * as part of the properties index. Added 12/14/2004.
     * @release 4.2
     */
    public static final int ANNOTATION_TAGS_INDEX_VERSION = 5;

    /**
     * The version number that is associated with the change made to add uuid for the
     * transformation mapping root on the transformation records, uuids would now be indexed
     * as part of the transformation index. Added 1/13/2005.
     * @release 4.2
     */
    public static final int TRANSFORMATION_UUID_INDEX_VERSION = 6;
    
    /**
     * The version number that is associated with the change made to add count of null and
     * distinct values for columns on the column records 7/8/2005.
     * @release 4.2
     */
    public static final int COLUMN_NULL_DISTINCT_INDEX_VERSION = 7;
    
    /**
     * The version number that is associated with the change made to add 
     * primitive type ID on datatype records 02/28/2006.
     * @release 5.0
     */
    public static final int PRIMITIVE_TYPE_ID_INDEX_VERSION = 8;
    
    /**
     * The version number that is associated with the change made to add 
     * an update count to physical stored and XQuery procedures 04/29/2008.
     * @release 5.0
     */
    public static final int PROCEDURE_UPDATE_COUNT_VERSION = 9;

    /**
     * The version number that is encoded with all newly created index records
     */
    public static final int CURRENT_INDEX_VERSION = PROCEDURE_UPDATE_COUNT_VERSION;

    // ==================================================================================
    //                      P U B L I C   M E T H O D S
    // ==================================================================================
    
    /**
     * Return a collection of {@link com.metamatrix.modeler.core.metadata.runtime.MetadataRecord}
     * instances for the result obtained from executing <code>queryEntriesMatching</code>
     * method on the {@link com.metamatrix.internal.core.index.impl.Index}
     * @param queryResult
     * @param container Container reference to be set on the record
     */
    public static Collection getMetadataRecord(final IEntryResult[] queryResult) {
        final Collection records = new ArrayList(queryResult.length);
        for (int i = 0; i < queryResult.length; i++) {
            final AbstractMetadataRecord record = getMetadataRecord(queryResult[i]);
            if (record != null) {
                records.add(record);
            }
        }
        return records;
    }

    /**
     * Return the {@link com.metamatrix.modeler.core.metadata.runtime.MetadataRecord}
     * instances for specified IEntryResult.
     * @param entryResult
     */
    private static AbstractMetadataRecord getMetadataRecord(final char[] record) {
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
            case MetadataConstants.RECORD_TYPE.ACCESS_PATTERN:
            case MetadataConstants.RECORD_TYPE.INDEX:
            case MetadataConstants.RECORD_TYPE.RESULT_SET: 
            case MetadataConstants.RECORD_TYPE.UNIQUE_KEY:
            case MetadataConstants.RECORD_TYPE.PRIMARY_KEY: return createColumnSetRecord(record);
            case MetadataConstants.RECORD_TYPE.FOREIGN_KEY: return createForeignKeyRecord(record);
            case MetadataConstants.RECORD_TYPE.DATATYPE: return createDatatypeRecord(record);
            case MetadataConstants.RECORD_TYPE.SELECT_TRANSFORM:
            case MetadataConstants.RECORD_TYPE.INSERT_TRANSFORM:
            case MetadataConstants.RECORD_TYPE.UPDATE_TRANSFORM:
            case MetadataConstants.RECORD_TYPE.DELETE_TRANSFORM:
            case MetadataConstants.RECORD_TYPE.MAPPING_TRANSFORM:
            case MetadataConstants.RECORD_TYPE.PROC_TRANSFORM: return createTransformationRecord(record);
            case MetadataConstants.RECORD_TYPE.ANNOTATION: return createAnnotationRecord(record);
            case MetadataConstants.RECORD_TYPE.PROPERTY: return createPropertyRecord(record);
            case MetadataConstants.RECORD_TYPE.FILE: return createFileRecord(record);
            default:
                throw new IllegalArgumentException("Invalid record type for creating MetadataRecord "+record[0]); //$NON-NLS-1$
        }
    }
    
    /**
     * Return the {@link com.metamatrix.modeler.core.metadata.runtime.MetadataRecord}
     * instances for specified IEntryResult.
     * @param entryResult
     * @param container Container reference to be set on the record
     */
    public static AbstractMetadataRecord getMetadataRecord(final IEntryResult queryResult) {
        AbstractMetadataRecord record = getMetadataRecord(queryResult.getWord());
        return record;
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
        Assertion.isNotNull(result);

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
        Assertion.isNotNull(result);
        
        char[] word = result.getWord();
        String baseStr = new String(word);
        int beginIndex = baseStr.indexOf(UUID.PROTOCOL);
        int endIndex   = word.length;
        Assertion.assertTrue(beginIndex != -1);
        for (int i = beginIndex; i < word.length; i++) {
            if (word[i] == IndexConstants.RECORD_STRING.RECORD_DELIMITER) {
                endIndex = i;
                break;
            }
        }
        Assertion.assertTrue(beginIndex < endIndex);
        return baseStr.substring(beginIndex,endIndex);
    }

    // ==================================================================================
    //                    P R O T E C T E D   M E T H O D S
    // ==================================================================================

    /**
     * Create a ModelRecord instance from the specified index record
     */
    public static ModelRecordImpl createModelRecord(final char[] record) {
        final String str = new String(record);
        final List tokens = StringUtil.split(str,String.valueOf(IndexConstants.RECORD_STRING.RECORD_DELIMITER));
        final ModelRecordImpl model = new ModelRecordImpl();

        // Extract the index version information from the record 
        int indexVersion = getIndexVersion(record);

        // The tokens are the standard header values
        int tokenIndex = 0;
        setRecordHeaderValues(model, (String)tokens.get(tokenIndex++), (String)tokens.get(tokenIndex++), (String)tokens.get(tokenIndex++),
                             (String)tokens.get(tokenIndex++), (String)tokens.get(tokenIndex++),
                             (String)tokens.get(tokenIndex++));
        
        // The next token is the max set size
        model.setMaxSetSize( Integer.parseInt((String)tokens.get(tokenIndex++)) );
        
        // The next token is the model type
        model.setModelType( Integer.parseInt((String)tokens.get(tokenIndex++)) );
        
        // The next token is the primary metamodel Uri
        model.setPrimaryMetamodelUri(getObjectValue((String)tokens.get(tokenIndex++)));

        // The next token are the supports flags
        char[] supportFlags = ((String)tokens.get(tokenIndex++)).toCharArray();
        model.setVisible(getBooleanValue(supportFlags[0]));
        model.setSupportsDistinct(getBooleanValue(supportFlags[1]));
        model.setSupportsJoin(getBooleanValue(supportFlags[2]));
        model.setSupportsOrderBy(getBooleanValue(supportFlags[3]));
        model.setSupportsOuterJoin(getBooleanValue(supportFlags[4]));
        model.setSupportsWhereAll(getBooleanValue(supportFlags[5]));

		// The next tokens are footer values - the footer will contain the version number for the index record
		setRecordFooterValues(model, tokens, tokenIndex);

        return model;
    }

    /**
     * Create a TransformationRecord instance from the specified index record
     */
    public static TransformationRecordImpl createTransformationRecord(final char[] record) {
        final String str = new String(record);
        final List tokens = StringUtil.split(str,String.valueOf(IndexConstants.RECORD_STRING.RECORD_DELIMITER));
        final TransformationRecordImpl transform = new TransformationRecordImpl();

        // Extract the index version information from the record 
        int indexVersion = getIndexVersion(record);

        // The tokens are the standard header values
        int tokenIndex = 0;
        
        char recordType = ((String)tokens.get(tokenIndex++)).charAt(0);
        
        // The next token is the transformation type        
        transform.setTransformationType(getObjectValue(getTransformTypeForRecordType(recordType)));
        // The next token is the name of the transformed object
        transform.setFullName(getObjectValue(((String)tokens.get(tokenIndex++))));

        // The next token is the UUID of the transformed object
        getObjectValue((String)tokens.get(tokenIndex++));

        // The next token is the UUID of the transformation object
        if(includeTransformationUUID(indexVersion)) {
            transform.setUUID(getObjectValue(((String)tokens.get(tokenIndex++))));
        }        

        // The next token is the transformation definition
        transform.setTransformation(getObjectValue((String)tokens.get(tokenIndex++)));

        // The next token are the list of bindings
        List bindings = getStrings((String)tokens.get(tokenIndex++), indexVersion);
        transform.setBindings(bindings);

        // The next token are the list of schemaPaths
        List schemaPaths = getStrings((String)tokens.get(tokenIndex++), indexVersion);
        transform.setSchemaPaths(schemaPaths);

		// The next tokens are footer values
		setRecordFooterValues(transform, tokens, tokenIndex);

        return transform;
    }
    
    protected static String getTransformTypeForRecordType(final char recordType) {
        switch (recordType) {
            case MetadataConstants.RECORD_TYPE.SELECT_TRANSFORM: return TransformationRecordImpl.Types.SELECT;
            case MetadataConstants.RECORD_TYPE.INSERT_TRANSFORM: return TransformationRecordImpl.Types.INSERT;
            case MetadataConstants.RECORD_TYPE.UPDATE_TRANSFORM: return TransformationRecordImpl.Types.UPDATE;
            case MetadataConstants.RECORD_TYPE.DELETE_TRANSFORM: return TransformationRecordImpl.Types.DELETE;
            case MetadataConstants.RECORD_TYPE.PROC_TRANSFORM: return TransformationRecordImpl.Types.PROCEDURE;
            case MetadataConstants.RECORD_TYPE.MAPPING_TRANSFORM: return TransformationRecordImpl.Types.MAPPING;
            default:
                throw new IllegalArgumentException("Invalid record type, for key " + recordType); //$NON-NLS-1$
        }
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
    public static TableRecordImpl createTableRecord(final char[] record) {
        final String str = new String(record);
        final List tokens = StringUtil.split(str,String.valueOf(IndexConstants.RECORD_STRING.RECORD_DELIMITER));
        final TableRecordImpl table = new TableRecordImpl();

        // Extract the index version information from the record 
        int indexVersion = getIndexVersion(record);

        // The tokens are the standard header values
        int tokenIndex = 0;
        setRecordHeaderValues(table, (String)tokens.get(tokenIndex++), (String)tokens.get(tokenIndex++),
                             (String)tokens.get(tokenIndex++), (String)tokens.get(tokenIndex++),
                             (String)tokens.get(tokenIndex++), (String)tokens.get(tokenIndex++));

        // The next token is the cardinality
        table.setCardinality( Integer.parseInt((String)tokens.get(tokenIndex++)) );

        // The next token is the tableType
        table.setTableType( Integer.parseInt((String)tokens.get(tokenIndex++)) );

        // The next token are the supports flags
        char[] supportFlags = ((String)tokens.get(tokenIndex++)).toCharArray();
        table.setVirtual(getBooleanValue(supportFlags[0]));
        table.setSystem(getBooleanValue(supportFlags[1]));
        table.setSupportsUpdate(getBooleanValue(supportFlags[2]));
        if(includeMaterializationFlag(indexVersion)) {
            table.setMaterialized(getBooleanValue(supportFlags[3]));
        }

        // The next token are the UUIDs for the column references
        List uuids = getIDs((String)tokens.get(tokenIndex++), indexVersion);
        table.setColumnIDs(uuids);

        // The next token is the UUID of the primary key
        table.setPrimaryKeyID(getObjectValue((String)tokens.get(tokenIndex++)));

        tokenIndex+=4; //skip reading uuids for associated records

        if(includeMaterializationFlag(indexVersion)) {
            // The next token are the UUIDs for the materialized table ID
            table.setMaterializedTableID((String)tokens.get(tokenIndex++));
            // The next token are the UUID for the materialized stage table ID
            table.setMaterializedStageTableID((String)tokens.get(tokenIndex++));
        }

		// The next tokens are footer values
		setRecordFooterValues(table, tokens, tokenIndex);       

        return table;
    }

    /**
     * Create a ColumnRecord instance from the specified index record
     */
    public static ColumnRecordImpl createColumnRecord(final char[] record) {
        final String str = new String(record);
        final List tokens = StringUtil.split(str,String.valueOf(IndexConstants.RECORD_STRING.RECORD_DELIMITER));
        final ColumnRecordImpl column = new ColumnRecordImpl();

        // Extract the index version information from the record 
        int indexVersion = getIndexVersion(record);

        // The tokens are the standard header values
        int tokenIndex = 0;
        setRecordHeaderValues(column, (String)tokens.get(tokenIndex++), (String)tokens.get(tokenIndex++),
                             (String)tokens.get(tokenIndex++), (String)tokens.get(tokenIndex++),
                             (String)tokens.get(tokenIndex++), (String)tokens.get(tokenIndex++));

        // The next token are the supports flags
        char[] supportFlags = ((String)tokens.get(tokenIndex++)).toCharArray();
        column.setSelectable(getBooleanValue(supportFlags[0]));
        column.setUpdatable(getBooleanValue(supportFlags[1]));
        column.setAutoIncrementable(getBooleanValue(supportFlags[2]));
        column.setCaseSensitive(getBooleanValue(supportFlags[3]));
        column.setSigned(getBooleanValue(supportFlags[4]));
        column.setCurrency(getBooleanValue(supportFlags[5]));
        column.setFixedLength(getBooleanValue(supportFlags[6]));
        if (includeInputParameterFlag(indexVersion)) {
            column.setTransformationInputParameter(getBooleanValue(supportFlags[7]));
        }

        // The next token is the search type
        column.setNullType( Integer.parseInt((String)tokens.get(tokenIndex++)) );

        // The next token is the search type
        column.setSearchType( Integer.parseInt((String)tokens.get(tokenIndex++)) );

        // The next token is the length
        column.setLength( Integer.parseInt((String)tokens.get(tokenIndex++)) );

        // The next token is the scale
        column.setScale( Integer.parseInt((String)tokens.get(tokenIndex++)) );

        // The next token is the precision
        column.setPrecision( Integer.parseInt((String)tokens.get(tokenIndex++)) );

        // The next token is the precision
        column.setPosition( Integer.parseInt((String)tokens.get(tokenIndex++)) );

        // The next token is the charOctetLength
        column.setCharOctetLength( Integer.parseInt((String)tokens.get(tokenIndex++)) );

        // The next token is the radix
        column.setRadix( Integer.parseInt((String)tokens.get(tokenIndex++)) );

        if (includeColumnNullDistinctValues(indexVersion)) {
            // The next token is the distinct value
            column.setDistinctValues(Integer.parseInt((String)tokens.get(tokenIndex++)) );
            // The next token is the null value
            column.setNullValues(Integer.parseInt((String)tokens.get(tokenIndex++)) );            
        }

        // The next token is the min value
        column.setMinValue( getObjectValue((String)tokens.get(tokenIndex++)) );

        // The next token is the max value
        column.setMaxValue( getObjectValue((String)tokens.get(tokenIndex++)) );

        // The next token is the format value
        column.setFormat( getObjectValue((String)tokens.get(tokenIndex++)) );

        // The next token is the runtime type
        column.setRuntimeType( getObjectValue((String)tokens.get(tokenIndex++)) );

        if(includeColumnNativeType(indexVersion)) {
	        // The next token is the native type
	        column.setNativeType( getObjectValue((String)tokens.get(tokenIndex++)) );
        }

        // The next token is the datatype ObjectID
        column.setDatatypeUUID( getObjectValue((String)tokens.get(tokenIndex++)) );

        // The next token is the default value
        column.setDefaultValue( getObjectValue((String)tokens.get(tokenIndex++)) );

		// The next tokens are footer values
		setRecordFooterValues(column, tokens, tokenIndex);

        return column;
    }

    /**
     * Create a ColumnSetRecord instance from the specified index record
     */
    public static ColumnSetRecordImpl createColumnSetRecord(final char[] record) {
        final String str = new String(record);
        final List tokens = StringUtil.split(str,String.valueOf(IndexConstants.RECORD_STRING.RECORD_DELIMITER));
        final ColumnSetRecordImpl columnSet = new ColumnSetRecordImpl(getKeyTypeForRecordType(record[0]));

        // Extract the index version information from the record 
        int indexVersion = getIndexVersion(record);

        // The tokens are the standard header values
        int tokenIndex = 0;
        setRecordHeaderValues(columnSet, (String)tokens.get(tokenIndex++), (String)tokens.get(tokenIndex++),
                             (String)tokens.get(tokenIndex++), (String)tokens.get(tokenIndex++),
                             (String)tokens.get(tokenIndex++), (String)tokens.get(tokenIndex++));

        // The next token are the UUIDs for the column references
        List uuids = getIDs((String)tokens.get(tokenIndex++), indexVersion);
        columnSet.setColumnIDs(uuids);

        if (record[0] == MetadataConstants.RECORD_TYPE.UNIQUE_KEY || record[0] == MetadataConstants.RECORD_TYPE.PRIMARY_KEY) {
        	//read the values from the index to update the tokenindex, but we don't actually use them.
        	getIDs((String)tokens.get(tokenIndex++), indexVersion);
        }
		// The next tokens are footer values
		setRecordFooterValues(columnSet, tokens, tokenIndex);

        return columnSet;
    }

    /**
     * Create a ForeignKeyRecord instance from the specified index record
     */
    public static ForeignKeyRecordImpl createForeignKeyRecord(final char[] record) {
        final String str = new String(record);
        final List tokens = StringUtil.split(str,String.valueOf(IndexConstants.RECORD_STRING.RECORD_DELIMITER));
        final ForeignKeyRecordImpl fkRecord = new ForeignKeyRecordImpl();

        // Extract the index version information from the record 
        int indexVersion = getIndexVersion(record);

        // The tokens are the standard header values
        int tokenIndex = 0;
        setRecordHeaderValues(fkRecord, (String)tokens.get(tokenIndex++), (String)tokens.get(tokenIndex++),
                             (String)tokens.get(tokenIndex++), (String)tokens.get(tokenIndex++),
                             (String)tokens.get(tokenIndex++), (String)tokens.get(tokenIndex++));
        
        // The next token are the UUIDs for the column references
        List uuids = getIDs((String)tokens.get(tokenIndex++), indexVersion);
        fkRecord.setColumnIDs(uuids);

        // The next token is the UUID of the unique key
        fkRecord.setUniqueKeyID(getObjectValue((String)tokens.get(tokenIndex++)));        

		// The next tokens are footer values
		setRecordFooterValues(fkRecord, tokens, tokenIndex);
        
        return fkRecord;
    }

    /**
     * Create a DatatypeRecord instance from the specified index record
     */
    public static DatatypeRecordImpl createDatatypeRecord(final char[] record) {
        final String str = new String(record);
        final List tokens = StringUtil.split(str,String.valueOf(IndexConstants.RECORD_STRING.RECORD_DELIMITER));
        final DatatypeRecordImpl dt = new DatatypeRecordImpl();

        // Extract the index version information from the record 
        int indexVersion = getIndexVersion(record);
        
        // The tokens are the standard header values
        int tokenIndex = 0;

        // Set the record type
        dt.setRecordType(((String)tokens.get(tokenIndex++)).toCharArray()[0]);

        // Set the datatype and basetype identifiers
        dt.setDatatypeID(getObjectValue((String)tokens.get(tokenIndex++)));
        dt.setBasetypeID(getObjectValue((String)tokens.get(tokenIndex++)));

        // Set the fullName/objectID/nameInSource
        dt.setFullName((String)tokens.get(tokenIndex++));
        dt.setUUID(getObjectValue((String)tokens.get(tokenIndex++)));
        dt.setNameInSource(getObjectValue((String)tokens.get(tokenIndex++)));
        
        // Set the variety type and its properties
        dt.setVarietyType( Short.parseShort((String)tokens.get(tokenIndex++)) );
        List props = getIDs((String)tokens.get(tokenIndex++), indexVersion);
        dt.setVarietyProps(props);
        
        // Set the runtime and java class names
        dt.setRuntimeTypeName(getObjectValue((String)tokens.get(tokenIndex++)));
        dt.setJavaClassName(getObjectValue((String)tokens.get(tokenIndex++)));
        
        // Set the datatype type
        dt.setType( Short.parseShort((String)tokens.get(tokenIndex++)) );
        
        // Set the search type
        dt.setSearchType( Short.parseShort((String)tokens.get(tokenIndex++)) );
        
        // Set the null type
        dt.setNullType( Short.parseShort((String)tokens.get(tokenIndex++)) );
 
        // Set the boolean flags
        char[] booleanValues = ((String)tokens.get(tokenIndex++)).toCharArray();
        dt.setSigned(getBooleanValue(booleanValues[0]));
        dt.setAutoIncrement(getBooleanValue(booleanValues[1]));
        dt.setCaseSensitive(getBooleanValue(booleanValues[2]));
        
        // Append the length
        dt.setLength( Integer.parseInt((String)tokens.get(tokenIndex++)) );
        
        // Append the precision length
        dt.setPrecisionLength( Integer.parseInt((String)tokens.get(tokenIndex++)) );
        
        // Append the scale
        dt.setScale( Integer.parseInt((String)tokens.get(tokenIndex++)) );
        
        // Append the radix
        dt.setRadix( Integer.parseInt((String)tokens.get(tokenIndex++)) );

        // Set the primitive type identifier
        if (includePrimitiveTypeIdValue(indexVersion)) {
            // The next token is the primitive type identifier
            dt.setPrimitiveTypeID(getObjectValue((String)tokens.get(tokenIndex++)));
        }

		// The next tokens are footer values
		setRecordFooterValues(dt, tokens, tokenIndex);       
        
        return dt;
    }

    /**
     * Create a ProcedureRecord instance from the specified index record
     */
    public static ProcedureRecordImpl createProcedureRecord(final char[] record) {

        final String str = new String(record);
        final List tokens = StringUtil.split(str,String.valueOf(IndexConstants.RECORD_STRING.RECORD_DELIMITER));
        final ProcedureRecordImpl procRd = new ProcedureRecordImpl();

        // Extract the index version information from the record 
        int indexVersion = getIndexVersion(record);

        // The tokens are the standard header values
        int tokenIndex = 0;

        // Set the record type
        setRecordHeaderValues(procRd, (String)tokens.get(tokenIndex++), (String)tokens.get(tokenIndex++),
                             (String)tokens.get(tokenIndex++), (String)tokens.get(tokenIndex++),
                             (String)tokens.get(tokenIndex++), (String)tokens.get(tokenIndex++));

        // Set the boolean flags
        char[] booleanValues = ((String)tokens.get(tokenIndex++)).toCharArray();
        // flag indicating if the procedure is a function
        procRd.setFunction(getBooleanValue(booleanValues[0]));
        // flag indicating if the procedure is virtual
        procRd.setVirtual(getBooleanValue(booleanValues[1]));

        // The next token are the UUIDs for the param references
        List uuids = getIDs((String)tokens.get(tokenIndex++), indexVersion);
        procRd.setParameterIDs(uuids);

        // The next token is the UUID of the resultSet object
        procRd.setResultSetID(getObjectValue((String)tokens.get(tokenIndex++)));
        
        if (includeProcedureUpdateCount(indexVersion)) {
            procRd.setUpdateCount(Integer.parseInt((String)tokens.get(tokenIndex++)));
        }
        
		// The next tokens are footer values
		setRecordFooterValues(procRd, tokens, tokenIndex);

        return procRd;
    }

    /**
     * Create a ProcedureParameterRecord instance from the specified index record
     * header|defaultValue|dataType|length|radix|scale|nullType|precision|paramType|footer|
     */
    public static ProcedureParameterRecordImpl createProcedureParameterRecord(final char[] record) {

        final String str = new String(record);
        final List tokens = StringUtil.split(str,String.valueOf(IndexConstants.RECORD_STRING.RECORD_DELIMITER));
        final ProcedureParameterRecordImpl paramRd = new ProcedureParameterRecordImpl();

        // Extract the index version information from the record 
        int indexVersion = getIndexVersion(record);

        // The tokens are the standard header values
        int tokenIndex = 0;

        // Set the record type
        setRecordHeaderValues(paramRd, (String)tokens.get(tokenIndex++), (String)tokens.get(tokenIndex++),
                             (String)tokens.get(tokenIndex++), (String)tokens.get(tokenIndex++),
                             (String)tokens.get(tokenIndex++), (String)tokens.get(tokenIndex++));

        // The next token is the default value of the parameter
        paramRd.setDefaultValue(getObjectValue((String)tokens.get(tokenIndex++)) );

        // The next token is the runtime type
        paramRd.setRuntimeType(getObjectValue((String)tokens.get(tokenIndex++)) );

        // The next token is the uuid
        paramRd.setDatatypeUUID(getObjectValue((String)tokens.get(tokenIndex++)) );

        // The next token is the length
        paramRd.setLength(Integer.parseInt((String)tokens.get(tokenIndex++)) );

        // The next token is the radix
        paramRd.setRadix(Integer.parseInt((String)tokens.get(tokenIndex++)) );

        // The next token is the scale
        paramRd.setScale(Integer.parseInt((String)tokens.get(tokenIndex++)) );

        // The next token is the null type
        paramRd.setNullType(Integer.parseInt((String)tokens.get(tokenIndex++)) );

        // The next token is the precision
        paramRd.setPrecision(Integer.parseInt((String)tokens.get(tokenIndex++)) );

        // The next token is the position
        paramRd.setPosition(Integer.parseInt((String)tokens.get(tokenIndex++)) );

        // The next token is parameter type        
        paramRd.setType(Short.parseShort((String)tokens.get(tokenIndex++)));

        // The next token is flag for parameter optional prop
        char[] flags = ((String)tokens.get(tokenIndex++)).toCharArray();
        paramRd.setOptional(getBooleanValue(flags[0]));

		// The next tokens are footer values
		setRecordFooterValues(paramRd, tokens, tokenIndex);

        return paramRd;
    }

    /**
     * Create a AnnotationRecord instance from the specified index record
     */
    public static AnnotationRecordImpl createAnnotationRecord(final char[] record) {
        final String str = new String(record);
        final List tokens = StringUtil.split(str,String.valueOf(IndexConstants.RECORD_STRING.RECORD_DELIMITER));
        final AnnotationRecordImpl annotation = new AnnotationRecordImpl();

        // Extract the index version information from the record 
        int indexVersion = getIndexVersion(record);

        // The tokens are the standard header values
        int tokenIndex = 0;
        setRecordHeaderValues(annotation, (String)tokens.get(tokenIndex++), (String)tokens.get(tokenIndex++),
                             (String)tokens.get(tokenIndex++), (String)tokens.get(tokenIndex++),
                             (String)tokens.get(tokenIndex++), (String)tokens.get(tokenIndex++));

        if(includeAnnotationProperties(indexVersion)) {
			// The next token are the properties, ignore it not going to be read any way
            tokenIndex++;
        }

        // The next token is the description
        annotation.setDescription((String)tokens.get(tokenIndex++));

        // The next tokens are footer values
		setRecordFooterValues(annotation, tokens, tokenIndex);        

        return annotation;
    }

    /**
     * Create a PropertyRecord instance from the specified index record
     */
    public static PropertyRecordImpl createPropertyRecord(final char[] record) {
        final String str = new String(record);
        final List tokens = StringUtil.split(str,String.valueOf(IndexConstants.RECORD_STRING.RECORD_DELIMITER));
        final PropertyRecordImpl property = new PropertyRecordImpl();

        int indexVersion = getIndexVersion(record);

        // The tokens are the standard header values
        int tokenIndex = 0;

        // The next token is the record type
        String recordType = (String)tokens.get(tokenIndex++);
        property.setRecordType(recordType.toCharArray()[0]);

        // The next token is the object ID
        String objectID = (String)tokens.get(tokenIndex++);
        property.setUUID(getObjectValue(objectID));

        // The next token is the property name
        property.setPropertyName( (String)tokens.get(tokenIndex++) );

        // The next token is the property value
        property.setPropertyValue((String)tokens.get(tokenIndex++));
        
        // for newer records
        if(!includeAnnotationProperties(indexVersion)) {
	        // The next token is extension boolean
	        tokenIndex++;
        }

		// The next tokens are footer values
		setRecordFooterValues(property, tokens, tokenIndex);

        return property;
    }
    
    /**
     * Create a FileRecord instance from the specified index record
     */
    public static FileRecordImpl createFileRecord(final char[] record) {
        final String str = new String(record);
        final List tokens = StringUtil.split(str,String.valueOf(IndexConstants.RECORD_STRING.RECORD_DELIMITER));
        final FileRecordImpl file = new FileRecordImpl();

        // The tokens are the standard header values
        int tokenIndex = 0;

        // The next token is the record type
        String recordType = (String)tokens.get(tokenIndex++);
        file.setRecordType(recordType.toCharArray()[0]);

        // The next token is the relative path to the file in vdb
        file.setPathInVdb((String)tokens.get(tokenIndex++) );

        return file;
    }

    /**
     * Search for and return the version number associated with this record.
     * If no version information is found encoded in the record then the
     * version number of NONVERSIONED_RECORD_INDEX_VERSION will be returned.
     * @param record
     * @since 4.2
     */
    private static int getIndexVersion(final char[] record) {
        Assertion.isNotNull(record);
        int endIndex   = record.length;
        int beginIndex = (endIndex - 6 > 0 ? endIndex - 6 : 1);
        int version    = NONVERSIONED_RECORD_INDEX_VERSION;
        for (int i = beginIndex; i < endIndex; i++) {
            if (record[i] == IndexConstants.RECORD_STRING.INDEX_VERSION_MARKER) {
                char versionPart1 = record[i+1];
                char versionPart2 = record[i+2];
                if (Character.isDigit(versionPart1) && Character.isDigit(versionPart2)){
                    version = Character.digit(versionPart1, 10) * 10 + Character.digit(versionPart2, 10);
                }
            }
        }
        return version;
    }

    public static String getObjectValue(final String str) {
        if (str != null && str.length() == 1 && str.charAt(0) == IndexConstants.RECORD_STRING.SPACE) {
            return null;
        } 
        return str;
    }

    public static boolean getBooleanValue(final char b) {
        if (b == IndexConstants.RECORD_STRING.TRUE) {
            return true;
        } 
        return false;
    }

    public static List getIDs(final String values, final int indexVersionNumber) {
        if (StringUtil.isEmpty(values)) {
            return Collections.EMPTY_LIST;
        }
        if (values.length() == 1 && values.charAt(0) == IndexConstants.RECORD_STRING.SPACE) {
            return Collections.EMPTY_LIST;
        } 
        final char listDelimiter = getListDelimiter(indexVersionNumber);
        final List tokens = StringUtil.split(values,String.valueOf(listDelimiter));
        final List result = new ArrayList(tokens.size());
        for (Iterator iter = tokens.iterator(); iter.hasNext();) {
            String token = getObjectValue((String)iter.next());
            if (token != null) {
                result.add(token);
            }
        }
        return result;
    }

    public static List getStrings(final String values, final int indexVersionNumber) {
        if (StringUtil.isEmpty(values)) {
            return Collections.EMPTY_LIST;
        }
        if (values.length() == 1 && values.charAt(0) == IndexConstants.RECORD_STRING.SPACE) {
            return Collections.EMPTY_LIST;
        } 
        final char listDelimiter = getListDelimiter(indexVersionNumber);
        final List tokens = StringUtil.split(values,String.valueOf(listDelimiter));
        final List result = new ArrayList(tokens.size());
        for (Iterator iter = tokens.iterator(); iter.hasNext();) {
            String token = (String)iter.next();
            if (token != null) {
                result.add(token);
            }
        }
        return result;
    }

	public static char getListDelimiter(final int indexVersionNumber) {
        if (indexVersionNumber < DELIMITER_INDEX_VERSION) {
            return IndexConstants.RECORD_STRING.LIST_DELIMITER_OLD;
        }
        return IndexConstants.RECORD_STRING.LIST_DELIMITER;
    }

	public static boolean includeMaterializationFlag(final int indexVersionNumber) {
        if (indexVersionNumber < TABLE_MATERIALIZATION_INDEX_VERSION) {
            return false;
        }
        return true;
    }

	public static boolean includeMaterializedTables(final int indexVersionNumber) {
        if (indexVersionNumber < TABLE_MATERIALIZATION_INDEX_VERSION) {
            return false;
        }
        return true;
    }

	public static boolean includeColumnNativeType(final int indexVersionNumber) {
        if (indexVersionNumber < COLUMN_NATIVE_TYPE_INDEX_VERSION) {
            return false;
        }
        return true;
    }    

	public static boolean includeColumnNullDistinctValues(final int indexVersionNumber) {
        if (indexVersionNumber < COLUMN_NULL_DISTINCT_INDEX_VERSION) {
            return false;
        }
        return true;
    } 

	public static boolean includePrimitiveTypeIdValue(final int indexVersionNumber) {
        if (indexVersionNumber < PRIMITIVE_TYPE_ID_INDEX_VERSION) {
            return false;
        }
        return true;
    } 

    public static boolean includeInputParameterFlag(final int indexVersionNumber) {
        if (indexVersionNumber < COLUMN_INPUT_PARAMETER_FLAG_INDEX_VERSION) {
            return false;
        }
        return true;
    }

    public static boolean includeAnnotationProperties(final int indexVersionNumber) {
        if (indexVersionNumber < ANNOTATION_TAGS_INDEX_VERSION) {
            return true;
        }
        return false;
    }

    public static boolean includeTransformationUUID(final int indexVersionNumber) {
        if (indexVersionNumber < TRANSFORMATION_UUID_INDEX_VERSION) {
            return false;
        }
        return true;
    }
    
    private static boolean includeProcedureUpdateCount(final int indexVersionNumber) {
        return (indexVersionNumber >= PROCEDURE_UPDATE_COUNT_VERSION);
    }

    public static int getCurrentIndexVersionNumber() {
        return CURRENT_INDEX_VERSION;
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
    private static void setRecordHeaderValues(final AbstractMetadataRecord record, final String recordType, 
                                              final String upperName, final String objectID, final String fullName, 
                                              final String nameInSource, 
                                              final String parentObjectID) {
        
        record.setRecordType(recordType.toCharArray()[0]);
        record.setUUID(getObjectValue(objectID));
        record.setFullName(fullName);
        record.setNameInSource(getObjectValue(nameInSource));
        record.setParentUUID(getObjectValue(parentObjectID));
    }

    /**
     * Set the "footer" values on the specified MetadataRecord.
     * All index file record footers are of the form:
     * modelPath|name|indexVersion
     * The order of the fields in the index file header must also 
     * be the order of the arguments in method signature.
     */
    private static void setRecordFooterValues(final AbstractMetadataRecord record, final List tokens, int tokenIndex) {
    	if (record instanceof TransformationRecordImpl) {
    		((TransformationRecordImpl)record).setResourcePath(getOptionalToken(tokens, tokenIndex));
    	}
    	tokenIndex++;
        record.setName(getOptionalToken(tokens, tokenIndex++));
        //placeholder for index version
        getOptionalToken(tokens, tokenIndex++);
    }

    public static String getOptionalToken( final List tokens, int tokenIndex) {
        if(tokens.size() > tokenIndex) {
            return (String) tokens.get(tokenIndex);     
        }
        return null;
    }

}