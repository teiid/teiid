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

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.jboss.virtual.VFS;
import org.jboss.virtual.VirtualFile;
import org.jboss.virtual.VirtualFileFilter;
import org.jboss.virtual.plugins.context.zip.ZipEntryContext;
import org.jboss.virtual.spi.VirtualFileHandler;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.core.TeiidException;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.id.UUID;
import org.teiid.core.index.IEntryResult;
import org.teiid.core.util.ArgCheck;
import org.teiid.core.util.StringUtil;
import org.teiid.internal.core.index.Index;
import org.teiid.metadata.AbstractMetadataRecord;
import org.teiid.metadata.Column;
import org.teiid.metadata.ColumnSet;
import org.teiid.metadata.Datatype;
import org.teiid.metadata.ForeignKey;
import org.teiid.metadata.KeyRecord;
import org.teiid.metadata.MetadataStore;
import org.teiid.metadata.Procedure;
import org.teiid.metadata.ProcedureParameter;
import org.teiid.metadata.Schema;
import org.teiid.metadata.Table;
import org.teiid.metadata.VdbConstants;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.metadata.TransformationMetadata.Resource;


/**
 * Loads MetadataRecords from index files.  
 */
public class IndexMetadataFactory {
	
	private Index[] indexes;
	private RecordFactory recordFactory = new RecordFactory();
	private Map<String, String> annotationCache = new HashMap<String, String>();
	private Map<String, LinkedHashMap<String, String>> extensionCache = new HashMap<String, LinkedHashMap<String,String>>();
    private Map<String, Datatype> datatypeCache;
    private Map<String, KeyRecord> primaryKeyCache = new HashMap<String, KeyRecord>();
    private Map<String, Table> tableCache = new HashMap<String, Table>();
	private MetadataStore store;
	private HashSet<VirtualFile> indexFiles = new HashSet<VirtualFile>();
	private LinkedHashMap<String, Resource> vdbEntries;
	
	public IndexMetadataFactory() {
		
	}
	
	/**
	 * Load index metadata from a URL.  For the system and test vdbs
	 * @param url
	 * @throws IOException
	 * @throws URISyntaxException
	 */
	public IndexMetadataFactory(URL url) throws IOException, URISyntaxException {
		VFS.init();
		ZipEntryContext context = new ZipEntryContext(url);
		VirtualFileHandler vfh = context.getRoot();
		VirtualFile vdb = new VirtualFile(vfh);
		List<VirtualFile> children = vdb.getChildrenRecursively(new VirtualFileFilter() {
			@Override
			public boolean accepts(VirtualFile file) {
				return file.getName().endsWith(IndexConstants.NAME_DELIM_CHAR+IndexConstants.INDEX_EXT);
			}
		});
		
		for (VirtualFile f: children) {
			addIndexFile(f);
		}
		//just use the defaults for model visibility
		addEntriesPlusVisibilities(vdb, new VDBMetaData());
	}
    
	public MetadataStore getMetadataStore(Collection<Datatype> systemDatatypes) throws IOException {
		if (this.store == null) {
			this.store = new MetadataStore();
	    	ArrayList<Index> tmp = new ArrayList<Index>();
			for (VirtualFile f : indexFiles) {
				Index index = new Index(f, true);
				index.setDoCache(true);
	            tmp.add(index);
			}
			this.indexes = tmp.toArray(new Index[tmp.size()]);
			getAnnotationCache();
			getExtensionCache();			
			Map<String, Datatype> datatypes = getDatatypeCache();
			if (systemDatatypes != null) {
				for (Datatype datatype : systemDatatypes) {
					datatypes.put(datatype.getUUID(), datatype);
				}
			}
			List<KeyRecord> keys = findMetadataRecords(MetadataConstants.RECORD_TYPE.PRIMARY_KEY, null, false);
			for (KeyRecord keyRecord : keys) {
				this.primaryKeyCache.put(keyRecord.getUUID(), keyRecord);
			}
			getModels();
			getTables();
			getProcedures();
			//force close, since we cached the index files
			for (Index index : tmp) {
				index.close(); 
			}
		}
		return store;
    }

	private void getExtensionCache() {
		IEntryResult[] properties = queryIndex(MetadataConstants.RECORD_TYPE.PROPERTY, null, false);

		for (IEntryResult iEntryResult : properties) {
        	final String str = new String(iEntryResult.getWord());
            final List<String> tokens = RecordFactory.getStrings(str, IndexConstants.RECORD_STRING.RECORD_DELIMITER);

            String uuid = tokens.get(1);
	    	LinkedHashMap<String, String> result = this.extensionCache.get(uuid);
	    	if (result == null) {
	    		result = new LinkedHashMap<String, String>(); 
	    		this.extensionCache.put(uuid, result);
	    	}
            // The tokens are the standard header values
            int tokenIndex = 2;
            result.put( tokens.get(tokenIndex++), tokens.get(tokenIndex++));
		}
	}

	private void getAnnotationCache() {
		IEntryResult[] results = queryIndex(MetadataConstants.RECORD_TYPE.ANNOTATION, null, false);
		
		for (IEntryResult iEntryResult : results) {
	        final String str = new String(iEntryResult.getWord());
	        final List<String> tokens = RecordFactory.getStrings(str, IndexConstants.RECORD_STRING.RECORD_DELIMITER);

	        // Extract the index version information from the record 
	        int indexVersion = recordFactory.getIndexVersion(iEntryResult.getWord());
	        String uuid = tokens.get(2);
	        
	        // The tokens are the standard header values
	        int tokenIndex = 6;

	        if(recordFactory.includeAnnotationProperties(indexVersion)) {
				// The next token are the properties, ignore it not going to be read any way
	            tokenIndex++;
	        }

	        // The next token is the description
	        this.annotationCache.put(uuid, tokens.get(tokenIndex++));
		}
	}

    public void addIndexFile(VirtualFile f) {
    	this.indexFiles.add(f);
    }
    
	public void addEntriesPlusVisibilities(VirtualFile root, VDBMetaData vdb) throws IOException {
		LinkedHashMap<String, Resource> visibilityMap = new LinkedHashMap<String, Resource>();
		for(VirtualFile f: root.getChildrenRecursively()) {
			if (f.isLeaf()) {
				// remove the leading vdb name from the entry
				String path = f.getPathName().substring(root.getPathName().length());
				if (!path.startsWith("/")) { //$NON-NLS-1$
					path = "/" + path; //$NON-NLS-1$
				}
				visibilityMap.put(path, new Resource(f, isFileVisible(f.getPathName(), vdb))); 
			}
		}
		this.vdbEntries = visibilityMap;
	}
	
	private boolean isFileVisible(String pathInVDB, VDBMetaData vdb) {

		if (pathInVDB.endsWith(".xmi")) { //$NON-NLS-1$
			String modelName = StringUtil.getFirstToken(StringUtil.getLastToken(pathInVDB, "/"), "."); //$NON-NLS-1$ //$NON-NLS-2$
	
			ModelMetaData model = vdb.getModel(modelName);
			if (model != null) {
				return model.isVisible();
			}
		}
		
		if (pathInVDB.startsWith("META-INF/")) {//$NON-NLS-1$
			return false;
		}
		
        String entry = StringUtil.getLastToken(pathInVDB, "/"); //$NON-NLS-1$
        
        // index files should not be visible
		if( entry.endsWith(VdbConstants.INDEX_EXT) || entry.endsWith(VdbConstants.SEARCH_INDEX_EXT)) {
			return false;
		}

		// deployment file should not be visible
        if(entry.equalsIgnoreCase(VdbConstants.DEPLOYMENT_FILE)) {
            return false;
        }
        
        // any other file should be visible
        return true;		
	}
	
	public LinkedHashMap<String, Resource> getEntriesPlusVisibilities(){
		return this.vdbEntries;
	}
	
    public void getModels() {
    	Collection<Schema> records = findMetadataRecords(MetadataConstants.RECORD_TYPE.MODEL, null, false);
    	for (Schema modelRecord : records) {
			store.addSchema(modelRecord);
		}
    }
    
    public void getTables() {
    	for (Schema model : store.getSchemas().values()) {
			List<Table> records = findMetadataRecords(MetadataConstants.RECORD_TYPE.TABLE, model.getName() + IndexConstants.NAME_DELIM_CHAR + IndexConstants.RECORD_STRING.MATCH_CHAR, true);
			//load non-materialized first, so that the uuid->table cache is populated
			Collections.sort(records, new Comparator<Table>() {
				@Override
				public int compare(Table o1, Table o2) {
					if (!o1.isMaterialized()) {
						return -1;
					}
					if (!o2.isMaterialized()) {
						return 1;
					}
					return 0;
				}
			});
			for (Table tableRecord : records) {
				tableCache.put(tableRecord.getUUID(), tableRecord);
		    	List<Column> columns = new ArrayList<Column>(findChildRecords(tableRecord, MetadataConstants.RECORD_TYPE.COLUMN));
		        for (Column columnRecordImpl : columns) {
		    		columnRecordImpl.setDatatype(getDatatypeCache().get(columnRecordImpl.getDatatypeUUID()));
		    		columnRecordImpl.setParent(tableRecord);
		    		String fullName = columnRecordImpl.getName();
		    		if (fullName.startsWith(tableRecord.getName() + '.')) {
		    			columnRecordImpl.setName(new String(fullName.substring(tableRecord.getName().length() + 1)));
		    		}
				}
		        Collections.sort(columns);
		        tableRecord.setColumns(columns);
		        tableRecord.setAccessPatterns(findChildRecords(tableRecord, MetadataConstants.RECORD_TYPE.ACCESS_PATTERN));
		        Map<String, Column> uuidColumnMap = new HashMap<String, Column>();
		        for (Column columnRecordImpl : columns) {
					uuidColumnMap.put(columnRecordImpl.getUUID(), columnRecordImpl);
				}
		        for (KeyRecord columnSetRecordImpl : tableRecord.getAccessPatterns()) {
					loadColumnSetRecords(columnSetRecordImpl, uuidColumnMap);
					columnSetRecordImpl.setParent(tableRecord);
				}
		        tableRecord.setForiegnKeys(findChildRecords(tableRecord, MetadataConstants.RECORD_TYPE.FOREIGN_KEY));
		        for (ForeignKey foreignKeyRecord : tableRecord.getForeignKeys()) {
		        	foreignKeyRecord.setPrimaryKey(getPrimaryKey(foreignKeyRecord.getUniqueKeyID()));
		        	loadColumnSetRecords(foreignKeyRecord, uuidColumnMap);
		        	foreignKeyRecord.setParent(tableRecord);
				}
		        tableRecord.setUniqueKeys(findChildRecords(tableRecord, MetadataConstants.RECORD_TYPE.UNIQUE_KEY));
		        for (KeyRecord columnSetRecordImpl : tableRecord.getUniqueKeys()) {
					loadColumnSetRecords(columnSetRecordImpl, uuidColumnMap);
					columnSetRecordImpl.setParent(tableRecord);
				}
		        tableRecord.setIndexes(findChildRecords(tableRecord, MetadataConstants.RECORD_TYPE.INDEX));
		        for (KeyRecord columnSetRecordImpl : tableRecord.getIndexes()) {
					loadColumnSetRecords(columnSetRecordImpl, uuidColumnMap);
					columnSetRecordImpl.setParent(tableRecord);
				}
		        if (tableRecord.getPrimaryKey() != null) {
		        	KeyRecord primaryKey = getPrimaryKey(tableRecord.getPrimaryKey().getUUID());
		        	loadColumnSetRecords(primaryKey, uuidColumnMap);
		        	primaryKey.setParent(tableRecord);
		        	tableRecord.setPrimaryKey(primaryKey);
		        }
		        String groupUUID = tableRecord.getUUID();
		        if (tableRecord.isVirtual()) {
		        	TransformationRecordImpl update = (TransformationRecordImpl)getRecordByType(groupUUID, MetadataConstants.RECORD_TYPE.UPDATE_TRANSFORM,false);
			        if (update != null) {
			        	tableRecord.setUpdatePlan(update.getTransformation());
			        }
			        TransformationRecordImpl insert = (TransformationRecordImpl)getRecordByType(groupUUID, MetadataConstants.RECORD_TYPE.INSERT_TRANSFORM,false);
			        if (insert != null) {
			        	tableRecord.setInsertPlan(insert.getTransformation());
			        }
			        TransformationRecordImpl delete = (TransformationRecordImpl)getRecordByType(groupUUID, MetadataConstants.RECORD_TYPE.DELETE_TRANSFORM,false);
			        if (delete != null) {
			        	tableRecord.setDeletePlan(delete.getTransformation());
			        }
			        TransformationRecordImpl select = (TransformationRecordImpl)getRecordByType(groupUUID, MetadataConstants.RECORD_TYPE.SELECT_TRANSFORM,false);
			        // this group may be an xml document            
			        if(select == null) {
				        select = (TransformationRecordImpl)getRecordByType(groupUUID, MetadataConstants.RECORD_TYPE.MAPPING_TRANSFORM,false);
			        }
			        if (select != null) {
				        tableRecord.setSelectTransformation(select.getTransformation());
				        tableRecord.setBindings(select.getBindings());
				        tableRecord.setSchemaPaths(select.getSchemaPaths());
				        tableRecord.setResourcePath(select.getResourcePath());
			        }
		        }
		        if (tableRecord.isMaterialized()) {
		        	tableRecord.setMaterializedStageTable(tableCache.get(tableRecord.getMaterializedStageTable().getUUID()));
		        	tableRecord.setMaterializedTable(tableCache.get(tableRecord.getMaterializedTable().getUUID()));
		        }
				model.addTable(tableRecord);
			}
    	}
    }

	private KeyRecord getPrimaryKey(String uuid) {
		KeyRecord key = this.primaryKeyCache.get(uuid);
		if (key == null) {
            throw new TeiidRuntimeException(uuid+" PrimaryKey "+TransformationMetadata.NOT_EXISTS_MESSAGE); //$NON-NLS-1$
    	}
		return key;
	}
	
    public Map<String, Datatype> getDatatypeCache() {
		if (this.datatypeCache == null) {
			this.datatypeCache = new HashMap<String, Datatype>();
			Collection<Datatype> dataTypes = findMetadataRecords(MetadataConstants.RECORD_TYPE.DATATYPE, null, false);
			for (Datatype datatypeRecordImpl : dataTypes) {
				datatypeCache.put(datatypeRecordImpl.getUUID(), datatypeRecordImpl);
				this.store.addDatatype(datatypeRecordImpl);
			}
		}
		return datatypeCache;
	}
	
	private Column findElement(String fullName) {
        Column columnRecord = (Column)getRecordByType(fullName, MetadataConstants.RECORD_TYPE.COLUMN);
    	columnRecord.setDatatype(getDatatypeCache().get(columnRecord.getDatatypeUUID()));
        return columnRecord;
    }
	    
    private AbstractMetadataRecord getRecordByType(final String entityName, final char recordType) {
    	return getRecordByType(entityName, recordType, true);
    }
    
    private AbstractMetadataRecord getRecordByType(final String entityName, final char recordType, boolean mustExist) {
    	// Query the index files
		final Collection results = findMetadataRecords(recordType,entityName,false);
        
		int resultSize = results.size();
        if(resultSize == 1) {
            // get the columnset record for this result            
            return (AbstractMetadataRecord) results.iterator().next();
        }
        if(resultSize == 0) {
        	if (mustExist) {
			// there should be only one for the UUID
	            throw new TeiidRuntimeException(entityName+TransformationMetadata.NOT_EXISTS_MESSAGE);
        	} 
        	return null;
		} 
        throw new TeiidRuntimeException(RuntimeMetadataPlugin.Util.getString("TransformationMetadata.0", entityName)); //$NON-NLS-1$
    }
    
    public void getProcedures() {
    	for (Schema model : store.getSchemas().values()) {
			Collection<Procedure> procedureRecordImpls = findMetadataRecords(MetadataConstants.RECORD_TYPE.CALLABLE, model.getName() + IndexConstants.NAME_DELIM_CHAR + IndexConstants.RECORD_STRING.MATCH_CHAR, true);
			for (Procedure procedureRecord : procedureRecordImpls) {
		        // get the parameter metadata info
		        for (int i = 0; i < procedureRecord.getParameters().size(); i++) {
		            ProcedureParameter paramRecord = (ProcedureParameter) this.getRecordByType(procedureRecord.getParameters().get(i).getUUID(), MetadataConstants.RECORD_TYPE.CALLABLE_PARAMETER);
		            paramRecord.setDatatype(getDatatypeCache().get(paramRecord.getDatatypeUUID()));
		            procedureRecord.getParameters().set(i, paramRecord);
		            paramRecord.setProcedure(procedureRecord);
		        }
		    	
		        ColumnSet<Procedure> result = procedureRecord.getResultSet();
		        if(result != null) {
		            ColumnSet<Procedure> resultRecord = (ColumnSet<Procedure>) getRecordByType(result.getUUID(), MetadataConstants.RECORD_TYPE.RESULT_SET, false);
		            if (resultRecord != null) {
		            	resultRecord.setParent(procedureRecord);
		            	resultRecord.setName(RecordFactory.getShortName(resultRecord.getName()));
			            loadColumnSetRecords(resultRecord, null);
			            procedureRecord.setResultSet(resultRecord);
		            }
		            //it is ok to be null here.  it will happen when a 
		            //virtual stored procedure is created from a
		            //physical stored procedure without a result set
		            //TODO: find a better fix for this
		        }
	
		        // if this is a virtual procedure get the procedure plan
		        if(procedureRecord.isVirtual()) {
		    		TransformationRecordImpl transformRecord = (TransformationRecordImpl)getRecordByType(procedureRecord.getUUID(), MetadataConstants.RECORD_TYPE.PROC_TRANSFORM, false);
		    		if(transformRecord != null) {
		    			procedureRecord.setQueryPlan(transformRecord.getTransformation());
		    		}
		        }
				model.addProcedure(procedureRecord);
			}
    	}
    }
    
    /**
     * Finds children by parent uuid - note that this is not the best way to query for columns,
     * but it removes the need to store the parent uuid
     * @param parentRecord
     * @param childRecordType
     * @return
     */
    private List findChildRecords(final AbstractMetadataRecord parentRecord, final char childRecordType) {
    	// construct the pattern string
        String patternStr = getUUIDMatchPattern(childRecordType, parentRecord.getUUID(), true);
		// Query the model index files
		IEntryResult[] results = queryIndex(childRecordType, patternStr.toCharArray(), false, true, false);

		return loadRecords(results);        
    }
    
	private void loadColumnSetRecords(ColumnSet<?> indexRecord, Map<String, Column> columns) {
		for (int i = 0; i < indexRecord.getColumns().size(); i++) {
			String uuid = indexRecord.getColumns().get(i).getUUID();
			Column c = null;
			if (columns != null) {
				c = columns.get(uuid);
			} else {
				c = findElement(uuid);
				c.setName(RecordFactory.getShortName(c.getName()));
			}
			indexRecord.getColumns().set(i, c);
			if (columns == null) {
				c.setParent(indexRecord);
			}
		}
	}
    
	private List findMetadataRecords(final char recordType,
			final String entityName, final boolean isPartialName) {
		IEntryResult[] results = queryIndex(recordType, entityName, isPartialName);
		List<AbstractMetadataRecord> records = loadRecords(results);
		return records;
	}

	private List<AbstractMetadataRecord> loadRecords(
			IEntryResult[] results) {
		List<AbstractMetadataRecord> records = recordFactory.getMetadataRecord(results);
		
		for (AbstractMetadataRecord metadataRecord : records) {
			String uuid = metadataRecord.getUUID();
			
			metadataRecord.setAnnotation(this.annotationCache.get(uuid));
			metadataRecord.setProperties(this.extensionCache.get(uuid));
		}
		return records;
	}
    
    /**
     * Return the pattern match string that could be used to match a UUID in 
     * an index record. All index records contain a header portion of the form:  
     * recordType|pathInModel|UUID|nameInSource|parentObjectID|
     * @param uuid The UUID for which the pattern match string is to be constructed.
     * @return The pattern match string of the form: recordType|*|uuid|*
     */
    private String getUUIDMatchPattern(final char recordType, String uuid, boolean parent) {
        ArgCheck.isNotNull(uuid);
        // construct the pattern string
        String patternStr = String.valueOf(recordType) + IndexConstants.RECORD_STRING.RECORD_DELIMITER + IndexConstants.RECORD_STRING.MATCH_CHAR + IndexConstants.RECORD_STRING.RECORD_DELIMITER;
        if (parent) {
        	for (int i = 0; i < 3;  i++) {
        		patternStr += String.valueOf(IndexConstants.RECORD_STRING.MATCH_CHAR) + IndexConstants.RECORD_STRING.RECORD_DELIMITER;
        	}
        }
        patternStr += uuid.toLowerCase() + IndexConstants.RECORD_STRING.RECORD_DELIMITER + IndexConstants.RECORD_STRING.MATCH_CHAR;                    
        return patternStr;        
    }
        
	/**
	 * Return all index file records that match the specified entity name  
	 * @param indexName
	 * @param entityName the name to match
	 * @param isPartialName true if the entity name is a partially qualified
	 * @return results
	 * @throws QueryMetadataException
	 */
	private IEntryResult[] queryIndex(final char recordType, final String entityName, final boolean isPartialName) {

		IEntryResult[] results = null;

		// Query based on UUID
		if (StringUtil.startsWithIgnoreCase(entityName,UUID.PROTOCOL)) {
            String patternString = null;
            if (recordType == MetadataConstants.RECORD_TYPE.DATATYPE) {
                patternString = getDatatypeUUIDMatchPattern(entityName);
            } else {
                patternString = getUUIDMatchPattern(recordType,entityName, false);
            }
			results = queryIndex(recordType, patternString.toCharArray(), false, true, true);
		}

		// Query based on partially qualified name
		else if (isPartialName) {
			String patternString = getMatchPattern(recordType,entityName);
			results = queryIndex(recordType, patternString.toCharArray(), false, true, false);
		}

		// Query based on fully qualified name
		else {
			String prefixString  = getPrefixPattern(recordType,entityName);
			results = queryIndex(recordType, prefixString.toCharArray(), true, true, entityName != null);
		}

		return results;
	}
	
    /**
     * Return the pattern match string that could be used to match a UUID in 
     * a datatype index record. The RECORD_TYPE.DATATYPE records contain a header portion of the form:  
     * recordType|datatypeID|basetypeID|fullName|objectID|nameInSource|...
     * @param uuid The UUID for which the pattern match string is to be constructed.
     * @return The pattern match string of the form: recordType|*|*|*|uuid|*
     */
    private String getDatatypeUUIDMatchPattern(final String uuid) {
        ArgCheck.isNotNull(uuid);
        String uuidString = uuid;
        if (StringUtil.startsWithIgnoreCase(uuid,UUID.PROTOCOL)) {
            uuidString = uuid.toLowerCase();
        }
        // construct the pattern string
        String patternStr = "" //$NON-NLS-1$
                          + MetadataConstants.RECORD_TYPE.DATATYPE            //recordType
                          + IndexConstants.RECORD_STRING.RECORD_DELIMITER
                          + IndexConstants.RECORD_STRING.MATCH_CHAR        //datatypeID 
                          + IndexConstants.RECORD_STRING.RECORD_DELIMITER
                          + IndexConstants.RECORD_STRING.MATCH_CHAR        //basetypeID 
                          + IndexConstants.RECORD_STRING.RECORD_DELIMITER
                          + IndexConstants.RECORD_STRING.MATCH_CHAR        //fullName 
                          + IndexConstants.RECORD_STRING.RECORD_DELIMITER
                          + uuidString                                     //objectID
                          + IndexConstants.RECORD_STRING.RECORD_DELIMITER
                          + IndexConstants.RECORD_STRING.MATCH_CHAR;                    
        return patternStr;        
    }
	
    /**
     * Return the prefix match string that could be used to exactly match a fully 
     * qualified entity name in an index record. All index records 
     * contain a header portion of the form:  
     * recordType|pathInModel|UUID|nameInSource|parentObjectID|
     * @param name The fully qualified name for which the prefix match 
     * string is to be constructed.
     * @return The pattern match string of the form: recordType|name|
     */
    private String getPrefixPattern(final char recordType, final String name) {

        // construct the pattern string
        String patternStr = "" //$NON-NLS-1$
                          + recordType
                          + IndexConstants.RECORD_STRING.RECORD_DELIMITER;
        if(name != null) {                          
            patternStr = patternStr + name.trim().toUpperCase() + IndexConstants.RECORD_STRING.RECORD_DELIMITER;
        }                    

        return patternStr;
    }
	
    /**
     * Return the pattern match string that could be used to match a 
     * partially/fully qualified entity name in an index record. All index records 
     * contain a header portion of the form:  
     * recordType|pathInModel|UUID|nameInSource|parentObjectID|
     * @param name The partially/fully qualified name for which
     * the pattern match string is to be constructed.
     * @return The pattern match string of the form: recordType|name|* 
     */
    private String getMatchPattern(final char recordType, final String name) {
        ArgCheck.isNotNull(name);

        // construct the pattern string
        String patternStr = "" //$NON-NLS-1$
                          + recordType
                          + IndexConstants.RECORD_STRING.RECORD_DELIMITER;
        if(name != null) {
            patternStr =  patternStr + name.trim().toUpperCase()
                          + IndexConstants.RECORD_STRING.RECORD_DELIMITER
                          + IndexConstants.RECORD_STRING.MATCH_CHAR;
        }                    
        return patternStr;        
    }

    /**
     * Return all index file records that match the specified record pattern.
     * @param indexes the array of MtkIndex instances to query
     * @param pattern
     * @return results
     * @throws QueryMetadataException
     */
    private IEntryResult[] queryIndex(char recordType, final char[] pattern, boolean isPrefix, boolean isCaseSensitive, boolean returnFirstMatch) {
    	// The the index file name for the record type
        final String indexName = SimpleIndexUtil.getIndexFileNameForRecordType(recordType);
        Index[] search = SimpleIndexUtil.getIndexes(indexName, this.indexes);       
        
        if (search.length == 0) {
        	search = this.indexes;
        }

    	try {
            return SimpleIndexUtil.queryIndex(search, pattern, isPrefix, isCaseSensitive, returnFirstMatch);
        } catch (TeiidException e) {
            throw new TeiidRuntimeException(e);
        }
    }    
}
