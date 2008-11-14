/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

package com.metamatrix.metadata.runtime.model;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.vdb.api.ModelInfo;
import com.metamatrix.common.vdb.api.VDBArchive;
import com.metamatrix.connector.metadata.IndexFile;
import com.metamatrix.connector.metadata.MetadataConnectorConstants;
import com.metamatrix.connector.metadata.MultiObjectSource;
import com.metamatrix.connector.metadata.PropertyFileObjectSource;
import com.metamatrix.connector.metadata.internal.IObjectQuery;
import com.metamatrix.connector.metadata.internal.IObjectSource;
import com.metamatrix.connector.metadata.internal.MetadataException;
import com.metamatrix.connector.metadata.internal.ObjectQueryProcessor;
import com.metamatrix.core.CoreConstants;
import com.metamatrix.core.util.ArgCheck;
import com.metamatrix.core.vdb.ModelType;
import com.metamatrix.metadata.runtime.RuntimeMetadataCatalog;
import com.metamatrix.metadata.runtime.RuntimeMetadataPlugin;
import com.metamatrix.metadata.runtime.api.GroupID;
import com.metamatrix.metadata.runtime.api.MetadataSourceAPI;
import com.metamatrix.metadata.runtime.api.Model;
import com.metamatrix.metadata.runtime.api.ModelID;
import com.metamatrix.metadata.runtime.api.VirtualDatabase;
import com.metamatrix.metadata.runtime.api.VirtualDatabaseID;
import com.metamatrix.metadata.runtime.exception.VirtualDatabaseDoesNotExistException;
import com.metamatrix.metadata.runtime.exception.VirtualDatabaseException;
import com.metamatrix.modeler.core.index.IndexSelector;
import com.metamatrix.modeler.core.metadata.runtime.MetadataConstants;
import com.metamatrix.vdb.runtime.BasicModelInfo;

/**
 */
public class MetadataCache implements MetadataSourceAPI {
	
	private static class MetadataQuery implements IObjectQuery {
		
		private String[] columnNames;
		private String tableNameInSource;
		private Map criteria = new HashMap();
		
		public MetadataQuery(String[] columnNames, String tableNameInSource) {
			this.columnNames = columnNames;
			this.tableNameInSource = tableNameInSource;
		}
		
		public void checkCaseType(int i, Object value) {
		}

		public void checkType(int i, Object value) {
		}

		public Integer getCaseType(int i) {
			return NO_CASE;
		}

		public String[] getColumnNames() {
			return columnNames;
		}

		public Map getCriteria() throws MetadataException {
			return criteria;
		}

		public String getTableNameInSource() throws MetadataException {
			return tableNameInSource;
		}
	}

    private static final String GROUPS_NAME_IN_SOURCE = "TABLES.INDEX";  //$NON-NLS-1$
    private static final String[] GROUP_COLUMNS = new String [] {"UUID", "Name", "FullName",  "TableType", "isPhysical", "supportsUpdate", "ModelName", "isSystem"};  //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$  //$NON-NLS-4$  //$NON-NLS-5$ //$NON-NLS-6$  //$NON-NLS-7$  //$NON-NLS-8$
    private static final String PROCEDURES_NAME_IN_SOURCE = "PROCEDURES.INDEX#E";  //$NON-NLS-1$
    private static final String[] PROCEDURE_COLUMNS = new String [] {"FullName"};  //$NON-NLS-1$ //$NON-NLS-2$
    private static final String COLUMNS_NAME_IN_SOURCE = "COLUMNS.INDEX"; //$NON-NLS-1$
    private static final String[] ELEMENT_COLUMNS = new String [] {"UUID", "FullName", "isUpdatable", "ParentFullName"}; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$  //$NON-NLS-4$
    
    private VirtualDatabase vdb = null;
    // this map are the models from the database
    private Map modelsFromDB = new HashMap();
    private Map resourceMap = null;
    private Map models = null;
    private Map groupToModelMap = new HashMap();
    private Map groupMap = new HashMap();
    private Map procToModelMap = null;
    private Map columnsToGroupMap = null;
    
    // if false, then groups and column info will not be loaded
    private boolean includeMetadata = false;
    
    /**
     * pass true for loadMetadata if the information for the groups and elements
     * are to be loaded.  
     * 
     * The use of no metadata is used by the RMCHelper for creating a new
     * VDB prior to being added to the repository. 
     * @param loadMetadata
     * @since 4.2
     */
    public MetadataCache() {

    }

    /**
     * Returns <code>true</code> if the metadata for the groups and elements have
     * been loaded.   The loading of the details are only used when the metadata tree
     * is beind displayed.  So to cut down on the overhead when it's never been asked
     * for, the details are not loaded when only the models are needed by
     * query processing. 
     * @return
     * @throws VirtualDatabaseException
     * @since 4.2
     */
     public boolean isModelDetailsLoaded()  {
         return includeMetadata;
     }

     /**
      * used for loading the SystemVDB models. 
      * @param vdbName
      * @param vdbVersion
      * @param systemvdb
      * @throws VirtualDatabaseException
      * @since 4.2
      */
     public void initSystemVDB(String vdbName, String vdbVersion, byte[] systemvdb) throws VirtualDatabaseException {
         Map sys = new HashMap();
         
         includeMetadata = false;
         BasicVirtualDatabaseID vdbid = new BasicVirtualDatabaseID(vdbName, vdbVersion);//$NON-NLS-1$
         BasicVirtualDatabase bvdb = new BasicVirtualDatabase(vdbid);
         bvdb.setFileName(CoreConstants.SYSTEM_VDB);
         vdb = bvdb;
         
         sys = loadModelsUsingVDBContext(vdb.getVirtualDatabaseID(), systemvdb); 
         models = new HashMap(sys.size());
         models.putAll(sys);
         
         LogManager.logInfo(RuntimeMetadataPlugin.PLUGIN_ID, RuntimeMetadataPlugin.Util.getString("RuntimeMetadataCatalog.System_cache_initialized"));  //$NON-NLS-1$
         
//         I18nLogManager.logCritical(LogRuntimeMetadataConstants.CTX_RUNTIME_METADATA, LogMessageKeys.MDC_0001);
     }

    /**
     * Call to initialize the adapter by providing a specific VDB defintion content to use
     * when querying the metadata 
     * *** NOTE: This is used for UnitTest only
     * @param vdbName
     * @param vdbVersion
     * @param vdbFileName
     * @param vdbcontents
     * @throws Exception
     */        
    public void init(String vdbName, String vdbVersion, String vdbFileName, byte[] vdbcontents) throws Exception {
        // needs system VDB
        BasicVirtualDatabaseID vdbid = new BasicVirtualDatabaseID(vdbName, vdbVersion);//$NON-NLS-1$
        BasicVirtualDatabase bvdb = new BasicVirtualDatabase(vdbid);
        bvdb.setFileName(vdbFileName);
        vdb = bvdb;

        init(bvdb, Collections.EMPTY_LIST, false, vdbcontents, new HashMap(1));
    }

	/**
	 * Call to initialize the adapter by providing a specific VDB defintion content to use
	 * when querying the metadata 
	 * @param vdb contains vdb specific information in the database
	 * @param modelList is a collection of Models from the database, providing properties
	 * not currently stored in the vdbcontents.
	 * @param vdbcontents is the vdb to queried
	 * @param systemModels are the system models preload incase metadata is not used
	 * @param systemvdb is the system vdb byte array
	 * @throws Exception
	 */        
    public void init(VirtualDatabase vdb, Collection modelList, final boolean loadMetadata, final byte[] vdbcontents, Map systemModels) throws VirtualDatabaseException{
        // needs system VDB
        ArgCheck.isNotNull(vdb, "VDB must be specified."); //$NON-NLS-1$
        if (modelList == null) {
            modelList = Collections.EMPTY_LIST;
        }
        ArgCheck.isNotNull(vdbcontents, "VDB contents for " + vdb.getFullName() + " must be specified."); //$NON-NLS-1$ //$NON-NLS-2$

        this.vdb = vdb;

        for (Iterator it=modelList.iterator(); it.hasNext();) {
            Model m = (Model) it.next();
            modelsFromDB.put(m.getName(), m);
        }

        models = new HashMap();

        Map mVB = loadModelsUsingVDBContext(vdb.getVirtualDatabaseID(), vdbcontents);
        models.putAll(mVB);
        
        models.putAll(systemModels);

        if (loadMetadata) {
            includeMetadata = true;
            loadMetadata();
        } 
        
        LogManager.logInfo(RuntimeMetadataPlugin.PLUGIN_ID, RuntimeMetadataPlugin.Util.getString("RuntimeMetadataCatalog.VDB_cache_initialized", vdb.getName()));  //$NON-NLS-1$
        
//        I18nLogManager.logCritical(LogRuntimeMetadataConstants.CTX_RUNTIME_METADATA, LogMessageKeys.MDC_0001);
     }

    public void loadModelDetails()  throws VirtualDatabaseException {
        includeMetadata = true;
        loadMetadata();       
    }

    private void loadMetadata() throws VirtualDatabaseException {
        groupToModelMap = new HashMap();
        groupMap = new HashMap();
        procToModelMap = new HashMap();
        columnsToGroupMap = new HashMap();
 
        try {
			buildGroupObjects(getObjectQueryProcessor().process(new MetadataQuery(GROUP_COLUMNS, GROUPS_NAME_IN_SOURCE)));
			buildColumnObjects(getObjectQueryProcessor().process(new MetadataQuery(ELEMENT_COLUMNS, COLUMNS_NAME_IN_SOURCE)));
			buildProcedureObjects(getObjectQueryProcessor().process(new MetadataQuery(PROCEDURE_COLUMNS, PROCEDURES_NAME_IN_SOURCE)));
		} catch (MetadataException e) {
			throw new VirtualDatabaseException(e);
		}
    }
    
    protected ObjectQueryProcessor getObjectQueryProcessor() throws VirtualDatabaseException {
    	final VirtualDatabaseID vdbID = vdb.getVirtualDatabaseID();
    	IndexSelector selector = RuntimeMetadataCatalog.getQueryMetadataCache().getCompositeSelector(vdbID.getName(), vdbID.getVersion());        
        
        // this is less than ideal, put since we are being called statically and we haven't bothered changing all the methods
    	// we don't have a reference to the actual vdbservice.  we can however just make direct calls.
        IObjectSource indexFile = new IndexFile(selector, vdbID.getName(), vdbID.getVersion(), null);

        IObjectSource multiObjectSource = new MultiObjectSource(indexFile, MetadataConnectorConstants.PROPERTIES_FILE_EXTENSION, new PropertyFileObjectSource());
        
        // return an adapter object that has access to all sources
        return new ObjectQueryProcessor(multiObjectSource);
    }

    private Map loadModelsUsingVDBContext(VirtualDatabaseID vdbID, byte[] vdbcontents) throws VirtualDatabaseException {
        Map modelMap = new HashMap();
        
        VDBArchive vdbArchive = null;
        try {
        	
            vdbArchive = new VDBArchive(new ByteArrayInputStream(vdbcontents));
            if (vdbArchive.getVDBValidityErrors() != null) {
                throw new VirtualDatabaseException(VirtualDatabaseException.VDB_NON_DEPLOYABLE_STATE, RuntimeMetadataPlugin.Util.getString("MetadataCache.VDB_is_at_a_nondeployable_severity_state", new Object[] {vdbArchive.getName(), "ERROR"} ));  //$NON-NLS-1$ //$NON-NLS-2$
            }

            Collection<BasicModelInfo> modelInfos = vdbArchive.getConfigurationDef().getModels();
            Map resourceToModel = new HashMap(modelInfos.size());
            BasicModel model;
            
            for (BasicModelInfo modelInfo:modelInfos) {

                int modelType = modelInfo.getModelType();
                switch (modelType) {

                	case ModelType.PHYSICAL :
                    case ModelType.VIRTUAL :
                    case ModelType.MATERIALIZATION:
                        
                        // if the database provides the model, then use it
                        // because of this info overrides that in the VDB jar
                        if (modelsFromDB.containsKey(modelInfo.getName())) {
                            BasicModel dbmodel = (BasicModel) modelsFromDB.get(modelInfo.getName());
                            
                            // create a new model based on the model retrieved from the database
                            // must recreate the model based on the modelref so that all models
                            // derived from the .VDB file are created the same for comparison sake.
                            // The reason for this change is that 2 models can be named the same,
                            // except the extension will be distinguishable.
                            model = createBasicModel( (BasicVirtualDatabaseID) vdbID, modelInfo, dbmodel);
                            
                            resourceToModel.put(modelInfo.getPath(), model);
                        } else {
                            model = createBasicModel( (BasicVirtualDatabaseID) vdbID, modelInfo);
                        }
                        break;
                    default:
                        model = createBasicModel( (BasicVirtualDatabaseID) vdbID, modelInfo);
                        break;
                }
                modelMap.put(model.getID(), model);  
            }

            Set<String> entriesInVDB = vdbArchive.getEntries();
            resourceMap = new HashMap(entriesInVDB.size());
            
            for (String entry:entriesInVDB) {
            	boolean isVisible = false;
            	
            	model = (BasicModel)resourceToModel.get(entry);
            	if (model != null) {
            		isVisible = model.isVisible();
            	}
            	else {
            		isVisible = vdbArchive.isVisible(entry);
            	}
            	
            	String path = entry.startsWith("/")?entry:"/"+entry; //$NON-NLS-1$ //$NON-NLS-2$                
                resourceMap.put(path, new Resource(path, isVisible));
            }

        } catch (VirtualDatabaseException e) {
            throw e;
        } catch (Exception e) {
             throw new VirtualDatabaseException(e);
         } finally {
            if (vdbArchive != null ) {
            	vdbArchive.close();
            }                              
        }
        return modelMap;
    }
    
    /* 
     * @see com.metamatrix.metadata.runtime.api.MetadataSourceAPI#getVirtualDatabase()
     */
    public VirtualDatabase getVirtualDatabase() throws VirtualDatabaseException {
        return this.vdb;
    }

    /* 
     * @see com.metamatrix.metadata.runtime.api.MetadataSourceAPI#getVirtualDatabaseID()
     */
    public VirtualDatabaseID getVirtualDatabaseID()
        throws VirtualDatabaseDoesNotExistException, VirtualDatabaseException {
        return this.vdb.getVirtualDatabaseID();
    }

    public Collection getAllModels() throws VirtualDatabaseException {
        if (models != null) {
//            System.out.println("GETMODELS in Cache");
            List m = new ArrayList();
            m.addAll(models.values());
            return m;
        }
        return Collections.EMPTY_LIST;
    }

    /** 
     * This returns those models that should be made visible to the console and
     * therefore, only the models from the database are considered for display
     * @see com.metamatrix.metadata.runtime.api.MetadataSourceAPI#getDisplayableModels()
     * @since 4.2
     */
    public Collection getDisplayableModels() throws VirtualDatabaseException {
        Collection result = new ArrayList(models.size());
        Iterator it = models.keySet().iterator();
        while (it.hasNext()) {
            final ModelID id = (ModelID) it.next();
            final Model m = (Model) models.get(id);

            switch (m.getModelType()) {
                case ModelType.PHYSICAL :
                case ModelType.VIRTUAL :
                case ModelType.MATERIALIZATION:
                    if (modelsFromDB.containsKey(m.getName())) {
                        Model dbModel = (BasicModel) modelsFromDB.get(m.getName());
                        if (dbModel.isVisible()) {
                            result.add(dbModel);
                        }
                    } else {
                        if (m.isVisible()) {
                            result.add(m);
                        }
                    }
                	break;
            }
        }
        return result;
    }

    public boolean isVisible(String resourcePath) {
        if (resourceMap == null) {
            return false;
        }
        Resource r = (Resource)  resourceMap.get(resourcePath);
        if (r!=null) {
            return r.isVisible();
        }
        return false;
    }

    public Map getModelMap() throws VirtualDatabaseException {
        Map modelMap = new HashMap(models.size());
        modelMap.putAll(models);
        return modelMap;
    }

    /** 
     * @see com.metamatrix.metadata.runtime.api.MetadataSourceAPI#getModel(com.metamatrix.metadata.runtime.api.ModelID)
     * @since 4.2
     */
    public Model getModel(ModelID modelID) throws VirtualDatabaseException {
        return (Model) models.get(modelID);
    }

    /** 
     * @see com.metamatrix.metadata.runtime.api.MetadataSourceAPI#getModelsForVisibility(boolean)
     * @since 4.2
     */
    public Collection getModelsForVisibility(boolean isVisible) throws VirtualDatabaseException {
        Collection result = new ArrayList(models.size());
        Iterator it = models.keySet().iterator();
        while (it.hasNext()) {
            Model m = (Model) it.next();
            if (m.isVisible() == isVisible) {
                result.add(m);
            }
        }
        return result;
    }    

    /* 
     * @see com.metamatrix.metadata.runtime.api.MetadataSourceAPI#getGroupsInModel(com.metamatrix.metadata.runtime.api.ModelID)
     */
    public Collection getGroupsInModel(ModelID modelID) throws VirtualDatabaseException {
        return getGroups(modelID);
    }    

    /**
     * Returns all the groups in the VDB
     * @return
     * @throws Exception
     */
    public List getGroups(ModelID modelID) throws VirtualDatabaseException {
        if (groupToModelMap.containsKey(modelID)) {
//            System.out.println("getGroups in cache ");
            return (List) groupToModelMap.get(modelID);
        }
        return Collections.EMPTY_LIST;
    }

    /* 
     * @see com.metamatrix.metadata.runtime.api.MetadataSourceAPI#getElementsInGroup(com.metamatrix.metadata.runtime.api.GroupID)
     */
    public List getElementsInGroup(GroupID groupID) throws VirtualDatabaseException {
        return getColumns(groupID);
    }    

    /**
     * Returns all the columns for a group in the VDB
     * @return
     * @throws Exception
     */    
    public List getColumns(GroupID groupID) throws VirtualDatabaseException {
//       System.out.println("CACHE: get Columns for " + groupID.getFullName());
        if (columnsToGroupMap.containsKey(groupID.getFullName())) {
//            System.out.println("getColumns in Cache ");
            
            return (List) columnsToGroupMap.get(groupID.getFullName());
        }
        // since all the columns for all the groups are loaded
        // at one time, if the group does not exist with columns
        // and the map is not empty, it
        // has to be assumed there are no columns for that group
        return Collections.EMPTY_LIST;
    }

    /* 
     * @see com.metamatrix.metadata.runtime.api.MetadataSourceAPI#getProceduresInModel(com.metamatrix.metadata.runtime.api.ModelID)
     */
    public Collection getProceduresInModel(ModelID modelID) throws VirtualDatabaseException {
        return getProcedures(modelID);
    }

    /**
     * Returns all the procedures in the VDB
     * @return
     * @throws Exception
     */
    public List getProcedures(ModelID modelID) throws VirtualDatabaseException {
        if (procToModelMap.containsKey(modelID)) {
//            System.out.println("getProcedures in cache ");
            return (List) procToModelMap.get(modelID);
        }
        return Collections.EMPTY_LIST;  
    }

    /**
     * When making changes, refer to {@link #MODEL_COLUMNS MODEL_COLUMNS}
     * regarding columns and order
     */
//    private Map buildModelsObjects(List objectList) {
//        Map result = new HashMap(objectList.size());
//       
////        String version = "0";//$NON-NLS-1$
////     System.out.println("*** BUILD MODEL OBJECTS *** : " + objectList.size());
//        String uuid=null;
//        String name=null;
//        String uri = null;
//        for (Iterator it=objectList.iterator(); it.hasNext(); ) {
//            List cols = (List) it.next();
//            BasicModel model;
//            BasicModelID modelID;
//            
////            int i=0;
////            for (Iterator it2=cols.iterator(); it2.hasNext(); ) {
////                Object o = it2.next();
////                System.out.println("Value("+i+"): " + (o==null?"null":o.toString()));
////                i++;
////            }
////            
//            uuid = (String) cols.get(0);
//            name = (String) cols.get(1);
//            Integer modelType = (Integer) cols.get(3);                                                 
//            uri = (String) cols.get(4);
//            Boolean isVisible = (Boolean) cols.get(5);
////
////            if (!isVisible.booleanValue()) {
////                continue;            
////            }
//            
////            if (modelType.intValue() != 0) {
////                continue;            
////            }
//            
//            
//            // HACK for now, until the model types 
//            // are being saved in the VDB's correctly.            
////            if (name.equalsIgnoreCase(DatatypeConstants.DATATYPES_MODEL_FILE_NAME_WITHOUT_EXTENSION) ||
////                name.equalsIgnoreCase(DatatypeConstants.DATATYPES_MODEL_FILE_NAME) )  { //$NON-NLS-1$
////                
////                continue;
////           }
////            boolean isphysical = ((Boolean) cols.get(2)).booleanValue();
//              
////            System.out.println("Model " + name + " ModelType " + ModelType.get(modelType.intValue()).getName());
//            
//            
//            // NOTE: the best practice is to try to derive everything that can be
//            // derived from the vdbcontents, not the database.
//            // fill in with the database only for those things the model does
//            // not track.
//            // 1.  Visibility
//            // 2.  ConnectorBinding 
//            if (modelsFromDB.containsKey(name)) {
//                model = (BasicModel) modelsFromDB.get(name);
//                modelID = (BasicModelID) model.getID();
//                
//                // visibility should be stored in the database
//               
//            } else {
//                modelID = new BasicModelID(name);
//                model = new BasicModel(modelID, (BasicVirtualDatabaseID)vdb.getVirtualDatabaseID());
//                // if the database provides the model, then do not override its value
//                model.setIsVisible(isVisible.booleanValue());
//            }
//                        
//            model.setModelType(modelType.intValue());
//            modelID.setUuid(uuid);      
//            model.setModelURI(uri);  
//                              
//            result.put(model.getID(), model);  
//            
//                
//        }    
//       
//                        
//        return result;
//       
//    }
    
    /**
     * Bould groups to be displayed in the metadata tree.
     * @param objectList  List of Lists of Objects: List of objects to add to the cache.
     * Each element in objectList represents a row containing data about one group.
     * See {@link #GROUP_COLUMNS GROUP_COLUMNS} regarding the order and meaning of the column in the row..
     */    
    protected void buildGroupObjects(Iterator it) {
      
         boolean isPhysical=false;
         boolean supportsUpdates=false;
         String fullname=null;
         List groups = null;
         while (it.hasNext()) {
             List cols = (List) it.next();
           
             fullname = (String) cols.get(2);
             Integer type = (Integer) cols.get(3);
             
             
             switch (type.shortValue()) {
                //only these 4 types are needed in the entitlement tree           
                case MetadataConstants.TABLE_TYPES.TABLE_TYPE :
                case MetadataConstants.TABLE_TYPES.VIEW_TYPE :
                case MetadataConstants.TABLE_TYPES.DOCUMENT_TYPE :
                case MetadataConstants.TABLE_TYPES.MATERIALIZED_TYPE :
                    break;
                default :
                    continue;
              }

             isPhysical = ((Boolean) cols.get(4)).booleanValue();
             supportsUpdates = (type.shortValue() == MetadataConstants.TABLE_TYPES.MATERIALIZED_TYPE) || 
                 ((Boolean) cols.get(5)).booleanValue();             

             BasicGroupID gID = new BasicGroupID(fullname);
             BasicGroup group = new BasicGroup(gID, (BasicVirtualDatabaseID) vdb.getVirtualDatabaseID());
             group.setSupportsUpdate(supportsUpdates);
             group.setIsPhysical(isPhysical);
             group.setTableType(type.shortValue());
             
             groups = getGroupsInMap(gID.getModelID());
             groups.add(group);
             // the fullname is used because when the columns are loaded,
             // this parentpath is verified it exists before the element is included
             groupMap.put(gID.getFullName(), group);
         }     
     }

    private List getGroupsInMap(ModelID modelID) {
        if (groupToModelMap.containsKey(modelID)) {
            return (List) groupToModelMap.get(modelID);
        }
        List groups = new ArrayList();
        groupToModelMap.put(modelID, groups);
        return groups;
    }

     /**
     * When making changes, refer to {@link #ELEMENT_COLUMNS ELEMENT_COLUMNSS}
     * regarding columns and order
     */
    
    private void buildColumnObjects(Iterator it) {
       
        String name=null;
//        String path=null;
        boolean supportsUpdates;
        List columns = null;
        
        while (it.hasNext()) {
            List cols = (List) it.next();
            
//          int i=0;
//          for (Iterator it2=cols.iterator(); it2.hasNext(); ) {
//              Object o = it2.next();
//              System.out.println("Value("+i+"): " + (o==null?"null":o.toString()));
//              i++;
//          }
//            String uuid = (String) cols.get(0);
            name = (String) cols.get(1);

             supportsUpdates = ((Boolean) cols.get(2)).booleanValue();           
            String parentPath = (String) cols.get(3);
//          System.out.println("** COL: " + name + " parent " + parentPath);

            // if the column doesnt belong to a parent it cannot be displayed
            // in the tree, an example of these types are return type columns             
            if (parentPath != null && parentPath.trim().length() > 0) {
                // if the parent doesnt exist, no tree node to add to
                if (groupMap.containsKey(parentPath)) {
//                    System.out.println("** ADDED COL: " + name + " parent " + parentPath);

                 BasicElementID elementID = new BasicElementID(name);

                 elementID.setGroupFullName(parentPath);
                 BasicElement element = new BasicElement(elementID, (BasicVirtualDatabaseID)vdb.getVirtualDatabaseID());
                 element.setSupportsUpdate(supportsUpdates);
                 columns = getColumnsInMap(elementID.getParentFullName());
                 columns.add(element);
                }
            }
        }       
    }      

    private List getColumnsInMap(String groupName) {
        if (columnsToGroupMap.containsKey(groupName)) {
            return (List) columnsToGroupMap.get(groupName);
        }
        List columns = new ArrayList();
        columnsToGroupMap.put(groupName, columns);
        return columns;
    }     

    /**
    * When making changes, refer to {@link #PROCEDURE_COLUMNS PROCEDURE_COLUMNS}
    * regarding columns and order
    */  
    private void buildProcedureObjects(Iterator it) {
       
        String name=null;
        List procedures = null;
        while (it.hasNext()) {
            List cols = (List) it.next();

            name = (String) cols.get(0);
            
            BasicProcedureID procID = new BasicProcedureID(name);
            BasicProcedure procedure = new BasicProcedure(procID, (BasicVirtualDatabaseID)vdb.getVirtualDatabaseID());
            
            procedures = getProceduresInMap(procID.getModelID());
            procedures.add(procedure);                
        }     
       
    } 

    private List getProceduresInMap(ModelID modelID) {
        if (procToModelMap.containsKey(modelID)) {
            return (List) procToModelMap.get(modelID);
        }
        List procs = new ArrayList();
        procToModelMap.put(modelID, procs);
        return procs;
    }    
    
    private class Resource {
        private String path = null;
        private boolean isVisible = false;
        
        public Resource(String resourcePath, boolean visible) {
            path = resourcePath;
            isVisible = visible;
        }

        public String getResourcePath() {
            return path;
        }

        public boolean isVisible() {
            return isVisible;
        }
    }
    
    BasicModel createBasicModel(BasicVirtualDatabaseID virtualDBID, ModelInfo modelInfo) {
        BasicModel result = new BasicModel(createID(modelInfo), virtualDBID);
     
        result.setModelType(modelInfo.getModelType());
        result.setModelURI(modelInfo.getModelURI());
        
        result.setIsVisible(modelInfo.isVisible()); 
        
        String uuid = (modelInfo.getUUID() == null ? "NoUUID" : modelInfo.getUUID()); //$NON-NLS-1$
        result.setGUID(uuid);

        result.setVersionDate( new Date());
        result.setVersionedBy(""); //$NON-NLS-1$
        return result;
    }
    
    BasicModel createBasicModel(BasicVirtualDatabaseID virtualDBID, ModelInfo modelInfo, BasicModel basicModel) {
    	BasicModel result = new BasicModel(createID(modelInfo), virtualDBID);
     
    	result.setModelType(modelInfo.getModelType());
    	result.setModelURI(modelInfo.getModelURI());
        
    	result.setConnectorBindingNames(basicModel.getConnectorBindingNames());
    	result.enableMutliSourceBindings(basicModel.isMultiSourceBindingEnabled());
    	result.setDescription(basicModel.getDescription());
        
    	result.setIsVisible(basicModel.isVisible()); 
        
        String uuid = (modelInfo.getUUID() == null ? "NoUUID" : modelInfo.getUUID()); //$NON-NLS-1$
        result.setGUID(uuid);

        result.setVersionDate(basicModel.getDateVersioned());
        result.setVersionedBy(basicModel.getVersionedBy()); //$NON-NLS-1$
        return result;
    }    
    
    private static BasicModelID createID(ModelInfo modelInfo) {
        String v = (modelInfo.getVersion() == null ? "0" : modelInfo.getVersion()); //$NON-NLS-1$
    	BasicModelID modelID = new BasicModelID(modelInfo.getName(), v);
        String uuid = (modelInfo.getUUID() == null ? "NoUUID" : modelInfo.getUUID()); //$NON-NLS-1$        
        modelID.setUuid(uuid);
        return modelID;
    }
    
}