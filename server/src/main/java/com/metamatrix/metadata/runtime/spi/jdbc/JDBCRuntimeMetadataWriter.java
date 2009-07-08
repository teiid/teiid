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

package com.metamatrix.metadata.runtime.spi.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.teiid.metadata.RuntimeMetadataPlugin;

import com.metamatrix.common.config.CurrentConfiguration;
import com.metamatrix.common.config.api.ConnectorBinding;
import com.metamatrix.common.jdbc.JDBCReservedWords;
import com.metamatrix.common.log.I18nLogManager;
import com.metamatrix.core.util.DateUtil;
import com.metamatrix.metadata.runtime.api.Model;
import com.metamatrix.metadata.runtime.api.ModelID;
import com.metamatrix.metadata.runtime.api.VirtualDatabase;
import com.metamatrix.metadata.runtime.api.VirtualDatabaseException;
import com.metamatrix.metadata.runtime.api.VirtualDatabaseID;
import com.metamatrix.metadata.runtime.exception.InvalidStateException;
import com.metamatrix.metadata.runtime.model.BasicModelID;
import com.metamatrix.metadata.runtime.model.BasicVirtualDatabase;
import com.metamatrix.metadata.runtime.model.BasicVirtualDatabaseID;
import com.metamatrix.metadata.runtime.util.LogRuntimeMetadataConstants;
import com.metamatrix.metadata.util.ErrorMessageKeys;

public final class JDBCRuntimeMetadataWriter {



/**
 * Updates the <code>VirtualDatabase</code> status.
 * @param vdb is the VirtualDatabase to be updated
 * @param userName of the person requesting the change
 * @exception RuntimeMetadataException if unable to perform update.
 * @exception InvalidStateException if the status to be set is invalid.
 * @param status is the state the VirtualDatabase should be set to
 */
    public static void setStatus(VirtualDatabaseID virtualDBID, long uid, short status, String userName, Connection jdbcConnection) throws InvalidStateException, VirtualDatabaseException {

        PreparedStatement statement = null;
        String sql = null;

        try{
            sql = JDBCTranslator.UPDATE_SET_STATUS;
            statement = jdbcConnection.prepareStatement(sql);

            statement.setShort(1, status);
            statement.setString(2, userName);
            statement.setString(3, DateUtil.getCurrentDateAsString());
            statement.setLong(4, uid);
            if (statement.executeUpdate() != 1){
                throw new VirtualDatabaseException(ErrorMessageKeys.JDBCW_0003, RuntimeMetadataPlugin.Util.getString(ErrorMessageKeys.JDBCW_0003, new Short(status), virtualDBID) );
            }
        }catch (SQLException se){
            throw new VirtualDatabaseException(se, ErrorMessageKeys.JDBCW_0002, RuntimeMetadataPlugin.Util.getString(ErrorMessageKeys.JDBCW_0002, sql) );
        }finally {
            if ( statement != null ) {
                try {
                    statement.close();
                } catch ( SQLException e ) {
                    I18nLogManager.logError(LogRuntimeMetadataConstants.CTX_RUNTIME_METADATA, ErrorMessageKeys.GEN_0008, e);
                }
            }
        }
    }


  public static void setConnectorBindingNames(VirtualDatabaseID vdbID, Collection models, Map modelAndCBNames, Connection jdbcConnection) throws VirtualDatabaseException{
      Map multiModels = new HashMap(models.size());
      Map singleModels = new HashMap(models.size());
      for (Iterator it=models.iterator(); it.hasNext();) {
          Model m =(Model) it.next();
          if (m.supportsMultiSourceBindings()) {
              multiModels.put(m.getID(), modelAndCBNames.get(m.getID()));
          } else {
              singleModels.put(m.getID(), modelAndCBNames.get(m.getID()));
          }
      }
      
      if (singleModels.size() > 0) {
          updatetSingleConnectorBindingName(vdbID, singleModels, jdbcConnection);
      }
      if (multiModels.size() > 0) {
          // 1st remove the existing bindings and then add the set back
          // this is done so that the logic doesn't need to figure out
          // what was new, removed, etc.
          deleteVDBModels(multiModels.keySet(), vdbID, jdbcConnection);
          insertMultiSourceVDBModels(multiModels, vdbID, jdbcConnection);
      }
      
      
//      Collection modelIDs = modelAndCBNames.keySet();
//        PreparedStatement statement = null;
//        String sql = null;
//        
//        Map modelMap = new HashMap(models.size());
//        for (Iterator it=models.iterator(); it.hasNext();) {
//            Model m = (Model) it.next();
//            modelMap.put(m.getID(), m);            
//        }        
//
//        try{
//            sql = JDBCTranslator.UPDATE_CONNECTOR_BINGING_NAME;
//            statement = jdbcConnection.prepareStatement(sql);
//            Iterator iter = modelIDs.iterator();
//            BasicModelID modelID;
//            while(iter.hasNext()){
//                modelID = (BasicModelID)iter.next();
//                
//                List cbNames = (List) modelAndCBNames.get(modelID);
//                for (Iterator it=cbNames.iterator(); it.hasNext();) {
//                    String name =(String) it.next();
//                
//                    statement.clearParameters();
//                    statement.setString(1, name);
//                    statement.setLong(2, ((BasicVirtualDatabaseID)vdbID).getUID());
//                    statement.setLong(3, modelID.getUID());
//    
//                    if (statement.executeUpdate() != 1){
//                        throw new VirtualDatabaseException(ErrorMessageKeys.JDBCW_0004, RuntimeMetadataPlugin.Util.getString(ErrorMessageKeys.JDBCW_0002, new Object[]{sql} ) );
//                    }
//                }
//            }
//        }catch (SQLException se){
//            I18nLogManager.logError(LogRuntimeMetadataConstants.CTX_RUNTIME_METADATA, ErrorMessageKeys.JDBCW_0001, se, new Object[]{sql});
//            throw new VirtualDatabaseException(se, ErrorMessageKeys.JDBCW_0002, RuntimeMetadataPlugin.Util.getString(ErrorMessageKeys.JDBCW_0002, sql) );
//        }finally {
//            if ( statement != null ) {
//                try {
//                    statement.close();
//                } catch ( SQLException e ) {
//                    I18nLogManager.logError(LogRuntimeMetadataConstants.CTX_RUNTIME_METADATA, ErrorMessageKeys.GEN_0008, e);
//                }
//            }
//        }
    }
  
  private static void insertMultiSourceVDBModels(Map models, VirtualDatabaseID vdbID, Connection jdbcConnection) throws VirtualDatabaseException{
      PreparedStatement statement = null;
      String sql = null;

      try{
          sql = JDBCTranslator.INSERT_VDB_MODELS_WITH_BINDING;
          statement = jdbcConnection.prepareStatement(sql);
          Iterator iter = models.keySet().iterator();
          long modelUID;
          while(iter.hasNext()){
              
              ModelID modelID = (ModelID) iter.next();
              
              Collection bindings = (List) models.get(modelID);
              if (bindings != null && bindings.size() > 0) {
                  for (Iterator it=bindings.iterator(); it.hasNext();) {
                      String cbName = (String) it.next();
                  
                      statement.clearParameters();
                                          
                      statement.setLong(1, ((BasicVirtualDatabaseID)vdbID).getUID());
                      
                      modelUID = ((BasicModelID) modelID).getUID();
                      
                      statement.setLong(2, modelUID);                                   
                      statement.setString(3, cbName);
      
                      if (statement.executeUpdate() != 1){
                          throw new VirtualDatabaseException(ErrorMessageKeys.JDBCW_0009, RuntimeMetadataPlugin.Util.getString(ErrorMessageKeys.JDBCW_0009, vdbID.getName()) );
                      }
                  }
              } else {
                  // if no bindings for the model, then still insert a row
                  // that maps the vdb to the model
                    statement.clearParameters();
                  
                    statement.setLong(1, ((BasicVirtualDatabaseID)vdbID).getUID());
                    
                    modelUID = ((BasicModelID) modelID).getUID();
                    
                    statement.setLong(2, modelUID);                                   
                    statement.setNull(3, java.sql.Types.VARCHAR);
                    
                    if (statement.executeUpdate() != 1){
                      throw new VirtualDatabaseException(ErrorMessageKeys.JDBCW_0009, RuntimeMetadataPlugin.Util.getString(ErrorMessageKeys.JDBCW_0009, vdbID.getName()) );
                    }
                  
              }
          }
      }catch (SQLException se){
          throw new VirtualDatabaseException(se, ErrorMessageKeys.JDBCW_0002, RuntimeMetadataPlugin.Util.getString(ErrorMessageKeys.JDBCW_0002, sql) );
      }finally {
          if ( statement != null ) {
              try {
                  statement.close();
              } catch ( SQLException e ) {
                  I18nLogManager.logError(LogRuntimeMetadataConstants.CTX_RUNTIME_METADATA, ErrorMessageKeys.GEN_0008, e);
              }
          }
      }
  }
  
  
  /** 
   * This method is used for Non-Multisource Models because the 1-to-1
   * relationship between the RT_MDLS and the RT_VDB_MDLS tables is
   * assumed intact (as originally done) so that only an update is performed
   */
  public static void updatetSingleConnectorBindingName(VirtualDatabaseID vdbID, Map modelAndCBNames, Connection jdbcConnection) throws VirtualDatabaseException{
      Collection modelIDs = modelAndCBNames.keySet();
      PreparedStatement statement = null;
      String sql = null;          

      try{
          sql = JDBCTranslator.UPDATE_CONNECTOR_BINGING_NAME;
          statement = jdbcConnection.prepareStatement(sql);
          Iterator iter = modelIDs.iterator();
          BasicModelID modelID;
          while(iter.hasNext()){
              modelID = (BasicModelID)iter.next();
              
              String cbName = null;
              List cbNames  = (List) modelAndCBNames.get(modelID);
              if (cbNames != null && !cbNames.isEmpty()) {
                  cbName = (String) cbNames.get(0);
              }
//              String cbName = (String) modelAndCBNames.get(modelID);
              
                  statement.clearParameters();
                  if (cbName == null) {
                      statement.setNull(1, java.sql.Types.VARCHAR);
                  } else {
                      statement.setString(1, cbName);
                  }

                  statement.setLong(2, ((BasicVirtualDatabaseID)vdbID).getUID());
                  statement.setLong(3, modelID.getUID());

  
                  if (statement.executeUpdate() != 1){
                      throw new VirtualDatabaseException(ErrorMessageKeys.JDBCW_0004, RuntimeMetadataPlugin.Util.getString(ErrorMessageKeys.JDBCW_0002, new Object[]{sql} ) );
                  }    
          }
      }catch (SQLException se){
          throw new VirtualDatabaseException(se, ErrorMessageKeys.JDBCW_0002, RuntimeMetadataPlugin.Util.getString(ErrorMessageKeys.JDBCW_0002, sql) );
      }finally {
          if ( statement != null ) {
              try {
                  statement.close();
              } catch ( SQLException e ) {
                  I18nLogManager.logError(LogRuntimeMetadataConstants.CTX_RUNTIME_METADATA, ErrorMessageKeys.GEN_0008, e);
              }
          }
      }
  }
    
  
//  public static void setMultiSourcedConnectorBindingNames(VirtualDatabaseID vdbID, Map modelAndCBNames, Connection jdbcConnection) throws VirtualDatabaseException{
//      Collection modelIDs = modelAndCBNames.keySet();
//      PreparedStatement statement = null;
//      String sql = null;
//             
//
//      try{
//          sql = JDBCTranslator.UPDATE_CONNECTOR_BINGING_NAME;
//          statement = jdbcConnection.prepareStatement(sql);
//          Iterator iter = modelIDs.iterator();
//          BasicModelID modelID;
//          while(iter.hasNext()){
//              modelID = (BasicModelID)iter.next();
//              
//              List cbNames = (List) modelAndCBNames.get(modelID);
//              for (Iterator it=cbNames.iterator(); it.hasNext();) {
//                  String name =(String) it.next();
//              
//                  statement.clearParameters();
//                  statement.setString(1, name);
//                  statement.setLong(2, ((BasicVirtualDatabaseID)vdbID).getUID());
//                  statement.setLong(3, modelID.getUID());
//  
//                  if (statement.executeUpdate() != 1){
//                      throw new VirtualDatabaseException(ErrorMessageKeys.JDBCW_0004, RuntimeMetadataPlugin.Util.getString(ErrorMessageKeys.JDBCW_0002, new Object[]{sql} ) );
//                  }
//              }
//          }
//      }catch (SQLException se){
//          I18nLogManager.logError(LogRuntimeMetadataConstants.CTX_RUNTIME_METADATA, ErrorMessageKeys.JDBCW_0001, se, new Object[]{sql});
//          throw new VirtualDatabaseException(se, ErrorMessageKeys.JDBCW_0002, RuntimeMetadataPlugin.Util.getString(ErrorMessageKeys.JDBCW_0002, sql) );
//      }finally {
//          if ( statement != null ) {
//              try {
//                  statement.close();
//              } catch ( SQLException e ) {
//                  I18nLogManager.logError(LogRuntimeMetadataConstants.CTX_RUNTIME_METADATA, ErrorMessageKeys.GEN_0008, e);
//              }
//          }
//      }
//  }

  


//public static void setModelVisibilityLevels(VirtualDatabaseID vdbID, Map modelAndVisibilities, Connection jdbcConnection) throws VirtualDatabaseException{
//        Collection modelIDs = modelAndVisibilities.keySet();
//        PreparedStatement statement = null;
//        String sql = null;
//
//        try{
//            sql = JDBCTranslator.UPDATE_VISIBILITY_LEVELS;
//            statement = jdbcConnection.prepareStatement(sql);
//            Iterator iter = modelIDs.iterator();
//            BasicModelID modelID;
//            while(iter.hasNext()){
//                modelID = (BasicModelID)iter.next();
//                statement.clearParameters();
//                statement.setShort(1, ((Short)modelAndVisibilities.get(modelID)).shortValue());
//                statement.setLong(2, ((BasicVirtualDatabaseID)vdbID).getUID());
//                statement.setLong(3, modelID.getUID());
//
//                if (statement.executeUpdate() != 1){
//                    throw new VirtualDatabaseException(ErrorMessageKeys.JDBCW_0005, RuntimeMetadataPlugin.Util.getString(ErrorMessageKeys.JDBCW_0005, vdbID.getName()) );
//                }
//            }
//        }catch (SQLException se){
//            I18nLogManager.logError(LogRuntimeMetadataConstants.CTX_RUNTIME_METADATA, ErrorMessageKeys.JDBCW_0001, se, new Object[]{sql});
//            throw new VirtualDatabaseException(se, ErrorMessageKeys.JDBCW_0002, RuntimeMetadataPlugin.Util.getString(ErrorMessageKeys.JDBCW_0002, sql) );
//        }finally {
//            if ( statement != null ) {
//                try {
//                    statement.close();
//                } catch ( SQLException e ) {
//                    I18nLogManager.logError(LogRuntimeMetadataConstants.CTX_RUNTIME_METADATA, ErrorMessageKeys.GEN_0008, e);
//                }
//            }
//        }
//    }

    public static void updateVirtualDatabase(VirtualDatabase vdb, String userName, Connection jdbcConnection) throws VirtualDatabaseException{
        Collection attributesList = ((BasicVirtualDatabase)vdb).getUpdatedAttributesList();
        if(attributesList == null)
            return;
        PreparedStatement statement = null;
        String sql = null;

        try{
            // This method never really updated anything but the description, so the implementation has
            // been changed to do that correctly (see defect 9161).

            String oriSql = JDBCTranslator.UPDATE_VDB;
            StringBuffer attributes = new StringBuffer();
            boolean descChanged = false;
            //build attributes pares
            Iterator iter = attributesList.iterator();
            while(iter.hasNext()){
                String attributeName = (String)iter.next();
                if(attributeName.equals(VirtualDatabase.ModifiableAttributes.DESCRIPTION)){
                    attributes.append(JDBCNames.VirtualDatabases.ColumnName.DESCRIPTION);
                    attributes.append("=?,");
                    descChanged = true;
                }
            }
            int setIndex = oriSql.indexOf(JDBCReservedWords.SET) + 4;
            sql = oriSql.substring(0, setIndex) + attributes.toString() + oriSql.substring(setIndex);
            statement = jdbcConnection.prepareStatement(sql);

            int index = 0;
            if ( descChanged ) {
                statement.setString(++index, vdb.getDescription() );
            }
            statement.setString(++index, userName);
            statement.setString(++index, DateUtil.getCurrentDateAsString() );
            statement.setLong(++index, ((BasicVirtualDatabaseID)vdb.getID()).getUID());
            if (statement.executeUpdate() != 1){
                throw new VirtualDatabaseException(ErrorMessageKeys.JDBCW_0006, RuntimeMetadataPlugin.Util.getString(ErrorMessageKeys.JDBCW_0006, vdb.getName()) );
            }

        }catch (SQLException se){
            throw new VirtualDatabaseException(se, ErrorMessageKeys.JDBCW_0002, RuntimeMetadataPlugin.Util.getString(ErrorMessageKeys.JDBCW_0002, sql) );
        }finally {
            if ( statement != null ) {
                try {
                    statement.close();
                } catch ( SQLException e ) {
                    I18nLogManager.logError(LogRuntimeMetadataConstants.CTX_RUNTIME_METADATA, ErrorMessageKeys.GEN_0008, e);
                }
            }
        }
    }

    //key: old modelID
//    public static void updateVDBModels(VirtualDatabaseID vdbID, Map modelIDs, Connection jdbcConnection) throws VirtualDatabaseException{
//        PreparedStatement statement = null;
//        
//       
//        String sql = null;
//        long oldUID, newUID;
//        try{
//            sql = JDBCTranslator.UPDATE_VDB_MODELS;
//            statement = jdbcConnection.prepareStatement(sql);
//            Iterator iter = modelIDs.keySet().iterator();
//            while(iter.hasNext()){
//                statement.clearParameters();
//                Object next = iter.next();
//                oldUID = ((BasicModelID)next).getUID();
//                newUID = ((BasicModelID)modelIDs.get(next)).getUID();
//                statement.setLong(1, newUID);
//                statement.setLong(2, ((BasicVirtualDatabaseID)vdbID).getUID());
//                statement.setLong(3, oldUID);
//
//                if (statement.executeUpdate() != 1){
//                    throw new VirtualDatabaseException(ErrorMessageKeys.JDBCW_0007, RuntimeMetadataPlugin.Util.getString(ErrorMessageKeys.JDBCW_0007, vdbID.getName()) );
//                }
//            }
//        }catch (SQLException se){
//            I18nLogManager.logError(LogRuntimeMetadataConstants.CTX_RUNTIME_METADATA, ErrorMessageKeys.JDBCW_0001, se, new Object[]{sql});
//            throw new VirtualDatabaseException(se, ErrorMessageKeys.JDBCW_0002, RuntimeMetadataPlugin.Util.getString(ErrorMessageKeys.JDBCW_0002, sql) );
//        }finally {
//            if ( statement != null ) {
//                try {
//                    statement.close();
//                } catch ( SQLException e ) {
//                    I18nLogManager.logError(LogRuntimeMetadataConstants.CTX_RUNTIME_METADATA, ErrorMessageKeys.GEN_0008, e);
//                }
//            }
//        }
//    }

/**
 *  Used by migration, after deploying the latest version from the source,
 *  to update the version to reflect the latest version.  Otherwise
 *  the version will start at one when it should be greater.
 */

    public static void updateVDBVersion(VirtualDatabaseID vdbID, Connection jdbcConnection) throws VirtualDatabaseException{
        PreparedStatement statement = null;
        String sql = null;
        try{
            sql = JDBCTranslator.UPDATE_VDB_VERSION;
            statement = jdbcConnection.prepareStatement(sql);

            statement.setString(1, vdbID.getVersion());
            statement.setLong(2, ((BasicVirtualDatabaseID)vdbID).getUID());

            if (statement.executeUpdate() != 1) {
                throw new VirtualDatabaseException(ErrorMessageKeys.JDBCW_0008, RuntimeMetadataPlugin.Util.getString(ErrorMessageKeys.JDBCW_0008, vdbID.getName()) );
            }

        }catch (SQLException se){
            throw new VirtualDatabaseException(se, ErrorMessageKeys.JDBCW_0002, RuntimeMetadataPlugin.Util.getString(ErrorMessageKeys.JDBCW_0002, sql) );
        }finally {
            if ( statement != null ) {
                try {
                    statement.close();
                } catch ( SQLException e ) {
                    I18nLogManager.logError(LogRuntimeMetadataConstants.CTX_RUNTIME_METADATA, ErrorMessageKeys.GEN_0008, e);
                }
            }
        }
    }

    /**
     * Used by the renameModel method to update the MDL_NM column in the RT_MDLS table
     */
//    public static void updateModelName(final ModelID modelID, final String newModelName, final Connection jdbcConnection) throws VirtualDatabaseException{
//        PreparedStatement statement = null;
//        String sql = null;
//        try{
//            sql = JDBCTranslator.UPDATE_MODEL_NAME;
//            statement = jdbcConnection.prepareStatement(sql);
//
//            final long uid = ((BasicModelID)modelID).getUID();
//            statement.setString(1, newModelName);
//            statement.setLong(2, uid);
//
//            if (statement.executeUpdate() != 1) {
//                throw new VirtualDatabaseException(ErrorMessageKeys.JDBCW_0011, RuntimeMetadataPlugin.Util.getString(ErrorMessageKeys.JDBCW_0011, new Object[]{modelID,new Long(uid)}) );
//            }
//
//        }catch (SQLException se){
//            I18nLogManager.logError(LogRuntimeMetadataConstants.CTX_RUNTIME_METADATA, ErrorMessageKeys.JDBCW_0001, se, new Object[]{sql});
//            throw new VirtualDatabaseException(se, ErrorMessageKeys.JDBCW_0002, RuntimeMetadataPlugin.Util.getString(ErrorMessageKeys.JDBCW_0002, sql) );
//        }finally {
//            if ( statement != null ) {
//                try {
//                    statement.close();
//                } catch ( SQLException e ) {
//                    I18nLogManager.logError(LogRuntimeMetadataConstants.CTX_RUNTIME_METADATA, ErrorMessageKeys.GEN_0008, e);
//                }
//            }
//        }
//    }

    

    public static void insertVDBModels(Collection models, VirtualDatabaseID vdbID, Connection jdbcConnection) throws VirtualDatabaseException{
        Collection multiModels = new ArrayList(models.size());
        Collection singleModels = new ArrayList(models.size());
        for (Iterator it=models.iterator(); it.hasNext();) {
            Model m =(Model) it.next();
            if (m.supportsMultiSourceBindings()) {
                multiModels.add(m);
            } else {
                singleModels.add(m);
            }
        }

        if (singleModels.size() > 0) {
            insertSingleBindingVDBModels(singleModels, vdbID, jdbcConnection);
        }
        
        if (multiModels.size() > 0) {
            insertMultiSourceVDBModels(multiModels, vdbID, jdbcConnection);
        }
    }
    
    
    private static void insertSingleBindingVDBModels(Collection models, VirtualDatabaseID vdbID, Connection jdbcConnection) throws VirtualDatabaseException{
      
        PreparedStatement statement = null;
        String sql = null;

        try{
            sql = JDBCTranslator.INSERT_VDB_MODELS_WITH_BINDING;
            statement = jdbcConnection.prepareStatement(sql);
            Iterator iter = models.iterator();
            long modelUID;
            String routing = null;
            ConnectorBinding cb;
            while(iter.hasNext()){
                statement.clearParameters();
                statement.setLong(1, ((BasicVirtualDatabaseID)vdbID).getUID());
                Object next = iter.next();
                routing = null;
                if(next instanceof Model) {
                    Model m = (Model) next;
                    modelUID = ((BasicModelID)m.getID()).getUID();
                    List bindings = m.getConnectorBindingNames();
                    if (bindings != null && bindings.size() > 0) {
                         String cbName = (String) bindings.get(0);
                         cb = CurrentConfiguration.getInstance().getConfiguration().getConnectorBinding(cbName);
                         if (cb != null) {
                             routing = cb.getRoutingUUID();
                         }
                    }
                } else {
                     modelUID = ((BasicModelID)next).getUID();
                }
                statement.setLong(2, modelUID);
                
                if (routing == null) {
                    statement.setNull(3, java.sql.Types.VARCHAR);
                } else {
                    statement.setString(3, routing);
                }                

                if (statement.executeUpdate() != 1){
                    throw new VirtualDatabaseException(ErrorMessageKeys.JDBCW_0009, RuntimeMetadataPlugin.Util.getString(ErrorMessageKeys.JDBCW_0009, vdbID.getName()) );
                }
            }
        
        }catch (Exception se){
            throw new VirtualDatabaseException(se, ErrorMessageKeys.JDBCW_0002, RuntimeMetadataPlugin.Util.getString(ErrorMessageKeys.JDBCW_0002, sql) );
//        } catch (ConfigurationException err) {
            
        }finally {
            if ( statement != null ) {
                try {
                    statement.close();
                } catch ( SQLException e ) {
                    I18nLogManager.logError(LogRuntimeMetadataConstants.CTX_RUNTIME_METADATA, ErrorMessageKeys.GEN_0008, e);
                }
            }
        }
    }
    
    private static void insertMultiSourceVDBModels(Collection models, VirtualDatabaseID vdbID, Connection jdbcConnection) throws VirtualDatabaseException{
        PreparedStatement statement = null;
        String sql = null;

        try{
            sql = JDBCTranslator.INSERT_VDB_MODELS_WITH_BINDING;
            statement = jdbcConnection.prepareStatement(sql);
            Iterator iter = models.iterator();
            long modelUID;
            while(iter.hasNext()){
                
                Model m = (Model) iter.next();
                Collection bindings = m.getConnectorBindingNames();
                if (bindings != null && bindings.size() > 0) {

                    for (Iterator it=bindings.iterator(); it.hasNext();) {
                        String cbName = (String) it.next();
                        
                        ConnectorBinding cb = CurrentConfiguration.getInstance().getConfiguration().getConnectorBinding(cbName);
                        
                        String routing = null;
                        if (cb != null) {
                            routing = cb.getRoutingUUID();
                        }
                        
                        statement.clearParameters();
                                            
                        statement.setLong(1, ((BasicVirtualDatabaseID)vdbID).getUID());
                        
                        modelUID = ((BasicModelID) m.getID()).getUID();
                        
                        statement.setLong(2, modelUID);
                        
                        if (routing == null) {
                            statement.setNull(3, java.sql.Types.VARCHAR);
                        } else {
                            statement.setString(3, routing);
                        }  
        
                        if (statement.executeUpdate() != 1){
                            throw new VirtualDatabaseException(ErrorMessageKeys.JDBCW_0009, RuntimeMetadataPlugin.Util.getString(ErrorMessageKeys.JDBCW_0009, vdbID.getName()) );
                        }
                    }
                } else {
                    // if no bindings for the model, then still insert a row
                    // that maps the vdb to the model
                      statement.clearParameters();
                    
                      statement.setLong(1, ((BasicVirtualDatabaseID)vdbID).getUID());

                      modelUID = ((BasicModelID) m.getID()).getUID();                      
                      
                      statement.setLong(2, modelUID);                                   
                      statement.setNull(3, java.sql.Types.VARCHAR);
                      
                      if (statement.executeUpdate() != 1){
                        throw new VirtualDatabaseException(ErrorMessageKeys.JDBCW_0009, RuntimeMetadataPlugin.Util.getString(ErrorMessageKeys.JDBCW_0009, vdbID.getName()) );
                      }
                    
                }
            }
        }catch (Exception se){
            throw new VirtualDatabaseException(se, ErrorMessageKeys.JDBCW_0002, RuntimeMetadataPlugin.Util.getString(ErrorMessageKeys.JDBCW_0002, sql) );
        }finally {
            if ( statement != null ) {
                try {
                    statement.close();
                } catch ( SQLException e ) {
                    I18nLogManager.logError(LogRuntimeMetadataConstants.CTX_RUNTIME_METADATA, ErrorMessageKeys.GEN_0008, e);
                }
            }
        }
    }
    

    /**
     * Delete VDB-models from the <code>VirtualDatabase</code>.
     * @param modelIDs is a collection of model IDs to be deleted.
     * @param vdbID is the ID of the virtual database.
     * @exception VirtualDatabaseException if unable to perform insertion.
     * @throws InvalidStateException is thrown if the VirtualDatabase is not in the proper state to change to active state.
     */
    public static void deleteVDBModels(Collection modelIDs, VirtualDatabaseID vdbID, Connection jdbcConnection) throws VirtualDatabaseException {
        PreparedStatement statement = null;
        String sql = null;
        Iterator iter = modelIDs.iterator();
        try{
            sql = JDBCTranslator.DELETE_VDB_MODEL;
            statement = jdbcConnection.prepareStatement(sql);
            while(iter.hasNext()){
                statement.clearParameters();
                final BasicModelID modelID = (BasicModelID)iter.next();
                final BasicVirtualDatabaseID basicVdbID = (BasicVirtualDatabaseID) vdbID;
                statement.setLong(1, basicVdbID.getUID());
                statement.setLong(2, modelID.getUID());
                statement.executeUpdate();
//                if (statement.executeUpdate() >= 1) {
//                    throw new VirtualDatabaseException(ErrorMessageKeys.JDBCC_0024, RuntimeMetadataPlugin.Util.getString(ErrorMessageKeys.JDBCC_0024, modelID ) );
//                }
            } // end while
        }catch (SQLException se){
            throw new VirtualDatabaseException(se, ErrorMessageKeys.JDBCC_0003, RuntimeMetadataPlugin.Util.getString(ErrorMessageKeys.JDBCC_0003, sql) );
        }finally {
            if ( statement != null ) {
                try {
                    statement.close();
                } catch ( SQLException e ) {
                    I18nLogManager.logError(LogRuntimeMetadataConstants.CTX_RUNTIME_METADATA, ErrorMessageKeys.GEN_0008, e);
                }
            }
        }
    }
   


}
