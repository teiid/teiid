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

package com.metamatrix.common.extensionmodule.spi.jdbc;

import java.io.ByteArrayInputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.CommonPlugin;
import com.metamatrix.common.extensionmodule.ExtensionModuleDescriptor;
import com.metamatrix.common.extensionmodule.exception.DuplicateExtensionModuleException;
import com.metamatrix.common.extensionmodule.exception.ExtensionModuleNotFoundException;
import com.metamatrix.common.extensionmodule.exception.ExtensionModuleOrderingException;
import com.metamatrix.common.id.dbid.DBIDGenerator;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.util.ErrorMessageKeys;
import com.metamatrix.common.util.LogCommonConstants;
import com.metamatrix.core.util.DateUtil;
import com.metamatrix.core.util.StringUtil;



public class JDBCExtensionModuleWriter {

        private static final String CONTEXT = LogCommonConstants.CTX_EXTENSION_SOURCE_JDBC;


        private static final int MAX_DESC_LEN = 4000;

        private static String IS_TRUE = "1"; //$NON-NLS-1$
        private static String IS_FALSE = "0"; //$NON-NLS-1$

    /**
     * Adds an extension module to the end of the list of modules
     * @param principalName name of principal requesting this addition
     * @param type one of the known types of extension file
     * @param sourceName name (e.g. filename) of extension module
     * @param data actual contents of module
     * @param checksum Checksum of file contents
     * @param description (optional) description of the extension module
     * @param enabled indicates whether each extension module is enabled for
     * being searched or not (for convenience, a module can be disabled
     * without being removed)
     * @return ExtensionModuleDescriptor describing the newly-added
     * extension module
     * @throws DuplicateExtensionModuleException if an extension module
     * with the same sourceName already exists
     * @throws MetaMatrixComponentException indicating a non-business-related
     * exception (such as a communication exception)
     */
    public static ExtensionModuleDescriptor addSource(String principalName, String type, String sourceName,
                byte[] data,
                long checksum,
                String description,
                boolean enabled,
                Connection jdbcConnection)
    throws DuplicateExtensionModuleException, MetaMatrixComponentException{

        String date = DateUtil.getCurrentDateAsString();
        return JDBCExtensionModuleWriter.addSource(principalName, date, principalName, date,
                                                  type, sourceName,
                                                  data,
                                                  checksum,
                                                  description,
                                                  enabled,
                                                  jdbcConnection);

        
    }


    /**
     * Adds an extension module to the end of the list of modules
     * @param createdBy name of principal requesting this addition
     * @param createdDate is the date created
     * @param updatedBy name is the principal that is doing the updating
     * @param updatedDate is the date the source is updated
     * @param type one of the known types of extension file
     * @param sourceName name (e.g. filename) of extension module
     * @param data actual contents of module
     * @param checksum Checksum of file contents
     * @param description (optional) description of the extension module
     * @param enabled indicates whether each extension module is enabled for
     * being searched or not (for convenience, a module can be disabled
     * without being removed)
     * @return ExtensionModuleDescriptor describing the newly-added
     * extension module
     * @throws DuplicateExtensionModuleException if an extension module
     * with the same sourceName already exists
     * @throws MetaMatrixComponentException indicating a non-business-related
     * exception (such as a communication exception)
     */
        
        
    public static ExtensionModuleDescriptor addSource(String createdBy, String createdDate,
                                                  String updatedBy, String updatedDate,
                                                  String type, String sourceName,
                                                  byte[] data,
                                                  long checksum,
                                                  String description,
                                                  boolean enabled, 
                                                  Connection jdbcConnection)
                                      throws DuplicateExtensionModuleException, MetaMatrixComponentException{
                                  LogManager.logTrace(CONTEXT, "adding extension module  " + sourceName + " containing # bytes: " + data.length); //$NON-NLS-1$ //$NON-NLS-2$


        PreparedStatement statement = null;
        String sql = JDBCExtensionModuleTranslator.ADD_SOURCE_FILE_DATA;
        ExtensionModuleDescriptor descriptor = null;

        description = StringUtil.truncString(description, MAX_DESC_LEN);

        int position = getNextPosition(jdbcConnection);
        
        boolean orignalAutocommit=true;
        boolean firstException = false;
        try{
            long longUID = DBIDGenerator.getInstance().getID(JDBCNames.ExtensionFilesTable.TABLE_NAME);
            
            orignalAutocommit=jdbcConnection.getAutoCommit();
            jdbcConnection.setAutoCommit(false);

            // Oracle specific handling
                sql = sql + JDBCExtensionModuleTranslator.ADD_SOURCE_FILE_DATA_PARAMS;
                statement = jdbcConnection.prepareStatement(sql);

                statement.setLong(1, longUID);
                statement.setLong(2, checksum);
                statement.setString(3, sourceName);
                statement.setInt(4, position);

                // vah - 8-17-03 DB2 driver does not handle
                // the conversion of boolean (true or false) to
                // char (1 or 0) like the oracle driver.
                statement.setString(5, enabled ? IS_TRUE : IS_FALSE);

                statement.setString(6, description);
                statement.setString(7, createdBy);
                statement.setString(8, createdDate);
                statement.setString(9, updatedBy);
                statement.setString(10, updatedDate);
                statement.setString(11, type);
                
                
                if (statement.executeUpdate() != 1){
                    if (JDBCExtensionModuleReader.isNameInUse(sourceName, jdbcConnection)){
                        throw new DuplicateExtensionModuleException(CommonPlugin.Util.getString(ErrorMessageKeys.EXTENSION_0053, sourceName));
                    }
                    throw new MetaMatrixComponentException(ErrorMessageKeys.EXTENSION_0054, CommonPlugin.Util.getString(ErrorMessageKeys.EXTENSION_0054, sourceName,sql));
                }
                
            updateFile(updatedBy, sourceName, data, checksum, jdbcConnection, JDBCExtensionModuleTranslator.UPDATE_SOURCE_FILE_DATA_ORACLE);
            
            jdbcConnection.commit();

        } catch (SQLException se){
            firstException = true;
            //check if name is in use
            
            try {
                jdbcConnection.rollback();
            } catch (SQLException re) {
                // do nothing.
            }

            if (JDBCExtensionModuleReader.isNameInUse(sourceName, jdbcConnection)){
                throw new DuplicateExtensionModuleException(sourceName);
            }
            throw new MetaMatrixComponentException(se, ErrorMessageKeys.EXTENSION_0054, CommonPlugin.Util.getString(ErrorMessageKeys.EXTENSION_0054, sourceName,sql));

        } catch ( DuplicateExtensionModuleException e ) {
            firstException = true;
            throw e;
        } catch ( MetaMatrixComponentException e ) {
            firstException = true;
            throw e;
        } catch (Exception e) {
            firstException = true;
            throw new MetaMatrixComponentException(e, ErrorMessageKeys.EXTENSION_0054, CommonPlugin.Util.getString(ErrorMessageKeys.EXTENSION_0054, sourceName,sql));
        } finally {
            if ( statement != null ) {
                try {
                    statement.close();
                    statement = null;
                } catch ( SQLException e ) {
                    LogManager.logWarning(CONTEXT, e, CommonPlugin.Util.getString(ErrorMessageKeys.EXTENSION_0048));
                }
            }
            
            try {
                jdbcConnection.setAutoCommit(orignalAutocommit);
            } catch (SQLException e) {
                if (!firstException) {
                    throw new MetaMatrixComponentException(e, ErrorMessageKeys.EXTENSION_0054, CommonPlugin.Util.getString(ErrorMessageKeys.EXTENSION_0054, sourceName,sql));
                }
            	LogManager.logDetail(CONTEXT, e, CommonPlugin.Util.getString(ErrorMessageKeys.EXTENSION_0054, new Object[]{sourceName, sql}));
            }
        }
        
        descriptor = new ExtensionModuleDescriptor(sourceName,type,position,enabled,description,createdBy,createdDate,updatedBy,updatedDate,checksum);

        LogManager.logTrace(CONTEXT, "success! " + sourceName ); //$NON-NLS-1$
        return descriptor;
    }

    public static void setSource(String principalName, String sourceName, byte[] data, long checksum, Connection jdbcConnection)
    throws ExtensionModuleNotFoundException, MetaMatrixComponentException{
        updateFile(principalName, sourceName, data, checksum, jdbcConnection, JDBCExtensionModuleTranslator.UPDATE_SOURCE_FILE_DATA_ORACLE);
    }

    private static void updateFile(String principalName, String sourceName, byte[] data, long checksum, Connection jdbcConnection, String sql)
    throws ExtensionModuleNotFoundException, MetaMatrixComponentException{

        LogManager.logTrace(CONTEXT, "setting extension module file " + sourceName + " containing # bytes: " + data.length); //$NON-NLS-1$ //$NON-NLS-2$

        PreparedStatement statement = null;
        ResultSet results = null;

        try{
            sql = JDBCExtensionModuleTranslator.UPDATE_SOURCE_FILE_DATA_DEFAULT;
            statement = jdbcConnection.prepareStatement(sql);

            statement.setString(1, principalName);
            statement.setString(2, DateUtil.getCurrentDateAsString());
            statement.setLong(3, checksum);
            
            statement.setBinaryStream(4, new ByteArrayInputStream(data), data.length);
            
            // DBstatement.setBytes(4, data);
            statement.setString(5, sourceName);

            if (statement.executeUpdate() != 1){
                if (!JDBCExtensionModuleReader.isNameInUse(sourceName, jdbcConnection)){
                    throw new ExtensionModuleNotFoundException(CommonPlugin.Util.getString(ErrorMessageKeys.EXTENSION_0059, sourceName));
                }
                throw new MetaMatrixComponentException(ErrorMessageKeys.EXTENSION_0065, CommonPlugin.Util.getString(ErrorMessageKeys.EXTENSION_0065, sql));
            }                
        } catch (SQLException se){

            //check if name is in use
            if (!JDBCExtensionModuleReader.isNameInUse(sourceName, jdbcConnection)){
                throw new ExtensionModuleNotFoundException(sourceName);
            }
            throw new MetaMatrixComponentException(se, ErrorMessageKeys.EXTENSION_0065, CommonPlugin.Util.getString(ErrorMessageKeys.EXTENSION_0065, sql));

        } finally {
            if ( statement != null ) {
                try {
                    statement.close();
                    statement = null;
                } catch ( SQLException e ) {
                    LogManager.logWarning(CONTEXT, e, CommonPlugin.Util.getString(ErrorMessageKeys.EXTENSION_0048));
                }
            }
            close(results);   
            
        }

    }
    
    
    /**
     * Updates the indicated extension module's source name
     * @param principalName name of principal requesting this addition
     * @param sourceName name (e.g. filename) of extension module
     * @param newName new name for the module
     * @return ExtensionModuleDescriptor describing the newly-updated
     * extension module
     * @throws ExtensionModuleNotFoundException if no extension module with
     * name sourceName can be found
     * @throws MetaMatrixComponentException indicating a non-business-related
     * exception (such as a communication exception)
     */
    public static ExtensionModuleDescriptor setSourceName(String principalName, String sourceName, String newName, Connection jdbcConnection)
    throws ExtensionModuleNotFoundException, MetaMatrixComponentException{
        try {
            String sql = JDBCExtensionModuleTranslator.UPDATE_SOURCE_NAME;
            updateModule(sourceName, newName, sql, principalName, jdbcConnection);
            
            return JDBCExtensionModuleReader.getSourceDescriptor(sourceName, jdbcConnection);
            
        } catch ( SQLException e ) {
              throw new MetaMatrixComponentException(e);
        }
            
    }

    /**
     * Updates the indicated extension module's description
     * @param principalName name of principal requesting this addition
     * @param sourceName name (e.g. filename) of extension module
     * @param description (optional) description of the extension module.
     * <code>null</code> can be passed in to indicate no description.
     * @return ExtensionModuleDescriptor describing the newly-updated
     * extension module
     * @throws ExtensionModuleNotFoundException if no extension module with
     * name sourceName can be found
     * @throws MetaMatrixComponentException indicating a non-business-related
     * exception (such as a communication exception)
     */
    public static ExtensionModuleDescriptor setSourceDescription(String principalName, String sourceName, String description, Connection jdbcConnection)
    throws ExtensionModuleNotFoundException, MetaMatrixComponentException{       
        
        try {
            String sql = JDBCExtensionModuleTranslator.UPDATE_SOURCE_DESCRIPTION;
            updateModule(sourceName, description, sql, principalName, jdbcConnection);
            
            return JDBCExtensionModuleReader.getSourceDescriptor(sourceName, jdbcConnection);
            
        } catch ( SQLException e ) {
              throw new MetaMatrixComponentException(e);
        }
        
    }




        /**
     * Sets the positions in the search order of all modules (all modules
     * must be included or an ExtensionModuleOrderingException will be thrown)
     * The sourceNames List parameter should indicate the new desired order.
     * @param principalName name of principal requesting this addition
     * @param sourceNames Collection of String names of existing
     * extension modules whose search position is to be set
     * @throws ExtensionModuleOrderingException if the extension files could
     * not be ordered as requested because another administrator had
     * concurrently added or removed an extension file or files, or because
     * an indicated position is out of bounds.
     * @throws MetaMatrixComponentException indicating a non-business-related
     * exception (such as a communication exception)
     */
    public static void setSearchOrder(String principalName, List sourceNames, Connection jdbcConnection)
    throws ExtensionModuleOrderingException, MetaMatrixComponentException{
        int rowCount = JDBCExtensionModuleReader.getExtensionModuleCount(jdbcConnection);
        if (sourceNames.size() != rowCount){
            throw new ExtensionModuleOrderingException(ErrorMessageKeys.EXTENSION_0066, CommonPlugin.Util.getString(ErrorMessageKeys.EXTENSION_0066, sourceNames));
        }

        String aSourceName = null;
        Integer aPosition = null;
        String currentDate = DateUtil.getCurrentDateAsString();

        String sql = JDBCExtensionModuleTranslator.UPDATE_SOURCE_SEARCH_POSITION;
        PreparedStatement statement=null;

        try {
            statement = jdbcConnection.prepareStatement(sql);

            for (ListIterator iter = sourceNames.listIterator(); iter.hasNext(); ){
    
                aPosition = new Integer(iter.nextIndex());
                aSourceName = (String)iter.next();
                
                
                statement.setInt(1,  aPosition.intValue());
                statement.setString(2, principalName); 
                statement.setString(3, currentDate); 
                  
                statement.setString(4, aSourceName);
                            
               
                statement.execute();
                
                statement.clearParameters();
            }
               
        } catch ( SQLException e ) {
            if ( statement != null ) {
                try {
                    statement.close();
                } catch ( SQLException ex ) {
                }
            }
            
              throw new MetaMatrixComponentException(e);
        }        

        
    }

    /**
     * Sets the "enabled" (for searching) property of all of the indicated
     * extension modules.
     * @param principalName name of principal requesting this addition
     * @param sourceNames Collection of String names of existing
     * extension modules whose "enabled" status is to be set
     * @param enabled indicates whether each extension module is enabled for
     * being searched or not (for convenience, a module can be disabled
     * without being removed)
     * @throws ExtensionModuleNotFoundException if no extension module with
     * name sourceName can be found
     * @throws MetaMatrixComponentException indicating a non-business-related
     * exception (such as a communication exception)
     */
    public static void setEnabled(String principalName, Collection sourceNames, boolean enabled, Connection jdbcConnection)
    throws ExtensionModuleNotFoundException, MetaMatrixComponentException{

        String aSourceName = null;
        String currentDate = DateUtil.getCurrentDateAsString();
        String sql = JDBCExtensionModuleTranslator.UPDATE_SOURCE_SEARCH_POSITION;

        PreparedStatement statement = null;
        try {
            statement = jdbcConnection.prepareStatement(sql);

            for (Iterator iter = sourceNames.iterator(); iter.hasNext(); ){
    
                aSourceName = (String)iter.next();
                
                statement.setString(1, enabled ? IS_TRUE : IS_FALSE);
                
                statement.setString(2, principalName); 
                statement.setString(3, currentDate); 
                  
                statement.setString(4, aSourceName);
                            
               
                statement.execute();
                
                statement.clearParameters();
               
                
            }
        } catch ( SQLException e ) {
            if ( statement != null ) {
                try {
                    statement.close();
                } catch ( SQLException ex ) {
                }
            }            
              throw new MetaMatrixComponentException(e);
        }        
        
        
     }

    private static void updateModule(String filenname, String value, String sql, String callerPrincipalName, Connection jdbcConnection) throws SQLException{

        
          PreparedStatement statement = null;
          try{
              statement = jdbcConnection.prepareStatement(sql);
            
              statement.setString(1, value);
              statement.setString(2, callerPrincipalName); 
              statement.setString(3, DateUtil.getCurrentDateAsString()); 
              
              statement.setString(4, filenname);
                        
           
              statement.execute();


          } finally {
              if (statement != null) {
                  try {
                       statement.close();
                       statement = null;
                  } catch ( SQLException e ) {
                      LogManager.logWarning(CONTEXT, e, CommonPlugin.Util.getString(ErrorMessageKeys.EXTENSION_0048));
                  }
              }
          }                              
        
        
      }      
    
    /**
     * Deletes a module from the list of modules
     * @param principalName name of principal requesting this addition
     * @param sourceName name (e.g. filename) of extension module
     * @throws ExtensionModuleNotFoundException if no extension module with
     * name sourceName can be found
     * @throws MetaMatrixComponentException indicating a non-business-related
     * exception (such as a communication exception)
     */
    public static void removeSource(String principalName, String sourceName, Connection jdbcConnection)
    throws ExtensionModuleNotFoundException, MetaMatrixComponentException{

        PreparedStatement statement = null;
        try{
            statement = jdbcConnection.prepareStatement(JDBCExtensionModuleTranslator.DELETE_SOURCE);
            
            statement.setString(1, sourceName);
           
            statement.execute();

        } catch ( SQLException e ) {
            throw new MetaMatrixComponentException(e);

        } finally {
            if (statement != null) {
                try {
                     statement.close();
                     statement = null;
                } catch ( SQLException e ) {
                    LogManager.logWarning(CONTEXT, e, CommonPlugin.Util.getString(ErrorMessageKeys.EXTENSION_0048));
                }
            }
        }                              

    }

    static int getNextPosition(Connection jdbcConnection) throws MetaMatrixComponentException{
        int nextPos = JDBCExtensionModuleReader.executeIntFunctionSQL(JDBCExtensionModuleTranslator.SELECT_MAX_SEARCH_POSITION, jdbcConnection);
        nextPos++;
        return nextPos;
    }



    
    private static void close(ResultSet resultset) {
        if (resultset != null) {
            try {
                resultset.close();
                resultset = null;
            } catch (SQLException e) {
                LogManager.logWarning(CONTEXT, e, CommonPlugin.Util.getString(ErrorMessageKeys.EXTENSION_0048));}
        }
    }
    
    
   


}
