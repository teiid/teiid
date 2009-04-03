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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.StringTokenizer;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.CommonPlugin;
import com.metamatrix.common.config.CurrentConfiguration;
import com.metamatrix.common.extensionmodule.ExtensionModuleDescriptor;
import com.metamatrix.common.extensionmodule.exception.ExtensionModuleNotFoundException;
import com.metamatrix.common.jdbc.JDBCPlatform;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.util.CommonPropertyNames;
import com.metamatrix.common.util.ErrorMessageKeys;
import com.metamatrix.common.util.LogCommonConstants;


/**
 * Utility class that loads extension modules and other files from table 
 * <code>ExtensionFilesTable.TABLE_NAME</code>.
 *
 */
public class JDBCExtensionModuleReader {
    private static final String CONTEXT = LogCommonConstants.CTX_EXTENSION_SOURCE_JDBC;

    private static FileCache fileCache = null;
    
    
    /**
     * Initialize the cache of byte[]s.
     * The initialization is only performed once (when fileCache is null). 
     * 
     * @since 4.2
     */
    private static void initFileCache() {
        CurrentConfiguration config = CurrentConfiguration.getInstance();
        if (config.isAvailable() && fileCache == null) {
            fileCache = new FileCache();
            String typesToCacheString = config.getProperties().getProperty(CommonPropertyNames.EXTENSION_TYPES_TO_CACHE);    
            if (typesToCacheString != null) {
                StringTokenizer tokenizer = new StringTokenizer(typesToCacheString, ","); //$NON-NLS-1$
                while (tokenizer.hasMoreTokens()) {
                    String type = tokenizer.nextToken().trim();
                    fileCache.addTypeToCache(type);
                }
            }
        }
    }
    
    
    
    /**
     * Returns List (of Strings) of all extension module names, in order of
     * their search ordering
     * @return List (of Strings) of all extension module names, in order of
     * their search ordering
     * @throws MetaMatrixComponentException indicating a non-business-related
     * exception (such as a communication exception)
     */
    public static List getSourceNames(Connection jdbcConnection) throws MetaMatrixComponentException{
        PreparedStatement statement = null;
        ResultSet results = null;
        String sql = JDBCExtensionModuleTranslator.SELECT_ALL_SOURCE_NAMES;
        List sourceNames = new ArrayList();

        try{

            statement = jdbcConnection.prepareStatement(sql);

            if ( ! statement.execute() ) {
                throw new MetaMatrixComponentException(ErrorMessageKeys.EXTENSION_0046, CommonPlugin.Util.getString(ErrorMessageKeys.EXTENSION_0046, sql));
            }
            results = statement.getResultSet();

            while (results.next()) {
                sourceNames.add(results.getString(JDBCNames.ExtensionFilesTable.ColumnName.FILE_NAME));
            }
        } catch (SQLException se){
            throw new MetaMatrixComponentException(se, ErrorMessageKeys.EXTENSION_0047, CommonPlugin.Util.getString(ErrorMessageKeys.EXTENSION_0047, sql));
        } finally {
            close(results);
            close(statement);
        }
        return sourceNames;
    }

    /**
     * Retrieves an extension module in byte[] form
     * @param sourceName name (e.g. filename) of extension module
     * @return actual contents of module in byte[] array form
     * @throws ExtensionModuleNotFoundException if no extension module with
     * name sourceName can be found
     * @throws MetaMatrixComponentException indicating a non-business-related
     * exception (such as a communication exception)
     */
    public static byte[] getSource(String sourceName, Connection jdbcConnection)
    throws ExtensionModuleNotFoundException, MetaMatrixComponentException, SQLException {

        return getFileContent(sourceName, jdbcConnection);
    }
    
    /**
     * Get file resource.  Caches files in memory.  Checks memory cache by checksum.  
     * If it's not in the cache or the checksum has changed, load from the DB.
     * @param sourceName
     * @param jdbcConnection
     * @return
     * @throws ExtensionModuleNotFoundException
     * @throws MetaMatrixComponentException
     * @throws SQLException
     * @since 4.2
     */
    private synchronized static byte[] getFileContent(String sourceName, Connection jdbcConnection)
    throws ExtensionModuleNotFoundException, MetaMatrixComponentException, SQLException {

        initFileCache();        
        
        CheckSumAndType csat = loadChecksumAndType(sourceName, jdbcConnection);
        long checksum = csat.getChecksum();
        String type = csat.getType();
        byte[] bytes = null;
        if (fileCache != null) {
	        long cachedChecksum = fileCache.getChecksum(sourceName);
	        
	        if (cachedChecksum == checksum) {
	            bytes = fileCache.getBytes(sourceName);
	        }
        }
        
        if (bytes == null) {
            bytes = loadBytes(sourceName, jdbcConnection);
            if (fileCache != null) {
            	fileCache.put(sourceName, checksum, bytes, type);
            }
        }
        
        return bytes;
                
    }
        
    
    
        
    private static CheckSumAndType loadChecksumAndType(String sourceName, Connection jdbcConnection) throws ExtensionModuleNotFoundException,
        MetaMatrixComponentException, SQLException {
        CheckSumAndType result = new CheckSumAndType();
        
        String sql = null;
        PreparedStatement statement = null;
        ResultSet results = null;
        sql = JDBCExtensionModuleTranslator.SELECT_SOURCE_CHECKSUM_AND_TYPE_BY_NAME;

        try {
            statement = jdbcConnection.prepareStatement(sql);
            statement.setString(1, sourceName);

            if (!statement.execute()) {
                throw new MetaMatrixComponentException(ErrorMessageKeys.EXTENSION_0046, CommonPlugin.Util.getString(
                    ErrorMessageKeys.EXTENSION_0046, sql));
            }
            results = statement.getResultSet();
             
            if (results.next()) {
                result.setChecksum(results.getLong(JDBCNames.ExtensionFilesTable.ColumnName.CHECKSUM));
                result.setType(results.getString(JDBCNames.ExtensionFilesTable.ColumnName.FILE_TYPE));
            } else {
                throw new ExtensionModuleNotFoundException(sourceName);
            }
        } catch (SQLException se) {
            throw se;
        } catch (ExtensionModuleNotFoundException e) {
            throw e;
        } catch (MetaMatrixComponentException e) {
            throw e;
        } catch (Exception e) {
            throw new MetaMatrixComponentException(e, ErrorMessageKeys.EXTENSION_0047, CommonPlugin.Util.getString(
                ErrorMessageKeys.EXTENSION_0047, sql));
        } finally {
            close(results);
            close(statement);
        }

        return result;
    }

        
    private static byte[] loadBytes(String sourceName, Connection jdbcConnection) 
    throws ExtensionModuleNotFoundException, MetaMatrixComponentException, SQLException {
        
        String sql = null;
        PreparedStatement statement = null;
        ResultSet results = null;
        sql = JDBCExtensionModuleTranslator.SELECT_SOURCE_FILE_DATA_BY_NAME;

        Object dataObj = null;
        try{

            statement = jdbcConnection.prepareStatement(sql);
            statement.setString(1, sourceName);

            if ( ! statement.execute() ) {
                throw new MetaMatrixComponentException(ErrorMessageKeys.EXTENSION_0046, CommonPlugin.Util.getString(ErrorMessageKeys.EXTENSION_0046, sql));
            }
            results = statement.getResultSet();

            if (results.next()) {
                dataObj = results.getObject(JDBCNames.ExtensionFilesTable.ColumnName.FILE_CONTENTS);
            } else {
                throw new ExtensionModuleNotFoundException(sourceName);
            }
        } catch (SQLException se){
            throw se;
        } catch ( ExtensionModuleNotFoundException e ) {
            throw e;
        } catch ( MetaMatrixComponentException e ) {
            throw e;
        } catch (Exception e) {
            throw new MetaMatrixComponentException(e, ErrorMessageKeys.EXTENSION_0047, CommonPlugin.Util.getString(ErrorMessageKeys.EXTENSION_0047, sql));
        } finally {
            close(results);
            close(statement);
        }

        byte[] data = null;
        try{
            data = JDBCPlatform.convertToByteArray(dataObj);
            
        } catch (Exception e) {
            throw new MetaMatrixComponentException(e, ErrorMessageKeys.EXTENSION_0049, CommonPlugin.Util.getString(ErrorMessageKeys.EXTENSION_0049));
        }

        return data;
    }
    

    /**
     * Returns the ExtensionModuleDescriptor object for the extension
     * module indicated by sourceName
     * @param sourceName name (e.g. filename) of extension module
     * @return the ExtensionModuleDescriptor object for the extension
     * module indicated by sourceName
     * @throws ExtensionModuleNotFoundException if no extension module with
     * name sourceName can be found
     * @throws MetaMatrixComponentException indicating a non-business-related
     * exception (such as a communication exception)
     */
    public static ExtensionModuleDescriptor getSourceDescriptor(String sourceName, Connection jdbcConnection)
    throws ExtensionModuleNotFoundException, MetaMatrixComponentException{
        //NOTE: DO NOT PUT LOGGING IN THIS METHOD
        // Because configuration initializes by getting the source
        // from the database prior to configuration properties being
        // available

      PreparedStatement statement = null;
      ResultSet results = null;
      String sql = JDBCExtensionModuleTranslator.SELECT_DESCRIPTOR_INFO;
      try{    
          
          statement = jdbcConnection.prepareStatement(sql);
          statement.setString(1, sourceName);
    
          if ( ! statement.execute() ) {
              throw new MetaMatrixComponentException(ErrorMessageKeys.EXTENSION_0046, CommonPlugin.Util.getString(ErrorMessageKeys.EXTENSION_0046, sql));
          }
          results = statement.getResultSet();
    
          if (results.next()) {
              return buildExtensionDescriptor(results);
          }
          throw new ExtensionModuleNotFoundException(sourceName);
      } catch (SQLException se){
          throw new MetaMatrixComponentException(se, ErrorMessageKeys.EXTENSION_0047, CommonPlugin.Util.getString(ErrorMessageKeys.EXTENSION_0047, sql));
      } finally {
          close(results);
          close(statement);
      }

        
    }

    /**
     * Returns List of ExtensionModuleDescriptor objects of indicated type,
     * in order of their search ordering
     * @param type one of the known types of extension file
     * @param includeDisabled if "false", only descriptors for <i>enabled</i>
     * extension modules will be returned; otherwise all modules will be.
     * @return List of ExtensionModuleDescriptor objects of indicated type,
     * in order of their search ordering
     * @throws MetaMatrixComponentException indicating a non-business-related
     * exception (such as a communication exception)
     */
    public static List getSourceDescriptors(String type, boolean includeDisabled, Connection jdbcConnection) throws MetaMatrixComponentException{
       PreparedStatement statement = null;
       ResultSet results = null;
       String sql = null;
       boolean bytype = false;
       if (type == null || type.length() == 0) {
           sql = JDBCExtensionModuleTranslator.SELECT_ALL_DESCRIPTORS_INFO;
       } else {
           bytype = true;
           sql = JDBCExtensionModuleTranslator.SELECT_ALL_DESCRIPTORS_INFO_BY_TYPE;
       }
       List descriptors = new ArrayList();
       try{
    
          
           statement = jdbcConnection.prepareStatement(sql);
           if (bytype) {
               statement.setString(1, type);
           }
           
           if ( ! statement.execute() ) {
               throw new MetaMatrixComponentException(ErrorMessageKeys.EXTENSION_0046, CommonPlugin.Util.getString(ErrorMessageKeys.EXTENSION_0046, sql));
           }
           results = statement.getResultSet();
    
           while (results.next()) {
        	   ExtensionModuleDescriptor desc = buildExtensionDescriptor(results);
               if (includeDisabled) {
                   descriptors.add(desc);
               } else {
                   if (desc.isEnabled()) {
                       descriptors.add(desc);
                   }
               }
           } 
       } catch (SQLException se){
           throw new MetaMatrixComponentException(se, ErrorMessageKeys.EXTENSION_0047, CommonPlugin.Util.getString(ErrorMessageKeys.EXTENSION_0047, sql));
       } finally {
           close(results);
           close(statement);
       }
       

        Collections.sort(descriptors);


        return descriptors;
    }


   public static int getExtensionModuleCount(Connection jdbcConnection) throws MetaMatrixComponentException{
        return JDBCExtensionModuleReader.executeIntFunctionSQL(JDBCExtensionModuleTranslator.SELECT_ROW_COUNT, jdbcConnection);
    }


   public static int executeIntFunctionSQL(String sql, Connection jdbcConnection) throws MetaMatrixComponentException{
        PreparedStatement statement = null;
        ResultSet results = null;
        int result = -1;
        try{
            statement = jdbcConnection.prepareStatement(sql);

            if ( ! statement.execute() ) {
                throw new MetaMatrixComponentException(ErrorMessageKeys.EXTENSION_0046, CommonPlugin.Util.getString(ErrorMessageKeys.EXTENSION_0046, sql));
            }
            results = statement.getResultSet();

            if (results.next()) {
                int resultColumn = 1;
                result = results.getInt(resultColumn);
            } else {
                throw new MetaMatrixComponentException(ErrorMessageKeys.EXTENSION_0050, CommonPlugin.Util.getString(ErrorMessageKeys.EXTENSION_0050, sql));
            }
        } catch (SQLException se){
            throw new MetaMatrixComponentException(se, ErrorMessageKeys.EXTENSION_0046, CommonPlugin.Util.getString(ErrorMessageKeys.EXTENSION_0046, sql));
        } finally {
            close(results);
            close(statement);
        }
        return result;
    }


    public static boolean isNameInUse(String sourceName, Connection jdbcConnection) throws MetaMatrixComponentException {
       PreparedStatement statement = null;
       ResultSet results = null; 
       String sql = JDBCExtensionModuleTranslator.SELECT_FILE_UID_BY_NAME;
       try{
           statement = jdbcConnection.prepareStatement(sql);
           statement.setString(1, sourceName);
    
           if ( ! statement.execute() ) {
               throw new MetaMatrixComponentException(ErrorMessageKeys.EXTENSION_0046, CommonPlugin.Util.getString(ErrorMessageKeys.EXTENSION_0046, sql));
           }
           results = statement.getResultSet();
    
           if (results.next()) {
               return true;
           } 
           return false;
       } catch (SQLException se){
           throw new MetaMatrixComponentException(se, ErrorMessageKeys.EXTENSION_0047, CommonPlugin.Util.getString(ErrorMessageKeys.EXTENSION_0047, sql));
       } finally {
           close(results);
           close(statement);
       }

    }

    
    private static void close(PreparedStatement statement) {
        if (statement != null) {
            try {
                statement.close();
                statement = null;
            } catch (SQLException e) {
                LogManager.logWarning(CONTEXT, e, CommonPlugin.Util.getString(ErrorMessageKeys.EXTENSION_0048));                
            }
        }
    }

    private static void close(ResultSet resultset) {
        if (resultset != null) {
            try {
                resultset.close();
                resultset = null;
            } catch (SQLException e) {
                LogManager.logWarning(CONTEXT, e, CommonPlugin.Util.getString(ErrorMessageKeys.EXTENSION_0048));                
            }
        }
    }

    
    /**
     * Simple dataholder 
     * @since 4.2
     */
    private static class CheckSumAndType {
        private long checksum;
        private String type;
        
        public void setChecksum(long checksum) {
            this.checksum = checksum;
        }
        public long getChecksum() {
            return checksum;
        }
        
        public void setType(String type) {
            this.type = type;
        }
        public String getType() {
            return type;
        }
    }
    
    public static ExtensionModuleDescriptor buildExtensionDescriptor (ResultSet resultSet) throws MetaMatrixComponentException {
        try{
        	ExtensionModuleDescriptor module = new ExtensionModuleDescriptor(
            resultSet.getString(JDBCNames.ExtensionFilesTable.ColumnName.FILE_NAME),
            resultSet.getString(JDBCNames.ExtensionFilesTable.ColumnName.FILE_TYPE),
            resultSet.getInt(JDBCNames.ExtensionFilesTable.ColumnName.SEARCH_POSITION),
            resultSet.getBoolean(JDBCNames.ExtensionFilesTable.ColumnName.IS_ENABLED),
            resultSet.getString(JDBCNames.ExtensionFilesTable.ColumnName.FILE_DESCRIPTION),
            resultSet.getString(JDBCNames.ExtensionFilesTable.ColumnName.CREATED_BY),
            resultSet.getString(JDBCNames.ExtensionFilesTable.ColumnName.CREATION_DATE),
            resultSet.getString(JDBCNames.ExtensionFilesTable.ColumnName.UPDATED_BY),
            resultSet.getString(JDBCNames.ExtensionFilesTable.ColumnName.UPDATED),
            resultSet.getLong(JDBCNames.ExtensionFilesTable.ColumnName.CHECKSUM)
            );
        	return module;
        } catch (SQLException e){
            throw new MetaMatrixComponentException(e, ErrorMessageKeys.EXTENSION_0044, CommonPlugin.Util.getString(ErrorMessageKeys.EXTENSION_0044));
        }
    }
    
}
