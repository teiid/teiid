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

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Properties;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.CommonPlugin;
import com.metamatrix.common.config.JDBCConnectionPoolHelper;
import com.metamatrix.common.extensionmodule.ExtensionModuleDescriptor;
import com.metamatrix.common.extensionmodule.ExtensionModuleManager;
import com.metamatrix.common.extensionmodule.exception.DuplicateExtensionModuleException;
import com.metamatrix.common.extensionmodule.exception.ExtensionModuleNotFoundException;
import com.metamatrix.common.util.ByteArrayHelper;
import com.metamatrix.common.util.ErrorMessageKeys;
import com.metamatrix.core.util.FileUtils;

public final class JDBCExtensionModuleUtil {

    private static final String PRINCIPAL = "JDBCExtensionUtil"; //$NON-NLS-1$

	private Properties properties;
	/**
	 * Instantiates this class with any Properties that need to override
	 * Properties from CurrentConfiguration.
	 * @param overrideResourceProps Properties that will override any
	 * properties gotten from CurrentConfiguration by
	 * ExtensionModuleManager.  Can be null or empty.
	 * See javadoc for this class.
	 */
	public JDBCExtensionModuleUtil(Properties overrideResourceProps){
        properties = overrideResourceProps;
        if (properties == null){
            properties = new Properties();
        }

	}


	/**
	 * Exports an extension module to the specified output stream
	 * @param outputStream is the output stream to write the module to
	 * @param sourceName is the name of the extension model to export
     * @return ExtensionModuleDescriptor describing the newly-added
     * extension module
     * @throws MetaMatrixComponentException indicating a non-business-related
     * exception (such as a communication exception)
     * @throws IllegalArguementException if any required args are null (args are
     * required unless otherwise noted)
	 */
	public void exportExtensionModule(OutputStream outputStream, String sourceName)
    throws ExtensionModuleNotFoundException, MetaMatrixComponentException{

        Connection connection = null;
        try {
            connection = JDBCConnectionPoolHelper.getInstance().getConnection(); 



            byte[] data = JDBCExtensionModuleReader.getSource(sourceName, connection);

            if (data == null) {
                throw new ExtensionModuleNotFoundException(ErrorMessageKeys.EXTENSION_0069, CommonPlugin.Util.getString(ErrorMessageKeys.EXTENSION_0069, sourceName));

            }

            ByteArrayInputStream bais = new ByteArrayInputStream(data);
            InputStream isContent = new BufferedInputStream(bais);

            byte[] buff = new byte[2048];
            int bytesRead;

            // Simple read/write loop.
            while(-1 != (bytesRead = isContent.read(buff, 0, buff.length))) {
                outputStream.write(buff, 0, bytesRead);
            }

            outputStream.flush();
            outputStream.close();

         } catch (ExtensionModuleNotFoundException notFound) {
        	throw notFound;

        } catch (Exception e) {
            throw new MetaMatrixComponentException(e, ErrorMessageKeys.EXTENSION_0070, CommonPlugin.Util.getString(ErrorMessageKeys.EXTENSION_0070, sourceName));
        } finally {
            try {
                if (connection != null) {                    
                    connection.close();
                }
            } catch (SQLException e1) {
            }
        }



	}

	/**
	 * Exports an extension module to the specified output stream
	 * @param outputFileName is the output file to write the module to
	 * @param sourceName is the name of the extension model to export
     * @return ExtensionModuleDescriptor describing the newly-added
     * extension module
     * @throws MetaMatrixComponentException indicating a non-business-related
     * exception (such as a communication exception)
     * @throws IllegalArguementException if any required args are null (args are
     * required unless otherwise noted)
	 */
	public void exportExtensionModule(String outputFileName, String sourceName)
    throws ExtensionModuleNotFoundException, MetaMatrixComponentException{

        Connection connection = null;
		try {

        	connection = JDBCConnectionPoolHelper.getInstance().getConnection(); 

        	byte[] data = JDBCExtensionModuleReader.getSource(sourceName, connection);


			if (data == null || data.length == 0) {
				throw new ExtensionModuleNotFoundException(ErrorMessageKeys.EXTENSION_0069, CommonPlugin.Util.getString(ErrorMessageKeys.EXTENSION_0069, sourceName));
			}

			FileUtils.write(data, outputFileName);

        } catch (ExtensionModuleNotFoundException notFound) {
           throw notFound;              

		} catch(Exception e) {
			throw new MetaMatrixComponentException(e, ErrorMessageKeys.EXTENSION_0006, CommonPlugin.Util.getString(ErrorMessageKeys.EXTENSION_0006, sourceName, outputFileName));
        } finally {
            try {
                if (connection != null) {                    
                    connection.close();
                }
            } catch (SQLException e1) {
            }
        }



	}
    
    public void importExtensionModule(String importFileName, String extName, String extType, String extDesc) 
        throws ExtensionModuleNotFoundException, DuplicateExtensionModuleException, MetaMatrixComponentException{
        
            Connection connection = null;
            FileInputStream stream = null;
            
            try {
                connection =
                    JDBCConnectionPoolHelper.getInstance().getConnection(); 
                         
                boolean inuse = JDBCExtensionModuleReader.isNameInUse(extName, connection);
                
                File aFile = new File(importFileName);
                
                if (!aFile.exists()) {               
                    throw new MetaMatrixComponentException(ErrorMessageKeys.EXTENSION_0064, CommonPlugin.Util.getString(ErrorMessageKeys.EXTENSION_0064, importFileName));
                }
    
                try {
                    stream = new FileInputStream(aFile);
                } catch (FileNotFoundException e) {
                    throw new MetaMatrixComponentException(ErrorMessageKeys.EXTENSION_0064, CommonPlugin.Util.getString(ErrorMessageKeys.EXTENSION_0064, importFileName));
                }
                int size = (int)aFile.length();
                
                byte[] data;
                try {
                    data = ByteArrayHelper.toByteArray(stream, size + 1);
                } catch (IOException e1) {
                    throw new MetaMatrixComponentException(e1, ErrorMessageKeys.EXTENSION_0070, CommonPlugin.Util.getString(ErrorMessageKeys.EXTENSION_0070, importFileName));   
                }
                
                if (inuse) {
                    JDBCExtensionModuleWriter.setSource(PRINCIPAL, extName, data, ExtensionModuleManager.getChecksum(data), connection);          
                } else {  
                    JDBCExtensionModuleWriter.addSource(PRINCIPAL, extType, extName, data, ExtensionModuleManager.getChecksum(data), extDesc, true, connection);
                }
                 
            } catch (MetaMatrixComponentException mce) {
                throw mce;
            } catch (Exception e) {            
                if (stream != null) {
                    try {                    
                        stream.close();
                    } catch (IOException e3) {
                    }                
                }
                throw new MetaMatrixComponentException(e);
                
            } finally {
                try {
                    if (connection != null) {                    
                        connection.close();
                    }
                } catch (SQLException e1) {
                }
            }

            
            connection = null;            

    }
    
    public void importExtensionModule(String importFileName, String extName, String extType, String extDesc, String position) 
    throws ExtensionModuleNotFoundException, DuplicateExtensionModuleException, MetaMatrixComponentException{
        importExtensionModule(importFileName, extName, extType, extDesc);
        positionExtensionModule(extName, position);
    }
    
    /**
     * Deletes an extension module 
     * @param sourceName is the name of the extension model to delete
     * @return ExtensionModuleDescriptor describing the newly-added
     * extension module
     * @throws MetaMatrixComponentException indicating a non-business-related
     * exception (such as a communication exception)
     * @throws IllegalArguementException if any required args are null (args are
     * required unless otherwise noted)
     */
    public void deleteExtensionModule(String sourceName)
    throws MetaMatrixComponentException{

        Connection connection = null;
        try {

            connection = JDBCConnectionPoolHelper.getInstance().getConnection(); 

            boolean inuse = JDBCExtensionModuleReader.isNameInUse(sourceName, connection);
            if (!inuse) {
                return;
            }
            JDBCExtensionModuleWriter.removeSource("JDBCExtensionUtil", sourceName, connection);//$NON-NLS-1$

        } catch(Exception e) {
            throw new MetaMatrixComponentException(e, ErrorMessageKeys.EXTENSION_0071, CommonPlugin.Util.getString(ErrorMessageKeys.EXTENSION_0071, sourceName));
        } finally {
            try {
                if (connection != null) {                    
                    connection.close();
                }
            } catch (SQLException e1) {
            }
        }



    }
    
    
    
    protected void positionExtensionModule(String extName, String position)  
    throws MetaMatrixComponentException{
        int pos = -1;
        if (position == null) {
            return;
        }
        try {
            pos = new Integer(position).intValue();
        }catch (Throwable t) {
            return;
        }

        Connection connection = null;
        try {
            connection =
                JDBCConnectionPoolHelper.getInstance().getConnection(); 
                          
            LinkedList orderList = new LinkedList();                                    
            Collection descriptors = JDBCExtensionModuleReader.getSourceDescriptors(null, true, connection);  
            ExtensionModuleDescriptor desc = null;
            boolean inserted=false;
            for (Iterator it=descriptors.iterator(); it.hasNext();) {
                desc = (ExtensionModuleDescriptor) it.next();  
                
                // do not include the descriptor for the one that
                // will be inserted at the specific position
                if (desc.getName().equals(extName)) {
                    continue;
                }
                    
// these should be coming in,in the order of their position
// from calling getSourceDescriptors()                    
                if (!inserted) {
                    if (desc.getPosition() < pos) {
                        orderList.add(desc.getName());                
                    } else if (desc.getPosition() >= pos){
                        orderList.add(extName);
                        orderList.add(desc.getName());                        
                        inserted=true;                        
                    } 
                } else {
                    orderList.add(desc.getName());
                }
            } 
            
            JDBCExtensionModuleWriter.setSearchOrder(PRINCIPAL, orderList, connection);
                     
        } catch (MetaMatrixComponentException mce) {
            throw mce;
        } catch (Exception e) {
                    
            throw new MetaMatrixComponentException(e);
                    
        } finally {
            try {
                if (connection != null) {                    
                    connection.close();
                }
            } catch (SQLException e1) {
            }
        }

  }
    
}
