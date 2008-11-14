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

package com.metamatrix.platform.config.transaction.lock.distributed;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.Properties;

import com.metamatrix.common.config.CurrentConfiguration;
import com.metamatrix.common.config.ResourceNames;
import com.metamatrix.common.config.api.SharedResource;
import com.metamatrix.common.config.api.exceptions.ConfigurationException;
import com.metamatrix.common.pooling.api.ResourceHelper;
import com.metamatrix.common.pooling.api.exception.ResourcePoolException;
import com.metamatrix.common.util.PropertiesUtils;
import com.metamatrix.common.util.VMNaming;
import com.metamatrix.core.util.DateUtil;
import com.metamatrix.platform.config.ConfigMessages;
import com.metamatrix.platform.config.ConfigPlugin;
import com.metamatrix.platform.config.transaction.ConfigTransactionLock;
import com.metamatrix.platform.config.transaction.ConfigTransactionLockException;
import com.metamatrix.platform.config.transaction.ConfigTransactionLockFactory;

public class DistributedVMTransactionLockFactory extends  ConfigTransactionLockFactory {

//	private Connection connection;
	private String hostName = "NotAssigned"; //$NON-NLS-1$

	private Properties resourceProps = null;
	private Properties allProps = null;

	public DistributedVMTransactionLockFactory(Properties props)  {
			super(props);

	}

	public void init() throws ConfigurationException {


/*

        DriverManager.setLoginTimeout(480);
        // autocommit is left at true because each insert, update or delete
        // should be committed immediately
        // there's no reason for controlling the transaction
        this.connection = JDBCPersistentUtil.getConnection(getProperties());
*/


     	hostName = VMNaming.getLogicalHostName();
        if (hostName == null) {
            throw new ConfigurationException(ConfigMessages.CONFIG_0179, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0179));
            
        }


	}


// implemented abstract method
	protected synchronized ConfigTransactionLock obtainLock(String principal, int lockReason)
    throws ConfigTransactionLockException {
		ConfigTransactionLock newLock = null;


		try {
			newLock = acquireLock(principal, lockReason);

		} catch (ConfigTransactionLockException e) {
			// if an error occurs make sure the lock was not obtained,
            // because it should be done cleanly, not halfway
			deleteLock();
			throw e;
		}

		return newLock;
	}


	protected synchronized void releaseLock() throws ConfigTransactionLockException  {
			deleteLock();
	}

	protected synchronized ConfigTransactionLock getCurrentLock() throws ConfigTransactionLockException {
		return getLock();
	}


	void deleteLock() throws ConfigTransactionLockException {

        PreparedStatement statement = null;
        Connection connection = null;

        try{

        	connection = getConnection();

            statement = connection.prepareStatement(SQL_Translator.DELETE_LOCK);

			statement.executeUpdate();


        } catch ( SQLException e ) {
            throw new ConfigTransactionLockException(e, ConfigMessages.CONFIG_0150, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0150, SQL_Translator.DELETE_LOCK));
        } catch ( Exception e ) {
            if (e instanceof ConfigTransactionLockException){
                throw (ConfigTransactionLockException)e;
            }
            throw new ConfigTransactionLockException(e, e.getMessage());
        } finally {

            if ( statement != null ) {
                try {
                    statement.close();
                    statement=null;
                } catch ( SQLException e ) {
                    e.printStackTrace();
                    System.out.println(ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0151, SQL_Translator.DELETE_LOCK));
                }
            }

           if (connection != null)  {
            	try {
            		connection.close();
            	} catch (SQLException ce) {
                    ce.printStackTrace();
                    System.out.println(ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0183));

            	}

            	connection = null;
            }

        }


	}


    ConfigTransactionLock getLock() throws ConfigTransactionLockException {

		ConfigTransactionLock lock=null;

        PreparedStatement statement = null;

        Connection connection = null;
        try{

        	connection = getConnection();

            statement = connection.prepareStatement(SQL_Translator.SELECT_LOCK);
            statement.execute();

            ResultSet results = statement.getResultSet();

            if (results != null && results.next()) {
 //         		String host         = results.getString(SQL_Translator.ConfigurationLockTable.ColumnName.HOST);
            	String userName     = results.getString(SQL_Translator.ConfigurationLockTable.ColumnName.USER_NAME);
            	String dateAcquired = results.getString(SQL_Translator.ConfigurationLockTable.ColumnName.DATETIME_ACQUIRED);
            	String dateExpires = results.getString(SQL_Translator.ConfigurationLockTable.ColumnName.DATETIME_EXPIRE);
            	int reason =	results.getInt(SQL_Translator.ConfigurationLockTable.ColumnName.LOCK_TYPE);

				Date acquired = DateUtil.convertStringToDate(dateAcquired);
				Date expires = DateUtil.convertStringToDate(dateExpires);

				lock = super.createLock(userName, acquired.getTime(), expires.getTime(), reason);

            }


        } catch ( SQLException e ) {
            throw new ConfigTransactionLockException(e, ConfigMessages.CONFIG_0150, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0150, SQL_Translator.SELECT_LOCK));
        } catch ( Exception e ) {
            if (e instanceof ConfigTransactionLockException){
                throw (ConfigTransactionLockException)e;
            }
            throw new ConfigTransactionLockException(e, e.getMessage());
        } finally {

            if ( statement != null ) {
                try {
                    statement.close();
                    statement=null;
                } catch ( SQLException e ) {
                    e.printStackTrace();
                    System.out.println(ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0151, SQL_Translator.SELECT_LOCK));
                }
            }

            if (connection != null)  {
            	try {
            		connection.close();
            	} catch (SQLException ce) {
                    ce.printStackTrace();
                    System.out.println(ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0183));

            	}

            	connection = null;
            }
        }



//		System.out.println("<CONFIGLOCK>Obtained lock for " + principal);
		return lock;

    }

	ConfigTransactionLock forceLock(String principal, int lockReason) throws ConfigTransactionLockException {

		deleteLock();

		return acquireLock(principal, lockReason);

	}

    /**
     *
     */
    ConfigTransactionLock acquireLock(String principal, int lockReason) throws  ConfigTransactionLockException{

		ConfigTransactionLock newLock=null;

        String sql = null;
        PreparedStatement statement = null;

        Connection connection = null;
        try{

        	connection = getConnection();

        	Date acqDate = new Date();
        	long expiration = calculateLockExpiration(acqDate.getTime());
        	Date expDate = new Date(expiration);

        	String a = DateUtil.getDateAsString(acqDate);
        	String e = DateUtil.getDateAsString(expDate);

            sql = SQL_Translator.INSERT_LOCK;
            statement = connection.prepareStatement(sql);

			statement.setString(1, hostName);
            statement.setString(2, principal);
            statement.setString(3, a);
            statement.setString(4, e);
            statement.setInt(5, lockReason);

            statement.execute();
            int updateCount = statement.getUpdateCount();
 
            if (updateCount <= 0){
                throw new ConfigurationException(ConfigMessages.CONFIG_0158, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0158, sql));
            }


			newLock = super.createLock(principal, acqDate.getTime(), expiration, lockReason);

        } catch ( SQLException e ) {
            throw new ConfigTransactionLockException(e, ConfigMessages.CONFIG_0150, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0150, sql));
        } catch ( Exception e ) {
            if (e instanceof ConfigTransactionLockException){
                throw (ConfigTransactionLockException)e;
            }
            throw new ConfigTransactionLockException(e, e.getMessage());
        } finally {
            if ( statement != null ) {
                try {
                    statement.close();
                    statement=null;
                } catch ( SQLException e ) {
                    e.printStackTrace();
                    System.out.println(ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0151, sql));
                }
            }
            if (connection != null)  {
            	try {
            		connection.close();
            	} catch (SQLException ce) {
                    ce.printStackTrace();
                    System.out.println(ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0183));

            	}

            	connection = null;
            }


        }

        return newLock;
    }


	private Connection getConnection() throws ConfigurationException, ResourcePoolException {

			if (resourceProps == null) {
			// this is done at this point so that XMLConfiguarationMgr can
			// complete the init method and not cause recursive behavior
				SharedResource r = CurrentConfiguration.getConfigurationModel().getResource(ResourceNames.CONFIGURATION_SERVICE);
				resourceProps = r.getProperties();

				allProps = PropertiesUtils.clone(getProperties(), false);
				allProps.putAll(resourceProps);
			}

//				System.out.println("CONFIG SVC PROPS: " + PropertiesUtils.prettyPrint(resourceProps));

		return (Connection) ResourceHelper.getResource(resourceProps, "ConfigurationLockTransaction"); //$NON-NLS-1$

	}

}
