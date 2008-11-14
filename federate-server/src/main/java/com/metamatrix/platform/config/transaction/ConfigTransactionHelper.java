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

package com.metamatrix.platform.config.transaction;

import com.metamatrix.common.transaction.TransactionException;
import com.metamatrix.platform.config.ConfigMessages;
import com.metamatrix.platform.config.ConfigPlugin;

public class ConfigTransactionHelper {
		/**
		 * The number retries to be performed before an exception will be thrown.
		 */
		private static final int RETRY_CNT = 100;
		/**
		 * The time interval between retries
		 */
		private static final int RETRY_PERIOD = 2000; //5 min, not 2.5 mins


  public static ConfigUserTransaction getWriteTransactionWithRetry(String principal, ConfigUserTransactionFactory factory ) throws ConfigTransactionException {
        ConfigUserTransaction trans = null;
        ConfigTransactionException initialException = null;
         int cnt = 0;
         while (trans == null) {
	         try {
//          System.out.println("<CFG TRANS HELPER>Get Trans acquired by " + principal  + " Time " + new Date() );

	         	trans = getUserTransaction(principal, factory);

	         } catch (ConfigTransactionLockException ctle) {
//                  ctle.printStackTrace();
                  throw ctle;

             } catch (ConfigTransactionException cte) {

                if (initialException == null) {
                    initialException = cte;
                }
	         	// if there is a lock currently held,
	         	// sleep and then retry
		         try {
		                Thread.sleep(RETRY_PERIOD); // sleep for 2 second
		                ++cnt;

		         } catch (Exception e) {
		         }

		         if (cnt > RETRY_CNT) {
		         	throw initialException;
		         }


	         }

         }
         return trans;

    }

   private static ConfigUserTransaction getUserTransaction(String principal, ConfigUserTransactionFactory factory) throws ConfigTransactionException {
		ConfigUserTransaction userTrans = null;

	    try {
	     // if the begin transaction does not work, a rollback is will
	     // automatically called because the transaction is no good,
	     // but the exception will be thrown so that the caller
	     // is notified of the problem
	    	userTrans = factory.createWriteTransaction(principal);

	        userTrans.begin();
	        return userTrans;

	    } catch (TransactionException te) {

			throw new ConfigTransactionException(te, ConfigMessages.CONFIG_0162, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0162, principal));

	    }

    }

}
