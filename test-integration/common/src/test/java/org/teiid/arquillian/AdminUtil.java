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

package org.teiid.arquillian;

import java.util.List;
import java.util.Properties;

import org.teiid.adminapi.Admin;
import org.teiid.adminapi.AdminException;
import org.teiid.adminapi.VDB;
import org.teiid.adminapi.AdminFactory.AdminImpl;
import org.teiid.adminapi.VDB.Status;

@SuppressWarnings("nls")
public class AdminUtil {

	static void cleanUp(Admin admin) throws AdminException {
		//TODO: cleanup when as supports it
		/*for (String name : admin.getDataSourceNames()) {
			admin.deleteDataSource(name);
		}*/
		for (VDB vdb : admin.getVDBs()) {
			String deploymentName = vdb.getPropertyValue("deployment-name");
			if (deploymentName != null) {
				admin.undeploy(deploymentName);
			}
		}
	}
	
	//TODO: this should not be needed, but cli doesn't currently support delete
	static boolean createDataSource(Admin admin, String deploymentName, String templateName, Properties properties) throws AdminException {
		if (admin.getDataSourceNames().contains(deploymentName)) {
			return false;
		}
		admin.createDataSource(deploymentName, templateName, properties);
		return true;
	}

	static boolean waitForVDBLoad(Admin admin, String vdbName, int vdbVersion,
			int timeoutInSecs) throws AdminException {
		long waitUntil = System.currentTimeMillis() + timeoutInSecs*1000;
		if (timeoutInSecs < 0) {
			waitUntil = Long.MAX_VALUE;
		}
		boolean first = true;
		do {
			if (!first) {
				try {
					Thread.sleep(2000);
				} catch (InterruptedException e) {
					break;
				}
			} else {
				first = false;
			}
			VDB vdb = admin.getVDB(vdbName, vdbVersion);
			if (vdb != null && vdb.getStatus() != Status.LOADING) {
				return true;
			}
		} while (System.currentTimeMillis() < waitUntil);
		return false;
	}
	
	static boolean waitForDeployment(Admin admin, String deploymentName,
			int timeoutInSecs) {
		long waitUntil = System.currentTimeMillis() + timeoutInSecs*1000;
		if (timeoutInSecs < 0) {
			waitUntil = Long.MAX_VALUE;
		}
		boolean first = true;
		do {
			if (!first) {
				try {
					Thread.sleep(2000);
				} catch (InterruptedException e) {
					break;
				}
			} else {
				first = false;
			}
			List<String> deployments = ((AdminImpl)admin).getDeployments();
			if (deployments.contains(deploymentName)) {
				return true;
			}
		} while (System.currentTimeMillis() < waitUntil);
		return false;
	}

}
