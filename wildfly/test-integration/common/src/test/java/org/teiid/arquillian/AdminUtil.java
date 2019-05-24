/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.teiid.arquillian;

import java.util.List;
import java.util.Properties;

import org.teiid.adminapi.Admin;
import org.teiid.adminapi.AdminException;
import org.teiid.adminapi.VDB;
import org.teiid.adminapi.VDB.Status;
import org.teiid.adminapi.jboss.AdminFactory.AdminImpl;

@SuppressWarnings("nls")
public class AdminUtil {
    public static final int MANAGEMENT_PORT = 9990;

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

    public static final int DEFAULT_VDB_LOAD_TIMEOUT = 7;

    static boolean waitForVDBLoad(Admin admin, String vdbName, Object vdbVersion) throws AdminException {
        long waitUntil = System.currentTimeMillis() + DEFAULT_VDB_LOAD_TIMEOUT*1000;
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
            VDB vdb = admin.getVDB(vdbName, vdbVersion!=null?vdbVersion.toString():null);
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
