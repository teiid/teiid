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
package org.teiid.rhq.comm.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.teiid.rhq.comm.Component;
import org.teiid.rhq.comm.Connection;
import org.teiid.rhq.comm.ConnectionException;
import org.teiid.rhq.comm.ConnectionPool;
import org.teiid.rhq.comm.ExecutedResult;


/** 
 * @since 6.2
 */
public class FakeConnection implements Connection{

    public static int NUM_CONNECTORS = 3;
    public static int NUM_VMS = 2;
    public static int NUM_HOSTS = 1;
    
    private static final String HOST_ID = "Host_id_1"; //$NON-NLS-1$
    
    private String installDir = null;
    private ConnectionPool pool;
    private Properties envProps = null;
    
    public FakeConnection(ConnectionPool pool, Properties env) {
        this.pool = pool;
        this.envProps = env;
        
    }
    
    

    @Override
	public Collection<Component> discoverComponents(String componentType,
			String identifier) throws ConnectionException {
		// TODO Auto-generated method stub
		return null;
	}



	@Override
	public void executeOperation(ExecutedResult result, Map valueMap)
			throws ConnectionException {
		// TODO Auto-generated method stub
		
	}



	@Override
	public String getKey() throws Exception {
		// TODO Auto-generated method stub
		return "fakekey";
	}



	@Override
	public Object getMetric(String componentType, String identifier,
			String metric, Map valueMap) throws ConnectionException {
		// TODO Auto-generated method stub
		return null;
	}



	/** 
     * @see com.metamatrix.rhq.comm.Connection#isValid()
     * @since 6.2
     */
    public boolean isValid() {
        return true;
    }

    /** 
     * @throws Exception 
     * @see com.metamatrix.rhq.comm.Connection#close()
     * @since 6.2
     */
    public void close()  {
        try {
            pool.close(this);
        } catch (Exception err) {
            throw new RuntimeException(err);
            
        }
    }


    /** 
     * @see com.metamatrix.rhq.comm.Connection#getConnectors(java.lang.String)
     * @since 6.2
     */
    public Collection<Component> getConnectors(String vmname) throws ConnectionException {
        Collection<Component> vms = getVMs();
        Collection<Component> cs = new ArrayList(5);
        for (Iterator vmIt=vms.iterator(); vmIt.hasNext();) {
            Component vm = (Component) vmIt.next();
            for (int i = 0; i < NUM_VMS; i++) {
                ComponentImpl c = new ComponentImpl();
                c.setSystemKey(this.installDir);
                c.setIdentifier(vm.getIdentifier() + "|VM_id_" + i); //$NON-NLS-1$
                c.setName("VM_id_" + i); //$NON-NLS-1$
                cs.add(c);
            }
        }
        
        return cs;
    }

    /** 
     * @see com.metamatrix.rhq.comm.Connection#getEnvironment()
     * @since 6.2
     */
    public Properties getEnvironment() throws Exception {
        return this.envProps;
    }

    /** 
     * @see com.metamatrix.rhq.comm.Connection#getHost()
     * @since 6.2
     */
    public Component getHost() throws ConnectionException {
            ComponentImpl c = new ComponentImpl();
            c.setSystemKey(this.installDir);
            c.setIdentifier(HOST_ID);
            c.setName(HOST_ID );
            c.setPort("1"); //$NON-NLS-1$
        return c;
    }

    /** 
     * @see com.metamatrix.rhq.comm.Connection#getInstallationDirectory()
     * @since 6.2
     */
    public String getInstallationDirectory() throws Exception {
        return installDir;
    }

    /** 
     * @see com.metamatrix.rhq.comm.Connection#getMetric(java.lang.String, java.lang.String, java.util.Map)
     * @since 6.2
     */
    public Object getMetric(String componentType,
                            String metric,
                            Map valueMap) throws ConnectionException {
        return null;
    }
    
    

    /** 
     * @see com.metamatrix.rhq.comm.Connection#getProperties(java.lang.String, java.lang.String)
     * @since 6.2
     */
    public Properties getProperties(String resourceType,
                                    String identifier) throws ConnectionException {
        return null;
    }

    /** 
     * @see com.metamatrix.rhq.comm.Connection#getProperty(java.lang.String, java.lang.String)
     * @since 6.2
     */
    public String getProperty(String identifier,
                              String property) throws ConnectionException {
        return this.envProps.getProperty(property);
    }

    /** 
     * @see com.metamatrix.rhq.comm.Connection#getVMs()
     * @since 6.2
     */
    public Collection<Component> getVMs() throws ConnectionException {
  
        
        Component host = getHost();
        Collection cs = new ArrayList(5);
        for (int i = 0; i < NUM_VMS; i++) {
            ComponentImpl c = new ComponentImpl();
            c.setSystemKey(this.installDir);
            c.setIdentifier(host.getIdentifier() + "|VM_id_" + i); //$NON-NLS-1$
            c.setName("VM_id_" + i); //$NON-NLS-1$
            c.setPort(String.valueOf(i));
            cs.add(c);
        }
        
        return cs;

    }
    
    public Collection<Component> getVDBs(List fieldNameList) throws ConnectionException {
		return Collections.EMPTY_LIST;
	}

	/** 
     * @see com.metamatrix.rhq.comm.Connection#isAlive()
     * @since 6.2
     */
    public boolean isAlive() {
        return true;
    }

    /** 
     * @see com.metamatrix.rhq.comm.Connection#isAvailable(java.lang.String, java.lang.String)
     * @since 6.2
     */
    public Boolean isAvailable(String componentType,
                               String identifier) throws ConnectionException {
        return true;
    }

}
