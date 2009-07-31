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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.StringTokenizer;

import org.teiid.adminapi.Admin;
import org.teiid.adminapi.AdminException;
import org.teiid.adminapi.ConnectorBinding;
import org.teiid.adminapi.Host;
import org.teiid.adminapi.ProcessObject;
import org.teiid.adminapi.PropertyDefinition;
import org.teiid.adminapi.Service;
import org.teiid.rhq.comm.ConnectionException;
import org.teiid.rhq.comm.ConnectionConstants.ComponentType.Runtime.Process;


/** 
 * @since 4.3
 */
public class ConnectionUtil implements TeiidConnectionConstants {
    
	private static Map<String, Map<String, Object>> defCache = new HashMap<String, Map<String, Object>>();
    
    /**
     * Call to find a host.  The hostName passed in can be any of the
     * the following:
     * <li>The fully qualified host name</li>
     * <li>The short name of the fully qualified host name</li>
     * <li>The IP address of box</li>
     * <li>The bind address defined in the host properties</li>
     * <li>The general reference of localhost</li>
     * 
     * The order of resolution will be as follows:
     * 1.  Try to match the IPAddress resolved host names to what is configured.
     * <li>hostName matches to configured host name</li>
     * <li>resolve hostName to an InetAddress and use its' full host name to match configured host(s)</li>
     * <li>resolve hostName to an InetAddress and use its' short host name to match configured host(s)</li>
     * <li>In cases where the <code>hostName</code> represents the short name and will not resolve to a longer name, 
     *     convert the <code>Host</code> full name to the short name to try to match.</li>
     * <li>match hostname to the physical address for a configurated host</li>  
     * <li>match hostname to the bindaddress for a configurated host</li>  
     * 2.  Reverse the match, try matching what's configurated to host name passed
     * <li>try using the short name of configured host to match</li>
     * <li>try using the physical address of the configurated host match</li>
     * <li>try using the bind address of the configured host to match</li>
     * 
     * NOTE:  This logic is duplicated from the CurrentConfiguration.findHost().  That method could not
     * be used due to the different <code>Host</code> object types that are used.
     * 
     * @param hostName
     * @return Host
     * @throws Exception
     * @since 5.5
     */
//    public static Host findHost(String hostName,  ServerAdmin adminapi)  throws AdminException  {
//
//        Host h = null;
//        
//        try {
//            // first try to match the name pas host by what was passed before
//            // substituting something else
//            h = getHost(hostName, adminapi);
//            if (h != null) {
//                return h;
//            }
//            
//            h = getHost(InetAddress.getLocalHost().getHostName(), adminapi);
//            if (h != null) {
//                return h;
//            }
//            
//                            
//            // the hostName could be an IP address that we'll use to try to 
//            // resolve back to the actuall host name
//            InetAddress inetAddress = InetAddress.getByName(hostName);           
//
//            // try using the fully qualified host name
//            
//            h = getHost(inetAddress.getCanonicalHostName(), adminapi);
//            if (h != null) {
//                return h;
//            }
//            
//            h = getHost(inetAddress.getHostName(), adminapi);            
//            if (h != null) {
//                return h;
//            }
//                    
//            // try the address
//              h = getHost(inetAddress.getHostAddress(), adminapi);
//              
//              if (h != null) {
//                  return h;
//              }            
//        } catch (AdminException ae) {
//            throw ae;
//        } catch (Exception e) {
//            // do nothing
//        }
//        
//        
//        
//        // 2nd try to match 
//        try {
//            Collection hosts = getAllHosts(adminapi);
//            // if the host name passed in is the short name,
//            // then try to match to the Host short name
//            
//            Iterator hi = hosts.iterator(); 
//            
//            while (hi.hasNext()) {
//                   
//                String hostAddress = h.getPropertyValue(Host.HOST_PHYSICAL_ADDRESS);
//                
//                if (hostAddress != null && hostAddress.equalsIgnoreCase(hostName)) {
//                    return h;
//                } 
//                String hostBindAddress = h.getPropertyValue(Host.HOST_BIND_ADDRESS);
//                
//                if (hostBindAddress.equalsIgnoreCase(hostName)) {
//                    return h;
//                }
//                         
//    
//            }                 
//               
//        } catch (AdminException ae) {
//            throw ae;
//        } catch (Exception e) {
//            // do nothing
//        }       
//        
//        
//        return null;
//
//    }    
    
    /**
     * Called to return only 1 host 
     * @param hostName
     * @param adminapi
     * @return
     * @throws AdminException
     * @since 4.3
     */
    public static Host getHost(String hostIdentifier,  Admin adminapi) throws AdminException {
        //  Configuring and monitoring will no longer be done thru a single connection.
    	//  each host will be managed seperately
     	
    	
//        Collection hosts = adminapi.getHosts(hostIdentifier);
//        
//        if (hosts != null && hosts.size() == 1) {
//            // return the unique host by the hostName
//            return (Host) hosts.iterator().next();
//        }
        // return null if no hosts were found or 
        // multiple hosts were found by that name.          
        return null;
    }
    
    /**
     * Called to return all the currently defined hosts 
     * @param adminapi
     * @return
     * @throws AdminException
     * @since 4.3
     */
//    public static Collection getAllHosts(Admin adminapi) throws AdminException {
//        
//    	return adminapi.getHosts("*"); //$NON-NLS-1$
//    }   
    
    
    /**
     * @param adminApi is the connection to the MM Server
     * @return Collection of MMProcesses
     */
//    public static Collection getAllProcesses(final Admin adminApi, String identifier) throws AdminException {
//
//        Collection processCollection = Collections.EMPTY_LIST;
//
//        processCollection =  adminApi.getProcesses(identifier + "|*");          //$NON-NLS-1$
//
//        return processCollection;
//    }
    
    public static ProcessObject getProcess(final Admin adminApi, String processIdentifier) throws AdminException {
        
        Collection processCollection =  adminApi.getProcesses(processIdentifier);
        
        if (processCollection != null && processCollection.size() == 1) {
            // return the unique connector binding
            return (ProcessObject) processCollection.iterator().next();
        }
        
        return null;
    }     
      
    /**
     * @param adminApi is the connection to the MM Server
     * @return Collection of MMProcesses
     */
//    public static Collection getAllConnectors(final Admin adminApi, String vmIdentifier) throws AdminException {
//
//        Collection connectorCollection = Collections.EMPTY_LIST;
//            
//        connectorCollection =  adminApi.getConnectorBindings(vmIdentifier +"|*");
//
//
//        return connectorCollection;
//    }   
    
    public static ConnectorBinding getConnector(final Admin adminApi, String cbIdentifier) throws AdminException {
            
        Collection connectorCollection =  adminApi.getConnectorBindings(cbIdentifier);
        
        if (connectorCollection != null && connectorCollection.size() == 1) {
            // return the unique connector binding
            return (ConnectorBinding) connectorCollection.iterator().next();
        }
        
        return null;
    }
    
    public static Service getService(final Admin adminApi, String svcIdentifier) throws AdminException {
        
//        Collection svcCollection =  adminApi.getServices(svcIdentifier);
//        
//        if (svcCollection != null && svcCollection.size() == 1) {
//            // return the unique connector binding
//            return (Service) svcCollection.iterator().next();
//        }
        
        return null;
    }

    
    /**
     * @param adminApi is the connection to the MM Server
     * 
     * @return Collection of VDBs
     */
//    public static Collection getAllVDBs(final Admin adminApi) throws AdminException {
//
//        Collection connectorCollection = Collections.EMPTY_LIST;
//            
//        connectorCollection =  adminApi.getVDBs("*"); //$NON-NLS-1$
//
//        return connectorCollection;
//    }    
    
    /*
     * Return the tokens in a string in a list. This is particularly
     * helpful if the tokens need to be processed in reverse order. In that case,
     * a list iterator can be acquired from the list for reverse order traversal.
     *
     * @param str String to be tokenized
     * @param delimiter Characters which are delimit tokens
     * @return List of string tokens contained in the tokenized string
     */
    public static List getTokens(String str, String delimiter) {
        ArrayList l = new ArrayList();
        StringTokenizer tokens = new StringTokenizer(str, delimiter);
        while(tokens.hasMoreTokens()) {
            l.add(tokens.nextToken());
        }
        return l;
    }    
    

    public static synchronized Map getPropertiesDefinitions(final Admin adminApi, String resourceType, String className, String identifier) throws AdminException, ConnectionException {
        Properties result = new Properties();
        
        boolean cache = false;
        if ( resourceType.equalsIgnoreCase(org.teiid.rhq.comm.ConnectionConstants.ComponentType.Runtime.Host.TYPE) ||
        		resourceType.equalsIgnoreCase(Process.TYPE) ) {
           	    cache = true;
           	    
                if (defCache.containsKey(className)) {
                	return defCache.get(className);
                }
        }

       
         Collection pdefs = adminApi.getPropertyDefinitions(identifier, className);

         Map<String, Object> defMap = new HashMap<String, Object>();
         
         for (Iterator it=pdefs.iterator(); it.hasNext();) {
             PropertyDefinition pdef = (PropertyDefinition)it.next();
             Object value = pdef.getValue() != null ? pdef.getValue() : pdef.getDefaultValue();
             defMap.put(pdef.getName(), (value != null ? value : "")); //$NON-NLS-1$             
             
         } 
         
         if (cache) {
        	 defCache.put(className, defMap);
         }
                  
        
        return defMap;
    }
    
    static class InvalidAdminException extends Throwable {

       public InvalidAdminException(String msg) {
           super(msg);
       }
        
    }
        

}
