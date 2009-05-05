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

package com.metamatrix.server;

import java.lang.management.ManagementFactory;
import java.util.Collection;
import java.util.Properties;
import java.util.StringTokenizer;

import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.OperationsException;

import org.jgroups.ChannelException;
import org.jgroups.JChannel;
import org.jgroups.mux.Multiplexer;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.metamatrix.common.config.CurrentConfiguration;
import com.metamatrix.common.config.ResourceNames;
import com.metamatrix.common.config.api.Host;
import com.metamatrix.common.config.api.HostID;
import com.metamatrix.common.config.api.VMComponentDefn;
import com.metamatrix.common.config.api.VMComponentDefnType;
import com.metamatrix.common.config.api.exceptions.ConfigurationException;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.util.LogCommonConstants;
import com.metamatrix.common.util.PropertiesUtils;
import com.metamatrix.core.MetaMatrixRuntimeException;

/**
 * This class contains a collection of utilities for managing JGroups objects.
 */
@Singleton
public class JGroupsProvider implements Provider<org.jgroups.mux.Multiplexer> {

	public static final String TCP = "tcp"; //$NON-NLS-1$
	private static final String DEFAULT_UDP_MCAST_ADDR_PREFIX = "224."; //$NON-NLS-1$
	private static final String ALL_INTERFACES_ADDR = "0.0.0.0"; //$NON-NLS-1$

    private static final String DEFAULT_JGROUPS_OTHER_SETTINGS =            
                    "MERGE2(min_interval=5000;max_interval=10000):" + //$NON-NLS-1$
                    "FD_SOCK:" +                                      //$NON-NLS-1$
                    "VERIFY_SUSPECT(timeout=1500):" +                 //$NON-NLS-1$
                    "pbcast.NAKACK(gc_lag=50;retransmit_timeout=300,600,1200,2400,4800):" + //$NON-NLS-1$
                    "UNICAST(timeout=5000):" + //$NON-NLS-1$
                    "pbcast.STABLE(desired_avg_gossip=20000):" + //$NON-NLS-1$
                    "FRAG(frag_size=4096;down_thread=false;up_thread=false):" + //$NON-NLS-1$
                    "pbcast.GMS(join_timeout=5000;join_retry_timeout=2000;" + //$NON-NLS-1$
                    "shun=false;print_local_addr=true):" + //$NON-NLS-1$
                    "pbcast.STATE_TRANSFER"; //$NON-NLS-1$
    
    private static final String ENCRYPT_ALL = ":ENCRYPT(key_store_name=teiid.keystore;store_password=changeit;alias=cluster_key)"; //$NON-NLS-1$
    private static final String ENCRYPT_ALL_KEY = "metamatrix.encryption.internal.secure.sockets"; //$NON-NLS-1$

    public enum Protocol { UNICAST_TCP, UNICAST_UDP, MULTICAST}
    
    public static final String CLUSTER_PROTOCOL = "cluster.protocol"; //$NON-NLS-1$
	private static final String CLUSTER_MULTICAST_PORT = "cluster.port"; //$NON-NLS-1$
	private static final String CLUSTER_MULTICAST_ADDRESS = "cluster.multicast.address"; //$NON-NLS-1$
	private static final String CLUSTER_PING = "cluster.unicast.ping"; //$NON-NLS-1$
	private static final String CLUSTER_TIMEOUT = "cluster.unicast.timout"; //$NON-NLS-1$
	private static final String JGROUPS_OTHER_SETTINGS_PROPERTY = "jgroups.other.channel.settings"; //$NON-NLS-1$
	private static final String BIND_ADDRESS_PROPERTY = "jgroups.bind.address"; //$NON-NLS-1$
    
	
	private int unicastPort;
	
	@Inject
	public JGroupsProvider(@Named(Configuration.UNICAST_PORT) int port) {
		this.unicastPort = port;
	}
	
    /**
	 * Helper method to create a JChannel.
	 * Properties needed to create the JChannel are read from current configuration.
	 * 
	 * @param String name - Name of the group the channel will join.
	 * @throws ChannelException if there is an error creating the JChannel
	 */
    
    public Multiplexer get() {
		try {
			JChannel channel=new JChannel(getChannelProperties());
			Multiplexer mux = new Multiplexer(channel);
			
			// JMX needs to moved out here, such that it can be registered and unregistered when shutdown occurs.
			// register the channel with the JMX server
			try {
				MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
				ObjectName on = new ObjectName("Teiid:service=JChannel,name=JGroups"); //$NON-NLS-1$
				mbs.registerMBean(new org.jgroups.jmx.JChannel(channel), on);
			} catch (MalformedObjectNameException e) {
				LogManager.logWarning(LogCommonConstants.CTX_CONFIG, "Failed to register JChannel to JMX"); //$NON-NLS-1$
			} catch (OperationsException e) {
				LogManager.logWarning(LogCommonConstants.CTX_CONFIG, "Failed to register JChannel to JMX"); //$NON-NLS-1$
			} catch (MBeanRegistrationException e) {
				LogManager.logWarning(LogCommonConstants.CTX_CONFIG, "Failed to register JChannel to JMX"); //$NON-NLS-1$
			} 
			
			return mux;
		} catch (ChannelException e) {
			throw new MetaMatrixRuntimeException(e);
		} 
	}

	private synchronized String getChannelProperties() {

		try {
        	String properties = null;
			Properties configProps = CurrentConfiguration.getInstance().getResourceProperties(ResourceNames.JGROUPS);
			boolean useEncrypt = PropertiesUtils.getBooleanProperty(CurrentConfiguration.getInstance().getProperties(), ENCRYPT_ALL_KEY, false);
			
			  
			Protocol protocol =  Protocol.valueOf(configProps.getProperty(CLUSTER_PROTOCOL, Protocol.UNICAST_TCP.toString()));
			boolean useMulticast = (protocol == Protocol.MULTICAST);
				
			String multicastPort = configProps.getProperty(CLUSTER_MULTICAST_PORT, "5555"); //$NON-NLS-1$
			String pingGossipRerefresh = configProps.getProperty(CLUSTER_PING, "10000"); //$NON-NLS-1$ 10 secs
			String pingTimeout = configProps.getProperty(CLUSTER_TIMEOUT, "2000"); //$NON-NLS-1$ 2 secs
			String otherSettings = configProps.getProperty(JGROUPS_OTHER_SETTINGS_PROPERTY, DEFAULT_JGROUPS_OTHER_SETTINGS);
			String unicastMembers =  CurrentConfiguration.getInstance().getProperties().getProperty(CurrentConfiguration.CLUSTER_MEMBERS);
			
			String bindAddress = getBindAddress();
			boolean multicastOnAllInterfaces = bindAddress.equalsIgnoreCase(ALL_INTERFACES_ADDR);
		    String clusterAddress = configProps.getProperty(CLUSTER_MULTICAST_ADDRESS);
		
			if (useMulticast) {
	            if (multicastOnAllInterfaces) {
	                properties = 
	                    "UDP(mcast_addr="+clusterAddress+";mcast_port="+multicastPort+";ip_ttl=32;receive_on_all_interfaces=true;" + //$NON-NLS-1$//$NON-NLS-2$ //$NON-NLS-3$
	                    "mcast_send_buf_size=150000;mcast_recv_buf_size=80000):" + //$NON-NLS-1$
	                    "PING(timeout="+pingTimeout+";):";//$NON-NLS-1$ //$NON-NLS-2$
	                LogManager.logInfo(LogCommonConstants.CTX_MESSAGE_BUS, "JGroups using multicast on all available interfaces"); //$NON-NLS-1$
	            } else {
	            	clusterAddress = getMulticastAddress(clusterAddress, bindAddress);
	    			properties = 
	    				"UDP(mcast_addr="+clusterAddress+";mcast_port="+multicastPort+";ip_ttl=32;bind_addr="+bindAddress+";"+   //$NON-NLS-1$//$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
	    				"mcast_send_buf_size=150000;mcast_recv_buf_size=80000):" + //$NON-NLS-1$
	    				"PING(timeout="+pingTimeout+";):";//$NON-NLS-1$ //$NON-NLS-2$
	    			LogManager.logInfo(LogCommonConstants.CTX_MESSAGE_BUS, "JGroups using multicast with address "+clusterAddress+ " and port "+ multicastPort); //$NON-NLS-1$ //$NON-NLS-2$
	            }
			} else {
				if (protocol == Protocol.UNICAST_UDP) { 
					clusterAddress = getMulticastAddress(clusterAddress, bindAddress);
					properties = 
						"UDP(ip_mcast=false;mcast_addr="+clusterAddress+";mcast_port="+multicastPort+";bind_addr="+bindAddress+"):" + //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
						"PING(gossip_host="+getGossipHost(unicastMembers)+";gossip_port="+getGossipPort(unicastMembers)+";gossip_refresh="+pingGossipRerefresh+";" + //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
						"timeout="+pingTimeout+";):";  //$NON-NLS-1$ //$NON-NLS-2$
					LogManager.logInfo(LogCommonConstants.CTX_MESSAGE_BUS, "JGroups using Unicast with UDP with gossip host"+getGossipHost(unicastMembers)+ " and port "+ getGossipPort(unicastMembers)); //$NON-NLS-1$ //$NON-NLS-2$
				}
				else {
					String resolvedClusterMemebers = resolveClusterMembers(unicastMembers);				
					properties = "TCP(start_port="+this.unicastPort+";port_range=0;bind_addr="+bindAddress+";loopback=true;skip_suspected_members=true;discard_incompatible_packets=true;sock_conn_timeout=3000;use_concurrent_stack=true;):" + //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
								 "TCPPING(initial_hosts="+resolvedClusterMemebers+";port_range=1;timeout="+pingTimeout+"):" +//$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
								 "pbcast.FLUSH(timeout=60000)"; //$NON-NLS-1$
					LogManager.logInfo(LogCommonConstants.CTX_MESSAGE_BUS, "JGroups using Unicast with TCP with initial members configured as "+ resolvedClusterMemebers + " with local process port "+this.unicastPort); //$NON-NLS-1$ //$NON-NLS-2$
				}
			}
			
			properties += otherSettings;
			if (useEncrypt) {
				properties += ENCRYPT_ALL;
			}
			return properties;
			
		} catch (ConfigurationException e) {
			throw new MetaMatrixRuntimeException(e);
		}            
	}
    
    private final String getBindAddress() {
    	String bindAddress = CurrentConfiguration.getInstance().getBindAddress();
            
        if (bindAddress == null) {
            LogManager.logWarning(LogCommonConstants.CTX_MESSAGE_BUS,"WARNING: Unable to set " + JGroupsProvider.BIND_ADDRESS_PROPERTY + ", will set to 127.0.0.1"); //$NON-NLS-1$ //$NON-NLS-2$
            bindAddress = "127.0.0.1";//$NON-NLS-1$
        }
        return bindAddress;
    }
    
    private String getMulticastAddress(String clusterAddress, String bindAddress){
    	if (clusterAddress == null || clusterAddress.length() == 0) {
	    	if (bindAddress.indexOf('.') != -1) {
	    		String lastNode = bindAddress.substring(bindAddress.indexOf('.')+1);
	    		return  DEFAULT_UDP_MCAST_ADDR_PREFIX + lastNode;
	    	}
	    }   
    	return clusterAddress;
    }
    
    private String getGossipHost(String clusterMembers) {
    	StringTokenizer st = new StringTokenizer(clusterMembers, ","); //$NON-NLS-1$
    	// the string must be in the format "hostA[port]"
    	String hostPort = st.nextToken();
    	int idx = hostPort.indexOf('[');
    	if (idx == -1) {
    		throw new MetaMatrixRuntimeException("Gossip Router information is not specified correctly. This must property must be in the form 'gossipHost[port]'"); //$NON-NLS-1$
    	}
    	return hostPort.substring(0, idx);
    }
    
    private String getGossipPort(String clusterMembers) {
    	StringTokenizer st = new StringTokenizer(clusterMembers, ","); //$NON-NLS-1$
    	// the string must be in the format "hostA[port]"
    	String hostPort = st.nextToken();
    	return hostPort.substring(hostPort.indexOf('[')+1, hostPort.lastIndexOf(']'));
    }    
	
	String resolveClusterMembers(String members) {
		try {
			com.metamatrix.common.config.api.Configuration config = CurrentConfiguration.getInstance().getConfiguration();
			if (members != null && members.trim().length() > 0) {
				StringBuilder sb = new StringBuilder();
				StringTokenizer st = new StringTokenizer(members.trim(), ","); //$NON-NLS-1$
				
				while(st.hasMoreTokens()) {
					String member = st.nextToken();
					int idx = member.indexOf('|');
					if (idx != -1) {
						String hostConfigName = member.substring(0, idx);
						String hostAddr = member.substring(idx+1);
						
						Host host = config.getHost(hostConfigName);
						if (host != null) {
							Collection<VMComponentDefn> allProcesses = config.getVMsForHost((HostID)host.getID());
							for (VMComponentDefn process:allProcesses) {
								String strPort = process.getProperty(VMComponentDefnType.CLUSTER_PORT);
								if (strPort != null && strPort.length() > 0) {
									sb.append(hostAddr).append("[").append(Integer.parseInt(strPort)).append("],"); //$NON-NLS-1$ //$NON-NLS-2$
									// this for host controller; will be removed when the hostcontroller is no longer 
									// be a member.
									sb.append(hostAddr).append("[").append(5556).append("],"); //$NON-NLS-1$ //$NON-NLS-2$
								}
							}
						}
					}
				}
				return sb.toString();
			}
		} catch (ConfigurationException e) {
			throw new MetaMatrixRuntimeException(e);
		}  catch (NumberFormatException e) {
			throw new MetaMatrixRuntimeException(e);
		}
		return ""; //$NON-NLS-1$
	}    
}
