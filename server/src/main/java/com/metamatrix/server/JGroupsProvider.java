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
import java.util.Properties;

import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.OperationsException;

import org.jgroups.ChannelException;
import org.jgroups.JChannel;
import org.jgroups.mux.Multiplexer;

import com.google.inject.Provider;
import com.google.inject.Singleton;
import com.metamatrix.common.config.CurrentConfiguration;
import com.metamatrix.common.config.ResourceNames;
import com.metamatrix.common.config.api.exceptions.ConfigurationException;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.util.LogCommonConstants;
import com.metamatrix.core.MetaMatrixRuntimeException;

/**
 * This class contains a collection of utilities for managing JGroups objects.
 */
@Singleton
public class JGroupsProvider implements Provider<org.jgroups.mux.Multiplexer> {

	// Default Multicast Property values
	private static final String DEFAULT_UDP_MCAST_SUPPORTED = "true"; //$NON-NLS-1$
	private static final String DEFAULT_UDP_MCAST_ADDR_PREFIX = "224."; //$NON-NLS-1$
	private static final String DEFAULT_UDP_MCAST_PORT = "5555"; //$NON-NLS-1$
	private static final String ALL_INTERFACES_ADDR = "0.0.0.0"; //$NON-NLS-1$

	// Default Gossip Server Property values
	private static final String DEFAULT_PING_GOSSIP_HOST = "localhost"; //$NON-NLS-1$
	private static final String DEFAULT_PING_GOSSIP_PORT = "5555"; //$NON-NLS-1$
	private static final String DEFAULT_PING_GOSSIP_REFRESH = "15000"; //$NON-NLS-1$
	private static final String DEFAULT_PING_TIMEOUT = "2000"; //$NON-NLS-1$
	private static final String DEFAULT_PING_NUM_INITIAL_MEMBERS = "3"; //$NON-NLS-1$
    
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
                
    private static final String UDP_MCAST_SUPPORTED_PROPERTY = "udp.multicast_supported"; //$NON-NLS-1$
	private static final String UDP_MCAST_MESSAGEBUS_PORT_PROPERTY = "udp.mcast_messagebus_port"; //$NON-NLS-1$
	private static final String UDP_MCAST_ADDR_PROPERTY = "udp.mcast_addr"; //$NON-NLS-1$
	private static final String PING_GOSSIP_HOST_PROPERTY = "ping.gossip_host"; //$NON-NLS-1$
	private static final String PING_GOSSIP_PORT_PROPERTY = "ping.gossip_port"; //$NON-NLS-1$
	private static final String PING_GOSSIP_REFRESH_PROPERTY = "ping.gossip_refresh"; //$NON-NLS-1$
	private static final String PING_GOSSIP_TIMEOUT_PROPERTY = "ping.gossip_timout"; //$NON-NLS-1$
	private static final String PING_GOSSIP_NUM_INITIAL_MEMBERS_PROPERTY = "ping.gossip_initialmembers"; //$NON-NLS-1$
	private static final String JGROUPS_OTHER_SETTINGS_PROPERTY = "jgroups.other.channel.settings"; //$NON-NLS-1$
	private static final String BIND_ADDRESS_PROPERTY = "jgroups.bind.address"; //$NON-NLS-1$
    
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

			String udpMulticastSupported =  configProps.getProperty(UDP_MCAST_SUPPORTED_PROPERTY, DEFAULT_UDP_MCAST_SUPPORTED);

			String udpMulticastPort = configProps.getProperty(UDP_MCAST_MESSAGEBUS_PORT_PROPERTY, DEFAULT_UDP_MCAST_PORT);
			String pingGossipHost         = configProps.getProperty(PING_GOSSIP_HOST_PROPERTY, DEFAULT_PING_GOSSIP_HOST);
			String pingGossipPort         = configProps.getProperty(PING_GOSSIP_PORT_PROPERTY, DEFAULT_PING_GOSSIP_PORT);
			String pingGossipRerefresh      = configProps.getProperty(PING_GOSSIP_REFRESH_PROPERTY, DEFAULT_PING_GOSSIP_REFRESH);
			String pingTimeout             = configProps.getProperty(PING_GOSSIP_TIMEOUT_PROPERTY, DEFAULT_PING_TIMEOUT);
			String pingInitialMemberCount = configProps.getProperty(PING_GOSSIP_NUM_INITIAL_MEMBERS_PROPERTY, DEFAULT_PING_NUM_INITIAL_MEMBERS);
			String otherSettings = configProps.getProperty(JGROUPS_OTHER_SETTINGS_PROPERTY, DEFAULT_JGROUPS_OTHER_SETTINGS);

			String bindAddress = getBindAddress();
			boolean multicastOnAllInterfaces = bindAddress.equalsIgnoreCase(ALL_INTERFACES_ADDR);

		    String udpMulticastAddress = configProps.getProperty(UDP_MCAST_ADDR_PROPERTY);
		    if (udpMulticastAddress == null || udpMulticastAddress.length() == 0) {
		    	String currentAddr = CurrentConfiguration.getInstance().getBindAddress();
		    	if (currentAddr.indexOf('.') != -1) {
		    		String lastNode = currentAddr.substring(currentAddr.indexOf('.')+1);
		    		udpMulticastAddress = DEFAULT_UDP_MCAST_ADDR_PREFIX + lastNode;
		    	}
		    	else {
		    		throw new ConfigurationException("Failed to set default multicast address"); //$NON-NLS-1$
		    	}
		    }
		
			if (udpMulticastSupported.equalsIgnoreCase("true")) { //$NON-NLS-1$
	            if (multicastOnAllInterfaces) {
	                properties = 
	                    "UDP(mcast_addr="+udpMulticastAddress+";mcast_port="+udpMulticastPort+";ip_ttl=32;receive_on_all_interfaces=" + udpMulticastSupported.toString() + ";" + //$NON-NLS-1$//$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
	                    "mcast_send_buf_size=150000;mcast_recv_buf_size=80000):" + //$NON-NLS-1$
	                    "PING(timeout="+pingTimeout+";num_initial_members="+pingInitialMemberCount+"):";//$NON-NLS-1$ //$NON-NLS-2$//$NON-NLS-3$
	                
	                
	            } else {
	    			properties = 
	    				"UDP(mcast_addr="+udpMulticastAddress+";mcast_port="+udpMulticastPort+";ip_ttl=32;bind_addr="+bindAddress+";"+   //$NON-NLS-1$//$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
	    				"mcast_send_buf_size=150000;mcast_recv_buf_size=80000):" + //$NON-NLS-1$
	    				"PING(timeout="+pingTimeout+";num_initial_members="+pingInitialMemberCount+"):";//$NON-NLS-1$ //$NON-NLS-2$//$NON-NLS-3$
	            }
			} else {
				properties = 
					"UDP(ip_mcast=false;mcast_addr="+udpMulticastAddress+";mcast_port="+udpMulticastPort+";bind_addr="+bindAddress+"):" + //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
					"PING(gossip_host="+pingGossipHost+";gossip_port="+pingGossipPort+";gossip_refresh="+pingGossipRerefresh+";" + //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
					"timeout="+pingTimeout+";num_initial_members="+pingInitialMemberCount+"):";  //$NON-NLS-1$ //$NON-NLS-2$//$NON-NLS-3$
			}
			properties += otherSettings;
			return properties;
			
		} catch (ConfigurationException e) {
			throw new MetaMatrixRuntimeException(e);
		}            
	}
    
    private final String getBindAddress() {
        // check for command line system property being set from vm.starter.command for jgroup
        String bindAddress = System.getProperty(JGroupsProvider.BIND_ADDRESS_PROPERTY);
        if (bindAddress == null)  { 
            bindAddress = CurrentConfiguration.getInstance().getBindAddress();
        }
            
        if (bindAddress == null) {
            LogManager.logWarning(LogCommonConstants.CTX_MESSAGE_BUS,"WARNING: Unable to set " + JGroupsProvider.BIND_ADDRESS_PROPERTY + ", will set to 127.0.0.1"); //$NON-NLS-1$ //$NON-NLS-2$
            bindAddress = "127.0.0.1";//$NON-NLS-1$
        }
        return bindAddress;
    }
}
