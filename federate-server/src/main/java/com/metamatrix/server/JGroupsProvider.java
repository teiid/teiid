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

package com.metamatrix.server;

import java.util.Properties;

import org.jgroups.Channel;
import org.jgroups.ChannelException;
import org.jgroups.JChannel;

import com.google.inject.Provider;
import com.google.inject.Singleton;
import com.metamatrix.common.config.CurrentConfiguration;
import com.metamatrix.common.config.ResourceNames;
import com.metamatrix.common.config.api.ConfigurationModelContainer;
import com.metamatrix.common.config.api.exceptions.ConfigurationException;
import com.metamatrix.common.util.NetUtils;
import com.metamatrix.common.util.VMNaming;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.core.util.StringUtil;

/**
 * This class contains a collection of utilities for managing JGroups objects.
 */
@Singleton
public class JGroupsProvider implements Provider<org.jgroups.Channel> {

	private static String properties;

	// Default Multicast Property values
	private static final String DEFAULT_UDP_MCAST_SUPPORTED  = "true"; //$NON-NLS-1$
	private static final String DEFAULT_UDP_MCAST_ADDR_PREFIX  = "224.224.223."; //$NON-NLS-1$
	private static final String DEFAULT_UDP_MCAST_PORT           = "5555"; //$NON-NLS-1$
    private static final String ALL_INTERFACES_ADDR = "0.0.0.0";  //$NON-NLS-1$

	// Default Gossip Server Property values
	private static final String DEFAULT_PING_GOSSIP_HOST         = "localhost"; //$NON-NLS-1$
	private static final String DEFAULT_PING_GOSSIP_PORT         = "5555"; //$NON-NLS-1$
	private static final String DEFAULT_PING_GOSSIP_REFRESH      = "15000"; //$NON-NLS-1$
	private static final String DEFAULT_PING_TIMEOUT             = "2000"; //$NON-NLS-1$
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
    private static final String UDP_MCAST_MESSAGEBUS_PORT_PROPERTY = "udp.mcast_messagebus_port";     //$NON-NLS-1$
    private static final String UDP_MCAST_JNDI_PORT_PROPERTY       = "udp.mcast_jndi_port";     //$NON-NLS-1$
    private static final String UDP_MCAST_ADDR_PROPERTY      = "udp.mcast_addr"; //$NON-NLS-1$
    private static final String PING_GOSSIP_HOST_PROPERTY    = "ping.gossip_host"; //$NON-NLS-1$
    private static final String PING_GOSSIP_PORT_PROPERTY    = "ping.gossip_port"; //$NON-NLS-1$
    private static final String PING_GOSSIP_REFRESH_PROPERTY      = "ping.gossip_refresh"; //$NON-NLS-1$
    private static final String PING_GOSSIP_TIMEOUT_PROPERTY             = "ping.gossip_timout"; //$NON-NLS-1$
    private static final String PING_GOSSIP_NUM_INITIAL_MEMBERS_PROPERTY = "ping.gossip_initialmembers"; //$NON-NLS-1$
    private static final String JGROUPS_OTHER_SETTINGS_PROPERTY = "jgroups.other.channel.settings"; //$NON-NLS-1$
    private static final String BIND_ADDRESS_PROPERTY = "jgroups.bind.address"; //$NON-NLS-1$
    
    private static final String MESSAGE_BUS_CHANNEL = "MessageBus"; //$NON-NLS-1$
    
    // Multicast Properties
    private static String UDP_MCAST_SUPPORTED      = DEFAULT_UDP_MCAST_SUPPORTED;
    private static String UDP_MCAST_ADDR           = null; 
        //DEFAULT_UDP_MCAST_ADDR;
    private static String UDP_MCAST_PORT           = DEFAULT_UDP_MCAST_PORT;
    
    private static Boolean UDP_RECV_ON_ALL_INTERFACES = Boolean.FALSE;

    // Gossip Server Properties
    private static String PING_GOSSIP_HOST         = DEFAULT_PING_GOSSIP_HOST;
    private static String PING_GOSSIP_PORT         = DEFAULT_PING_GOSSIP_PORT;
    private static String PING_GOSSIP_REFRESH      = DEFAULT_PING_GOSSIP_REFRESH;
    private static String PING_TIMEOUT             = DEFAULT_PING_TIMEOUT;
    private static String PING_NUM_INITIAL_MEMBERS = DEFAULT_PING_NUM_INITIAL_MEMBERS;
    
    private static String JGROUPS_OTHER_SETTINGS = DEFAULT_JGROUPS_OTHER_SETTINGS;
    
    private static String BIND_ADDRESS = null;
    
    /**
	 * Helper method to create a JChannel.
	 * Properties needed to create the JChannel are read from current configuration.
	 * 
	 * @param String name - Name of the group the channel will join.
	 * @throws ChannelException if there is an error creating the JChannel
	 */
    
    public Channel get() {
		try {
			JChannel channel=new JChannel(getChannelProperties(MESSAGE_BUS_CHANNEL));
			String systemName = null;
			try {
			    systemName = CurrentConfiguration.getSystemName();
			} catch (ConfigurationException err) {
			    systemName = ConfigurationModelContainer.DEFAULT_SYSTEM_NAME;
			}
			channel.connect(systemName + "_" + MESSAGE_BUS_CHANNEL); //$NON-NLS-1$
			return channel;
		} catch (ChannelException e) {
			throw new MetaMatrixRuntimeException(e);
		}
	}

	private static synchronized String getChannelProperties(String name) {
        // Based on the channel name "name" get the property name for the appropriate channel port.
        String port = null;
        if (MESSAGE_BUS_CHANNEL.equals(name)) {
            port = UDP_MCAST_MESSAGEBUS_PORT_PROPERTY;
        } else {
            port = UDP_MCAST_JNDI_PORT_PROPERTY;
        }
        
	    try {
	        Properties configProps = CurrentConfiguration.getResourceProperties(ResourceNames.JGROUPS);

            UDP_MCAST_SUPPORTED      =  configProps.getProperty(UDP_MCAST_SUPPORTED_PROPERTY, DEFAULT_UDP_MCAST_SUPPORTED);

	        UDP_MCAST_PORT           = configProps.getProperty(port, DEFAULT_UDP_MCAST_PORT);
	        PING_GOSSIP_HOST         = configProps.getProperty(PING_GOSSIP_HOST_PROPERTY, DEFAULT_PING_GOSSIP_HOST);
	        PING_GOSSIP_PORT         = configProps.getProperty(PING_GOSSIP_PORT_PROPERTY, DEFAULT_PING_GOSSIP_PORT);
	        PING_GOSSIP_REFRESH      = configProps.getProperty(PING_GOSSIP_REFRESH_PROPERTY, DEFAULT_PING_GOSSIP_REFRESH);
	        PING_TIMEOUT             = configProps.getProperty(PING_GOSSIP_TIMEOUT_PROPERTY, DEFAULT_PING_TIMEOUT);
	        PING_NUM_INITIAL_MEMBERS = configProps.getProperty(PING_GOSSIP_NUM_INITIAL_MEMBERS_PROPERTY, DEFAULT_PING_NUM_INITIAL_MEMBERS);
            JGROUPS_OTHER_SETTINGS = configProps.getProperty(JGROUPS_OTHER_SETTINGS_PROPERTY, DEFAULT_JGROUPS_OTHER_SETTINGS);

            BIND_ADDRESS = getBindAddress();
            
            
            if (UDP_MCAST_ADDR == null) {
                UDP_MCAST_ADDR           = configProps.getProperty(UDP_MCAST_ADDR_PROPERTY);
                if (UDP_MCAST_ADDR == null || UDP_MCAST_ADDR.length() == 0) {
                    // use the last node of the local machine address as the last node
                    // of the DEFAULT address.
                    String lastNode = StringUtil.getLastToken(NetUtils.getHostAddress(), ".");//$NON-NLS-1$
                    
                    UDP_MCAST_ADDR = DEFAULT_UDP_MCAST_ADDR_PREFIX + lastNode;
                }
            }            
            
        } catch (Exception e) {
            // If there is an error reading currentConfig then use default properties. 
        }
        
		if (UDP_MCAST_SUPPORTED.equalsIgnoreCase("true")) { //$NON-NLS-1$
            
            if (UDP_RECV_ON_ALL_INTERFACES.booleanValue()) {
                properties = 
                    "UDP(mcast_addr="+UDP_MCAST_ADDR+";mcast_port="+UDP_MCAST_PORT+";ip_ttl=32;receive_on_all_interfaces=" + UDP_MCAST_SUPPORTED.toString() + ";" + //$NON-NLS-1$//$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
                    "mcast_send_buf_size=150000;mcast_recv_buf_size=80000):" + //$NON-NLS-1$
                    "PING(timeout="+PING_TIMEOUT+";num_initial_members="+PING_NUM_INITIAL_MEMBERS+"):";//$NON-NLS-1$ //$NON-NLS-2$//$NON-NLS-3$
                
                
            } else {
    			properties = 
    				"UDP(mcast_addr="+UDP_MCAST_ADDR+";mcast_port="+UDP_MCAST_PORT+";ip_ttl=32;bind_addr="+BIND_ADDRESS+";"+   //$NON-NLS-1$//$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
    				"mcast_send_buf_size=150000;mcast_recv_buf_size=80000):" + //$NON-NLS-1$
    				"PING(timeout="+PING_TIMEOUT+";num_initial_members="+PING_NUM_INITIAL_MEMBERS+"):";//$NON-NLS-1$ //$NON-NLS-2$//$NON-NLS-3$
            }
                //timeout=2000;num_initial_members=3):"; //$NON-NLS-1$
		} else {
			properties = 
				"UDP(ip_mcast=false;mcast_addr="+UDP_MCAST_ADDR+";mcast_port="+UDP_MCAST_PORT+";bind_addr="+BIND_ADDRESS+"):" + //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
				"PING(gossip_host="+PING_GOSSIP_HOST+";gossip_port="+PING_GOSSIP_PORT+";gossip_refresh="+PING_GOSSIP_REFRESH+";" + //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
				"timeout="+PING_TIMEOUT+";num_initial_members="+PING_NUM_INITIAL_MEMBERS+"):";  //$NON-NLS-1$ //$NON-NLS-2$//$NON-NLS-3$
		}
		properties += JGROUPS_OTHER_SETTINGS;

		return properties;
	}
    
    private static final String getBindAddress() {
        String bindAddress = System.getProperty(JGroupsProvider.BIND_ADDRESS_PROPERTY);
        // check for command line system property being set from vm.starter.command for jgroup
        if (bindAddress == null)  { 
            bindAddress = VMNaming.getBindAddress();
        }
            
        if (bindAddress == null) {
            System.out.println("WARNING: Unable to set " + JGroupsProvider.BIND_ADDRESS_PROPERTY + ", will set to 127.0.0.1"); //$NON-NLS-1$ //$NON-NLS-2$
            bindAddress = "127.0.0.1";//$NON-NLS-1$
        }
        
        if (bindAddress.equalsIgnoreCase(ALL_INTERFACES_ADDR)) {
            UDP_RECV_ON_ALL_INTERFACES = Boolean.TRUE;
        }

        return bindAddress;

    }
	
}
