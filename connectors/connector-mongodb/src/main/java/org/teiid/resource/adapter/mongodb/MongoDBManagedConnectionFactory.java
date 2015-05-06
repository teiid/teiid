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
package org.teiid.resource.adapter.mongodb;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import javax.net.ssl.SSLSocketFactory;
import javax.resource.ResourceException;
import javax.resource.spi.InvalidPropertyException;

import org.teiid.core.BundleUtil;
import org.teiid.resource.spi.BasicConnectionFactory;
import org.teiid.resource.spi.BasicManagedConnectionFactory;

import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;

public class MongoDBManagedConnectionFactory extends BasicManagedConnectionFactory{
	private static final long serialVersionUID = -4945630936957298180L;

	public static final BundleUtil UTIL = BundleUtil.getBundleUtil(MongoDBManagedConnectionFactory.class);

	public enum SecurityType {None, SCRAM_SHA_1, MONGODB_CR, Kerberos, X509};
	private String remoteServerList=null;
	private String username;
	private String password;
	private String database;
	private String securityType = SecurityType.None.name();
	private String authDatabase;
	private Boolean ssl;

	@Override
	@SuppressWarnings("serial")
	public BasicConnectionFactory<MongoDBConnectionImpl> createConnectionFactory() throws ResourceException {
		if (this.remoteServerList == null) {
			throw new InvalidPropertyException(UTIL.getString("no_server")); //$NON-NLS-1$
		}
		if (this.database == null) {
			throw new InvalidPropertyException(UTIL.getString("no_database")); //$NON-NLS-1$
		}

		final List<ServerAddress> servers = getServers();
		if (servers != null) {    
    		return new BasicConnectionFactory<MongoDBConnectionImpl>() {
    			@Override
    			public MongoDBConnectionImpl getConnection() throws ResourceException {
                    return new MongoDBConnectionImpl(
                            MongoDBManagedConnectionFactory.this.database,
                            servers, getCredential(), getOptions());
    			}
    		};
		}
		
		// Make connection using the URI format
        return new BasicConnectionFactory<MongoDBConnectionImpl>() {
            @Override
            public MongoDBConnectionImpl getConnection() throws ResourceException {
                try {
                    return new MongoDBConnectionImpl(MongoDBManagedConnectionFactory.this.database, getConnectionURI());
                } catch (UnknownHostException e) {
                    throw new ResourceException(e);
                }
            }
        };
		
	}
	
	private MongoCredential getCredential() {
	    
        MongoCredential credential = null;        
        if (this.securityType.equals(SecurityType.SCRAM_SHA_1.name())) {
            credential = MongoCredential.createScramSha1Credential(this.username, 
                    (this.authDatabase == null) ? this.database: this.authDatabase, 
                    this.password.toCharArray());            
        } 
        else if (this.securityType.equals(SecurityType.MONGODB_CR.name())) {
            credential = MongoCredential.createMongoCRCredential(this.username, 
                    (this.authDatabase == null) ? this.database: this.authDatabase, 
                    this.password.toCharArray());
        }
        else if (this.securityType.equals(SecurityType.Kerberos.name())) {
            credential = MongoCredential.createGSSAPICredential(this.username);
        }
        else if (this.securityType.equals(SecurityType.X509.name())) {
            credential = MongoCredential.createMongoX509Credential(this.username);
        }        
        else if (this.username != null && this.password != null) {
            // to support legacy pre-3.0 authentication 
            credential = MongoCredential.createMongoCRCredential(
                    MongoDBManagedConnectionFactory.this.username,
                    (this.authDatabase == null) ? this.database: this.authDatabase, 
                    this.password.toCharArray());
        }
        return credential;
	}
	
	private MongoClientOptions getOptions() {
        //if options needed then use URL format
        final MongoClientOptions.Builder builder = MongoClientOptions.builder();
        if (getSsl()) {
            builder.socketFactory(SSLSocketFactory.getDefault());
        }
        return builder.build();
	}
	
	/**
	 * Returns the <code>host:port[;host:port...]</code> list that identifies the remote servers
	 * to include in this cluster;
	 * @return <code>host:port[;host:port...]</code> list
	 */
   public String getRemoteServerList() {
        return this.remoteServerList;
    }

    /**
     * Set the list of remote servers that make up the MongoDB cluster.
     * @param remoteServerList the server list in appropriate <code>server:port;server2:port2</code> format.
     */
    public void setRemoteServerList( String remoteServerList ) {
        this.remoteServerList = remoteServerList;
    }

	public String getUsername() {
		return this.username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getPassword() {
		return this.password;
	}

	public void setPassword(String googlePassword) {
		this.password = googlePassword;
	}
	
    public Boolean getSsl() {
        return this.ssl != null?this.ssl:false;
    }

    public void setSsl(Boolean ssl) {
        this.ssl = ssl;
    }	

	public String getDatabase() {
		return this.database;
	}

	public void setDatabase(String database) {
		this.database = database;
	}
	
    public String getSecurityType() {
        return this.securityType;
    }

    public void setSecurityType(String securityType) {
        this.securityType = securityType;
    }
    
    public String getAuthDatabase() {
        return this.authDatabase;
    }

    public void setAuthDatabase(String database) {
        this.authDatabase = database;
    }    

	protected MongoClientURI getConnectionURI() {
        String serverlist = getRemoteServerList();
        if (serverlist.startsWith("mongodb://")) { //$NON-NLS-1$	    
            return new MongoClientURI(getRemoteServerList());
        }
        return null;
	}

	protected List<ServerAddress> getServers() throws ResourceException {
	    String serverlist = getRemoteServerList();
	    if (!serverlist.startsWith("mongodb://")) { //$NON-NLS-1$
    		List<ServerAddress> addresses = new ArrayList<ServerAddress>();
    		StringTokenizer st = new StringTokenizer(serverlist, ";"); //$NON-NLS-1$
    		while (st.hasMoreTokens()) {
    			String token = st.nextToken();
    			int idx = token.indexOf(':');
    			if (idx < 0) {
    				throw new InvalidPropertyException(UTIL.getString("no_database")); //$NON-NLS-1$
    			}
    			try {
    				addresses.add(new ServerAddress(token.substring(0, idx), Integer.valueOf(token.substring(idx+1))));
    			} catch(UnknownHostException e) {
    				throw new ResourceException(e);
    			}
    		}
    		return addresses;
	    }
	    return null;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((this.remoteServerList == null) ? 0 : this.remoteServerList.hashCode());
		result = prime * result + ((this.database == null) ? 0 : this.database.hashCode());
		result = prime * result + ((this.username == null) ? 0 : this.username.hashCode());
		result = prime * result + ((this.password == null) ? 0 : this.password.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		MongoDBManagedConnectionFactory other = (MongoDBManagedConnectionFactory) obj;
		if (!checkEquals(this.remoteServerList, other.remoteServerList)) {
			return false;
		}
		if (!checkEquals(this.database, other.database)) {
			return false;
		}
		if (!checkEquals(this.username, other.username)) {
			return false;
		}
		if (!checkEquals(this.password, other.password)) {
			return false;
		}
		return true;
	}
}
