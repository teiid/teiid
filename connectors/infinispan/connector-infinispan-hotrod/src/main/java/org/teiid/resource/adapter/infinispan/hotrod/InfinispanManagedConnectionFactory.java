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
package org.teiid.resource.adapter.infinispan.hotrod;

import java.io.IOException;
import java.util.HashSet;

import javax.resource.ResourceException;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.UnsupportedCallbackException;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.marshall.ProtoStreamMarshaller;
import org.infinispan.protostream.FileDescriptorSource;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.query.remote.client.ProtobufMetadataManagerConstants;
import org.teiid.core.BundleUtil;
import org.teiid.infinispan.api.ProtobufResource;
import org.teiid.resource.spi.BasicConnectionFactory;
import org.teiid.resource.spi.BasicManagedConnectionFactory;
import org.teiid.translator.TranslatorException;


public class InfinispanManagedConnectionFactory extends BasicManagedConnectionFactory {
    public static final BundleUtil UTIL = BundleUtil.getBundleUtil(InfinispanManagedConnectionFactory.class);
    private static final long serialVersionUID = -4791974803005018658L;

    private String remoteServerList;
    private String cacheName;
    
    // security
    private String[] saslAllowed = {"CRAM-MD5", "DIGEST-MD5", "PLAIN"}; 
    private String saslMechanism;
    private String userName;
    private String password;
    private String authenticationRealm;
    private String authenticationServerName;

    private String trustStoreFileName = System.getProperty("javax.net.ssl.trustStore");
    private String trustStorePassword = System.getProperty("javax.net.ssl.trustStorePassword");
    private String keyStoreFileName = System.getProperty("javax.net.ssl.keyStore");
    private String keyStorePassword = System.getProperty("javax.net.ssl.keyStorePassword");
    
    public String getRemoteServerList() {
        return remoteServerList;
    }

    public void setRemoteServerList(String remoteServerList) {
        this.remoteServerList = remoteServerList;
    }

    public String getCacheName() {
        return cacheName;
    }

    public void setCacheName(String cacheName) {
        this.cacheName = cacheName;
    }

    @Override
    public BasicConnectionFactory<InfinispanConnectionImpl> createConnectionFactory()
            throws ResourceException {
        return new InfinispanConnectionFactory();
    }

    class InfinispanConnectionFactory extends BasicConnectionFactory<InfinispanConnectionImpl> {
        private static final long serialVersionUID = 1064143496037686580L;
        private RemoteCacheManager cacheManager;
        private HashSet<String> registeredProtoFiles = new HashSet<>();
        private SerializationContext ctx;

        public InfinispanConnectionFactory() throws ResourceException {
            try {
                ConfigurationBuilder builder = new ConfigurationBuilder();
                builder.addServers(remoteServerList);
                builder.marshaller(new ProtoStreamMarshaller());
                
                handleSecurity(builder);

                // note this object is expensive, so there needs to only one
                // instance for the JVM, in this case one per RA instance.
                this.cacheManager = new RemoteCacheManager(builder.build());

                // register default marshellers
                /*
                SerializationContext ctx = ProtoStreamMarshaller.getSerializationContext(this.cacheManager);
                FileDescriptorSource fds = new FileDescriptorSource();
                ctx.registerProtoFiles(fds);
                */
                this.cacheManager.start();
                this.ctx = ProtoStreamMarshaller.getSerializationContext(this.cacheManager);
            } catch (Throwable e) {
                throw new ResourceException(e);
            }
        }

        public void handleSecurity(ConfigurationBuilder builder) throws ResourceException {
            if (saslMechanism != null && supportedSasl(saslMechanism)) {                    
                if (userName == null) {
                    throw new ResourceException(UTIL.getString("no_user"));
                }
                if (password == null) {
                    throw new ResourceException(UTIL.getString("no_pass"));
                }
                if (authenticationRealm == null) {
                    throw new ResourceException(UTIL.getString("no_realm"));
                }
                if (authenticationServerName == null) {
                    throw new ResourceException(UTIL.getString("no_auth_server"));
                }
                builder.security().authentication().enable().saslMechanism(saslMechanism).username(userName)
                        .realm(authenticationRealm).password(password).serverName(authenticationServerName);
            } else if (saslMechanism != null && saslMechanism.equals("EXTERNAL")) {

                if (keyStoreFileName == null || keyStoreFileName.isEmpty()) {
                    throw new ResourceException(UTIL.getString("no_keystore"));
                }

                if (keyStorePassword == null) {
                    throw new ResourceException(UTIL.getString("no_keystore_pass"));
                }
                
                if (trustStoreFileName == null &&  trustStorePassword.isEmpty()) {
                    throw new ResourceException(UTIL.getString("no_truststore"));
                }

                if (trustStorePassword == null) {
                    throw new ResourceException(UTIL.getString("no_truststore_pass"));
                }
                
                CallbackHandler callback = new CallbackHandler() {
                    @Override
                    public void handle(Callback[] callbacks)
                            throws IOException, UnsupportedCallbackException {
                    }
                };                    
                builder.security().authentication().enable().saslMechanism("EXTERNAL").callbackHandler(callback)
                        .ssl().enable().keyStoreFileName(keyStoreFileName)
                        .keyStorePassword(keyStorePassword.toCharArray()).trustStoreFileName(trustStoreFileName)
                        .trustStorePassword(trustStorePassword.toCharArray());
            }
        }

        private boolean supportedSasl(String saslMechanism) {
            for (String supported : saslAllowed) {
                if (supported.equals(saslMechanism)) {
                    return true;
                }
            }
            return false;
        }

        public void registerProtobufFile(ProtobufResource protobuf) throws TranslatorException {
            try {
                if (protobuf != null && !this.registeredProtoFiles.contains(protobuf.getIdentifier())) {
                    // client side
                    this.ctx.registerProtoFiles(FileDescriptorSource.fromString(protobuf.getIdentifier(), protobuf.getContents()));

                    // server side
                    RemoteCache<String, String> metadataCache = this.cacheManager
                            .getCache(ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME);
                    if (metadataCache != null && metadataCache.get(protobuf.getIdentifier()) == null) {
                        metadataCache.put(protobuf.getIdentifier(), protobuf.getContents());
                        String errors = metadataCache.get(ProtobufMetadataManagerConstants.ERRORS_KEY_SUFFIX);
                        if (errors != null) {
                           throw new TranslatorException(InfinispanManagedConnectionFactory.UTIL.getString("proto_error", errors));
                        }
                        this.registeredProtoFiles.add(protobuf.getIdentifier());
                    }
                }
            } catch(Throwable t) {
                throw new TranslatorException(t);
            }
        }

        @Override
        public InfinispanConnectionImpl getConnection() throws ResourceException {
            return new InfinispanConnectionImpl(this.cacheManager, cacheName,this.ctx, this);
        }
    }

    public String getSaslMechanism() {
        return saslMechanism;
    }

    public void setSaslMechanism(String saslMechanism) {
        this.saslMechanism = saslMechanism;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getAuthenticationRealm() {
        return authenticationRealm;
    }

    public void setAuthenticationRealm(String authenticationRealm) {
        this.authenticationRealm = authenticationRealm;
    }

    public String getAuthenticationServerName() {
        return authenticationServerName;
    }

    public void setAuthenticationServerName(String authenticationServerName) {
        this.authenticationServerName = authenticationServerName;
    }

    public String getTrustStoreFileName() {
        return trustStoreFileName;
    }

    public void setTrustStoreFileName(String trustStoreFileName) {
        this.trustStoreFileName = trustStoreFileName;
    }

    public String getTrustStorePassword() {
        return trustStorePassword;
    }

    public void setTrustStorePassword(String trustStorePassword) {
        this.trustStorePassword = trustStorePassword;
    }

    public String getKeyStoreFileName() {
        return keyStoreFileName;
    }

    public void setKeyStoreFileName(String keyStoreFileName) {
        this.keyStoreFileName = keyStoreFileName;
    }

    public String getKeyStorePassword() {
        return keyStorePassword;
    }

    public void setKeyStorePassword(String keyStorePassword) {
        this.keyStorePassword = keyStorePassword;
    }
    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((remoteServerList == null) ? 0 : remoteServerList.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        InfinispanManagedConnectionFactory other = (InfinispanManagedConnectionFactory) obj;
        if (remoteServerList == null) {
            if (other.remoteServerList != null)
                return false;
        } else if (!remoteServerList.equals(other.remoteServerList))
            return false;
        return true;
    }    
}
