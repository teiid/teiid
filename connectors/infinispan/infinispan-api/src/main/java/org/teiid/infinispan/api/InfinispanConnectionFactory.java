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

package org.teiid.infinispan.api;

import java.io.Closeable;
import java.io.IOException;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.UnsupportedCallbackException;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.AuthenticationConfigurationBuilder;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.configuration.TransactionMode;
import org.infinispan.client.hotrod.marshall.MarshallerUtil;
import org.infinispan.commons.api.BasicCache;
import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.marshall.ProtoStreamMarshaller;
import org.infinispan.commons.tx.lookup.TransactionManagerLookup;
import org.infinispan.protostream.FileDescriptorSource;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.query.remote.client.ProtobufMetadataManagerConstants;
import org.teiid.translator.TranslatorException;

public class InfinispanConnectionFactory implements Closeable {

    private InfinispanConfiguration config;
    private SerializationContext ctx;
    private TransactionMode transactionMode;
    private RemoteCacheManager cacheManager;
    private RemoteCacheManager scriptCacheManager;
    private TeiidMarshallerProvider teiidMarshallerProvider = new TeiidMarshallerProvider();

    public InfinispanConnectionFactory(InfinispanConfiguration config, TransactionManagerLookup transactionManagerLookup) throws TranslatorException {
        this.config = config;
        if (config.getTransactionMode() != null) {
            this.transactionMode = TransactionMode.valueOf(config.getTransactionMode());
        }
        if (config.getRemoteServerList() == null) {
            throw new TranslatorException("The remoteServerList is required"); //$NON-NLS-1$
        }
        buildCacheManager(transactionManagerLookup);
        buildScriptCacheManager();
    }

    private void buildCacheManager(TransactionManagerLookup transactionManagerLookup) throws TranslatorException {
        try {
            ConfigurationBuilder builder = new ConfigurationBuilder();
            builder.addServers(config.getRemoteServerList());
            builder.marshaller(new ProtoStreamMarshaller() {
                @Override
                protected ByteBuffer objectToBuffer(Object o,
                        int estimatedSize)
                        throws IOException, InterruptedException {
                    try {
                        if (o instanceof InfinispanDocument) {
                            TeiidMarshallerProvider.setCurrentDocument((InfinispanDocument)o);
                        }
                        return super.objectToBuffer(o, estimatedSize);
                    } finally {
                        TeiidMarshallerProvider.setCurrentDocument(null);
                    }
                }
            });

            if (transactionMode != null) {
                builder.transaction()
                    .transactionMode(transactionMode)
                    .transactionManagerLookup(transactionManagerLookup);
            }

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
            this.ctx = MarshallerUtil.getSerializationContext(this.cacheManager);
            this.ctx.registerMarshallerProvider(teiidMarshallerProvider);
        } catch (Exception e) {
            throw new TranslatorException(e);
        }
    }

    private void buildScriptCacheManager() throws TranslatorException {
        try {
            ConfigurationBuilder builder = new ConfigurationBuilder();
            builder.addServers(config.getRemoteServerList());
            //builder.marshaller(new GenericJBossMarshaller());
            handleSecurity(builder);

            // note this object is expensive, so there needs to only one
            // instance for the JVM, in this case one per RA instance.
            this.scriptCacheManager = new RemoteCacheManager(builder.build());
            this.scriptCacheManager.start();
        } catch (Throwable e) {
            throw new TranslatorException(e);
        }
    }

    public void handleSecurity(ConfigurationBuilder builder) throws TranslatorException {
        if (config.getSaslMechanism() != null && config.getSaslMechanism().equals("EXTERNAL")) {

            if (config.getKeyStoreFileName() == null || config.getKeyStoreFileName().isEmpty()) {
                throw new TranslatorException(InfinispanPlugin.Util.getString("no_keystore"));
            }

            if (config.getKeyStorePassword() == null) {
                throw new TranslatorException(InfinispanPlugin.Util.getString("no_keystore_pass"));
            }

            if (config.getTrustStoreFileName() == null ||  config.getTrustStoreFileName().isEmpty()) {
                throw new TranslatorException(InfinispanPlugin.Util.getString("no_truststore"));
            }

            if (config.getTrustStorePassword() == null) {
                throw new TranslatorException(InfinispanPlugin.Util.getString("no_truststore_pass"));
            }

            CallbackHandler callback = new CallbackHandler() {
                @Override
                public void handle(Callback[] callbacks)
                        throws IOException, UnsupportedCallbackException {
                }
            };
            builder.security().authentication().enable().saslMechanism("EXTERNAL").callbackHandler(callback)
                    .ssl().enable().keyStoreFileName(config.getKeyStoreFileName())
                    .keyStorePassword(config.getKeyStorePassword().toCharArray()).trustStoreFileName(config.getTrustStoreFileName())
                    .trustStorePassword(config.getTrustStorePassword().toCharArray());
        } else if (config.getSaslMechanism() != null || config.getUsername() != null) {
            if (config.getUsername() == null) {
                throw new TranslatorException(InfinispanPlugin.Util.getString("no_user"));
            }
            if (config.getPassword() == null) {
                throw new TranslatorException(InfinispanPlugin.Util.getString("no_pass"));
            }
            if (config.getAuthenticationRealm() == null) {
                throw new TranslatorException(InfinispanPlugin.Util.getString("no_realm"));
            }
            if (config.getAuthenticationServerName() == null) {
                throw new TranslatorException(InfinispanPlugin.Util.getString("no_auth_server"));
            }
            AuthenticationConfigurationBuilder authBuilder = builder.security()
                    .authentication().enable()
                    .saslMechanism(config.getSaslMechanism())
                    .username(config.getUsername())
                    .realm(config.getAuthenticationRealm())
                    .password(config.getPassword())
                    .serverName(config.getAuthenticationServerName());
            if (config.getSaslMechanism() != null) {
                authBuilder.saslMechanism(config.getSaslMechanism());
            }
        }
    }

    public TransactionMode getTransactionMode() {
        return transactionMode;
    }

    public void registerProtobufFile(ProtobufResource protobuf,
            BasicCache<String, String> metadataCache) throws TranslatorException {
        try {
            // client side
            this.ctx.registerProtoFiles(FileDescriptorSource.fromString(protobuf.getIdentifier(), protobuf.getContents()));

            // server side
            if (metadataCache != null) {
                metadataCache.put(protobuf.getIdentifier(), protobuf.getContents());
                String errors = metadataCache.get(ProtobufMetadataManagerConstants.ERRORS_KEY_SUFFIX);
                // ispn removes leading '/' in a string in the results
                String protoSchemaIdent = (protobuf.getIdentifier().startsWith("/"))
                        ? protobuf.getIdentifier().substring(1)
                        : protobuf.getIdentifier();
                if (errors != null && isProtoSchemaInErrors(protoSchemaIdent, errors)) {
                   throw new TranslatorException(InfinispanPlugin.Util.gs("proto_error", errors));
                }
            }
        } catch(Throwable t) {
            throw new TranslatorException(t);
        }
    }

    private boolean isProtoSchemaInErrors(String ident, String errors) {
        for (String s : errors.split("\n")) {
            if (s.trim().startsWith(ident)) {
                return true;
            }
        }
        return false;
    }

    public RemoteCacheManager getCacheManager() {
        return cacheManager;
    }

    public RemoteCacheManager getScriptCacheManager() {
        return scriptCacheManager;
    }

    public InfinispanConfiguration getConfig() {
        return config;
    }

    public TeiidMarshallerProvider getTeiidMarshallerProvider() {
        return teiidMarshallerProvider;
    }

    @Override
    public void close() throws IOException {
        if (this.cacheManager != null) {
            this.cacheManager.close();
        }
        if (this.scriptCacheManager != null) {
            this.scriptCacheManager.close();
        }
    }

}
