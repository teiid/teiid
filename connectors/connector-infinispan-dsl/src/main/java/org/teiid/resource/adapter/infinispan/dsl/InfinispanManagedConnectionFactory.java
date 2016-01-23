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
package org.teiid.resource.adapter.infinispan.dsl;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringWriter;
import java.util.Properties;

import javax.resource.ResourceException;
import javax.resource.spi.InvalidPropertyException;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.marshall.ProtoStreamMarshaller;
import org.infinispan.protostream.BaseMarshaller;
import org.infinispan.protostream.FileDescriptorSource;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.query.remote.client.ProtobufMetadataManagerConstants;
import org.teiid.core.BundleUtil;
import org.teiid.core.util.PropertiesUtils;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.resource.adapter.infinispan.dsl.base.AbstractInfinispanManagedConnectionFactory;
import org.teiid.translator.infinispan.dsl.InfinispanPlugin;

public class InfinispanManagedConnectionFactory extends AbstractInfinispanManagedConnectionFactory {
	public static final BundleUtil UTIL = BundleUtil.getBundleUtil(InfinispanManagedConnectionFactory.class);

	/**
	 */
	private static final long serialVersionUID = 1L;

	@Override
	protected RemoteCacheManager createRemoteCacheWrapperFromProperties(
			ClassLoader classLoader) throws ResourceException {

		File f = new File(this.getHotRodClientPropertiesFile());
		if (!f.exists()) {
			throw new InvalidPropertyException(
					InfinispanManagedConnectionFactory.UTIL.getString(
							"clientPropertiesFileDoesNotExist",
							f.getAbsoluteFile()));

		}
		try {
			Properties props = PropertiesUtils.load(f.getAbsolutePath());

			LogManager
					.logInfo(
							LogConstants.CTX_CONNECTOR,
							"=== Using RemoteCacheManager (created from properties file " + f.getAbsolutePath() + ") ==="); //$NON-NLS-1$

			return createRemoteCacheWrapper(props, classLoader);

		} catch (Exception err) {
			throw new ResourceException(err);
		}

	}

	@Override
	protected RemoteCacheManager createRemoteCacheWrapperFromServerList(
			ClassLoader classLoader) throws ResourceException {

		Properties props = new Properties();
		props.put(
				"infinispan.client.hotrod.server_list", this.getRemoteServerList()); //$NON-NLS-1$

		LogManager.logInfo(LogConstants.CTX_CONNECTOR,
				"=== Using RemoteCacheManager (loaded by serverlist) ==="); //$NON-NLS-1$

		return createRemoteCacheWrapper(props, classLoader);
	}

	protected RemoteCacheManager createRemoteCacheWrapper(Properties props,
			ClassLoader classLoader) throws ResourceException {
		RemoteCacheManager remoteCacheManager;
		try {
			ConfigurationBuilder cb = new ConfigurationBuilder();
			cb.marshaller(new ProtoStreamMarshaller());
			cb.withProperties(props);
			if (classLoader != null)
				cb.classLoader(classLoader);
			remoteCacheManager = new RemoteCacheManager(cb.build(), true);

		} catch (Exception err) {
			throw new ResourceException(err);
		}

		return remoteCacheManager;

	}

	@Override
	protected SerializationContext getContext() {
		return ProtoStreamMarshaller.getSerializationContext(this
				.getCacheContainer());
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	protected void registerMarshallers(SerializationContext ctx,
			RemoteCacheManager cc, ClassLoader cl) throws ResourceException {

		String protoBufResource = getProtobufDefinitionFile();
		try {
			FileDescriptorSource fds = new FileDescriptorSource();
			fds.addProtoFile("protofile",
					cl.getResourceAsStream(protoBufResource));

			ctx.registerProtoFiles(fds);

			RemoteCache<String, String> metadataCache = cc
					.getCache(ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME);
			metadataCache.put(protoBufResource,
					readResource(protoBufResource, cl));

			String errors = metadataCache
					.get(ProtobufMetadataManagerConstants.ERRORS_KEY_SUFFIX);
			if (errors != null) {
				throw new ResourceException(
						"Error registering Protobuf schema files:\n" + errors);
			}

			for (String clzName : messageMarshallerMap.keySet()) {
				BaseMarshaller m = messageMarshallerMap.get(clzName);
				ctx.registerMarshaller(m);
			}
			
			LogManager.logTrace(LogConstants.CTX_CONNECTOR,
					"=== Registered marshalling with RemoteCacheManager ==="); //$NON-NLS-1$

		} catch (IOException e) {
			throw new ResourceException(
					InfinispanPlugin.Util.gs(InfinispanPlugin.Event.TEIID25032),
					e);
		}
	}

	private String readResource(String resourcePath, ClassLoader cl)
			throws IOException {
		InputStream is = cl.getResourceAsStream(resourcePath);
		try {
			final Reader reader = new InputStreamReader(is, "UTF-8");
			StringWriter writer = new StringWriter();
			char[] buf = new char[1024];
			int len;
			while ((len = reader.read(buf)) != -1) {
				writer.write(buf, 0, len);
			}
			return writer.toString();
		} finally {
			is.close();
		}
	}
}
