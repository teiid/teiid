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
package org.teiid.resource.adapter.infinispan.dsl.schema;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringWriter;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.resource.ResourceException;
import javax.resource.spi.InvalidPropertyException;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.protostream.BaseMarshaller;
import org.infinispan.protostream.FileDescriptorSource;
import org.infinispan.protostream.descriptors.Descriptor;
import org.infinispan.query.remote.client.ProtobufMetadataManagerConstants;
import org.teiid.core.util.StringUtil;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.resource.adapter.infinispan.dsl.InfinispanConnectionImpl;
import org.teiid.resource.adapter.infinispan.dsl.InfinispanManagedConnectionFactory;
import org.teiid.resource.adapter.infinispan.dsl.InfinispanSchemaDefinition;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.infinispan.dsl.InfinispanPlugin;
import org.teiid.translator.object.ClassRegistry;

/**
 * 
 * ProtobBufSchema handles operations related to configuring JDG using protobuf definition files and marshallers.  
 * 
 * @author vhalbert
 */
public class ProtobufSchema  implements InfinispanSchemaDefinition {
	@SuppressWarnings("rawtypes")
	protected Map<String, BaseMarshaller> messageMarshallerMap = null;
	
	@Override
	public void initialize(InfinispanManagedConnectionFactory config, ClassRegistry methodUtil) throws ResourceException {
		List<String> marshallers = StringUtil.getTokens(config.getMessageMarshallers(), ","); //$NON-NLS-1$
		
		Map<String, BaseMarshaller> mmp = new HashMap<String, BaseMarshaller>(marshallers.size());
		
		for (String mm : marshallers) {
			
			List<String> marshallMap = StringUtil.getTokens(mm, ":"); //$NON-NLS-1$
			if (marshallMap.size() != 2) {
				throw new InvalidPropertyException(InfinispanPlugin.Util.gs(InfinispanPlugin.Event.TEIID25031, new Object[] {mm}));
			}
			final String clzName = marshallMap.get(0);
			final String m = marshallMap.get(1);

			try {
				Object bmi = (config.loadClass(m)).newInstance();
				Class ci = config.loadClass(clzName);

				mmp.put(clzName, (BaseMarshaller) bmi); 	

				methodUtil.registerClass(ci);
		
			} catch (InstantiationException e) {
				throw new ResourceException(e);
			} catch (IllegalAccessException e) {	
				throw new ResourceException(e);
			} 
		
		}
		
		messageMarshallerMap=Collections.unmodifiableMap(mmp);
	}

	@Override
	public void registerSchema(InfinispanManagedConnectionFactory config, InfinispanConnectionImpl conn)  throws ResourceException {
		
		String protoBufResource = config.getProtobufDefinitionFile();
		try {
			FileDescriptorSource fds = new FileDescriptorSource();
			fds.addProtoFile("protofile",
					config.getClassLoader().getResourceAsStream(protoBufResource));

			conn.getContext().registerProtoFiles(fds);

			@SuppressWarnings("unchecked")
			RemoteCache<String, String> metadataCache = conn.
						getCache(ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME);


			metadataCache.put(protoBufResource,
					readResource(protoBufResource, config.getClassLoader()));

			String errors = metadataCache
					.get(ProtobufMetadataManagerConstants.ERRORS_KEY_SUFFIX);
			if (errors != null) {
				throw new ResourceException(
						"Error registering Protobuf schema files:\n" + errors);
			}

			for (String clzName : messageMarshallerMap.keySet()) {
				BaseMarshaller m = messageMarshallerMap.get(clzName);
				conn.getContext().registerMarshaller(m);
			}
			
			LogManager.logTrace(LogConstants.CTX_CONNECTOR,
					"=== Registered marshalling with RemoteCacheManager ==="); //$NON-NLS-1$
		} catch (TranslatorException e) {
			throw new ResourceException(e);
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
	

	@Override
	public Descriptor getDecriptor(InfinispanManagedConnectionFactory config, InfinispanConnectionImpl conn, Class<?> clz) throws TranslatorException {
		BaseMarshaller m = conn.getContext().getMarshaller(clz);
		Descriptor d = conn.getContext().getMessageDescriptor(m.getTypeName());
		if (d == null) {
			throw new TranslatorException(InfinispanPlugin.Util.gs(InfinispanPlugin.Event.TEIID25028,  m.getTypeName(), config.getCacheName()));			
		}
		return d;
	}
}
