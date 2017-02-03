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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.resource.ResourceException;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.protostream.BaseMarshaller;
import org.infinispan.protostream.annotations.ProtoSchemaBuilder;
import org.infinispan.protostream.annotations.ProtoSchemaBuilderException;
import org.infinispan.protostream.descriptors.Descriptor;
import org.infinispan.query.remote.client.ProtobufMetadataManagerConstants;
import org.teiid.core.util.StringUtil;
import org.teiid.resource.adapter.infinispan.dsl.InfinispanConnectionImpl;
import org.teiid.resource.adapter.infinispan.dsl.InfinispanManagedConnectionFactory;
import org.teiid.resource.adapter.infinispan.dsl.InfinispanSchemaDefinition;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.infinispan.dsl.InfinispanPlugin;
import org.teiid.translator.object.ClassRegistry;

/**
 *  AnnotationSchema handles operations related to configuring JDG using pojo's that have been annotated.. 
 * 
 * @author vhalbert
 *
 */
public class AnnotationSchema implements InfinispanSchemaDefinition {
	private Set<Class> classes = new HashSet<Class>();
	
	@Override
	public void initialize(InfinispanManagedConnectionFactory config, ClassRegistry methodUtil) throws ResourceException {
		
		if (config.getChildClasses() != null) {
			List<String> clzzes = StringUtil.getTokens(config.getChildClasses(), ","); //$NON-NLS-1$
			for (String clzName : clzzes) {
				Class<?> ci = config.loadClass(clzName);

				methodUtil.registerClass(ci);	
				classes.add(ci);
			}
		}

	}


	@Override
	public void registerSchema(InfinispanManagedConnectionFactory config, InfinispanConnectionImpl conn) throws ResourceException {
		final Class<?> clzzType = config.getCacheClassType();
		String p = clzzType.getPackage().getName();
		
		String protoName = clzzType.getName() + ".proto";
		
		ProtoSchemaBuilder protoSchemaBuilder = null;
				
		try {
			Class<?> clzz = config.loadClass("org.infinispan.protostream.annotations.ProtoSchemaBuilder");
			protoSchemaBuilder = (ProtoSchemaBuilder) clzz.newInstance();
			protoSchemaBuilder.fileName(protoName);
			protoSchemaBuilder.packageName(p);
			protoSchemaBuilder.addClass(clzzType);
			
			for(Class<?> c:classes) {
				protoSchemaBuilder.addClass(c);
			}
			String protoSchema = protoSchemaBuilder.build(conn.getContext());
			
		     RemoteCache<String, String> metadataCache = conn.getCache(ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME);
		     metadataCache.put(protoName, protoSchema);
		
		} catch (ProtoSchemaBuilderException e) {
			throw new ResourceException(e);
		} catch (IOException e) {
			throw new ResourceException(e);
		} catch (InstantiationException e) {
			throw new ResourceException(e);
		} catch (IllegalAccessException e) {
			throw new ResourceException(e);
		} catch (TranslatorException e) {
			// TODO Auto-generated catch block
			throw new ResourceException(e);
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
