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

import javax.resource.ResourceException;

import org.infinispan.protostream.descriptors.Descriptor;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.object.ClassRegistry;

/**
 * The InfinispanSchemaDefintion interface is used to support the various options (i.e, protobuf files or annotations) for configuring
 * the JDG schema.  Additionally, responsible for providing information based on how the schema was configured.
 * 
 * @author vhalbert
 *
 */
public interface InfinispanSchemaDefinition {
	
	/**
	 * Called to perform any initialization required by registering the classes. 
	 * @param config
	 * @param methodUtil
	 * @throws ResourceException
	 */
	public void initialize(InfinispanManagedConnectionFactory config, ClassRegistry methodUtil) throws ResourceException ;
	
	/**
	 * Called to perform the JDG schema configuration by registering with the remote JDG cache. 
	 * @param config
	 * @throws ResourceException
	 */
	public void registerSchema(InfinispanManagedConnectionFactory config, InfinispanConnectionImpl conn)  throws ResourceException;

	/**
	 * Called to obtain a <code>Descriptor</code> for the specified class.
	 * @param config
	 * @param clz
	 * @return Descriptor
	 * @throws TranslatorException if no descriptor is found.
	 */
	public Descriptor getDecriptor(InfinispanManagedConnectionFactory config, InfinispanConnectionImpl conn, Class<?> clz) throws TranslatorException;
}
