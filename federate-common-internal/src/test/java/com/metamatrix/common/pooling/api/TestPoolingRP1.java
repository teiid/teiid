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

package com.metamatrix.common.pooling.api;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Properties;

import junit.framework.TestCase;

import com.metamatrix.common.config.api.ComponentTypeID;
import com.metamatrix.common.config.api.ConfigurationObjectEditor;
import com.metamatrix.common.config.api.ResourceComponentType;
import com.metamatrix.common.config.api.ResourceDescriptor;
import com.metamatrix.common.config.api.ResourceDescriptorID;
import com.metamatrix.common.config.model.BasicConfigurationObjectEditor;
import com.metamatrix.common.pooling.impl.ResourcePoolMgrImpl;
import com.metamatrix.common.pooling.resource.GenericResource;
import com.metamatrix.common.pooling.resource.GenericResourceAdapter;
import com.metamatrix.common.pooling.resource.GenericResourcePoolMgr;

/**
 * Tests that operate where there should only be 1 (ONE) resource pool
 */
public class TestPoolingRP1 extends TestCase {

	BasicConfigurationObjectEditor editor;

	ResourceComponentType compType;
	ComponentTypeID compTypeID;
	ComponentTypeID superID;
	ComponentTypeID parentID = null;

	private static final String USER = "TestPoolingRP1"; //$NON-NLS-1$

	public TestPoolingRP1(String name) throws Exception {
		super(name);
		editor = new BasicConfigurationObjectEditor();

		superID = new ComponentTypeID("ResourceType"); //$NON-NLS-1$
		compTypeID = new ComponentTypeID("GenericResourceType"); //$NON-NLS-1$
		compType = ConfigUtil.createComponentType(
				"GenericResourceType", superID, parentID); //$NON-NLS-1$
	}
	
	@Override
	protected void setUp() throws Exception {
		GenericResourcePoolMgr mgr = new GenericResourcePoolMgr();
		mgr.shutDown();
	}

	/**
	 * Basic test, does it work.
	 */
	public void testScenario1() throws Exception {

		String min = "1"; //$NON-NLS-1$
		String max = "1"; //$NON-NLS-1$
		String users = "1"; //$NON-NLS-1$
		String scenario = "Scenario1"; //$NON-NLS-1$

		ResourceDescriptor descriptor = ConfigUtil.createDescriptor(scenario
				+ " Pool ", compTypeID); //$NON-NLS-1$

		GenericResourcePoolMgr mgr = new GenericResourcePoolMgr();

		Properties props = new Properties();

		props
				.setProperty(
						ResourcePoolPropertyNames.RESOURCE_ADAPTER_CLASS_NAME,
						"com.metamatrix.common.pooling.resource.GenericResourceAdapter"); //$NON-NLS-1$
		props.setProperty(ResourcePoolPropertyNames.RESOURCE_POOL_CLASS_NAME,
				"com.metamatrix.common.pooling.impl.BasicResourcePool"); //$NON-NLS-1$

		props.setProperty(GenericResourceAdapter.RESOURCE_CLASS_NAME,
				"com.metamatrix.common.pooling.NoOpResource"); //$NON-NLS-1$
		props.setProperty(ResourcePoolPropertyNames.MINIMUM_RESOURCE_POOL_SIZE,
				min);
		props.setProperty(ResourcePoolPropertyNames.MAXIMUM_RESOURCE_POOL_SIZE,
				max);
		props.setProperty(ResourcePoolPropertyNames.NUM_OF_CONCURRENT_USERS,
				users);

		editor.modifyProperties(descriptor, props,
				ConfigurationObjectEditor.ADD);

		GenericResource resource = (GenericResource) mgr.getResource(
				descriptor, USER);

		assertNotNull(resource);

		validateState(scenario, mgr, min, max, users, 1);

		validateResourcePoolMgr(scenario, mgr);

		resource.close();

		mgr.shutDown();

		validateShutDown(scenario, mgr);
	}

	private static final String S2_NO_RESOURCE = "Scenario \"{0}\" error - did not obtain a resource from the pool."; //$NON-NLS-1$
	private static final String S2_NO_RESOURCE2 = "Scenario \"{0}\" error - did not obtain a second resource from the pool."; //$NON-NLS-1$
	private static final String S2_NOT_SAME_RESOURCE = "Scenario \"{0}\" error - did not obtain the same resource object from the pool."; //$NON-NLS-1$
	private static final String S2_SAME_RESOURCE_REF = "Scenario \"{0}\" error - obtained the same resource object reference when concurrency is 1."; //$NON-NLS-1$

	/**
	 * Validating that from the SAME Pool Mgr instance that the expected number
	 * of resources in the pool were created
	 */

	public void testScenario2() throws Exception {
		int num_of_users = 1;
		int max_size = 10;

		String min = "1"; //$NON-NLS-1$
		String max = String.valueOf(max_size);
		String users = String.valueOf(num_of_users);
		String scenario = "Scenario2"; //$NON-NLS-1$
		Collection rs = new ArrayList(max_size);

		ResourceDescriptor descriptor = ConfigUtil.createDescriptor(scenario
				+ " Pool ", compTypeID); //$NON-NLS-1$

		GenericResourcePoolMgr mgr = new GenericResourcePoolMgr();

		Properties props = new Properties();
		props
				.setProperty(
						ResourcePoolPropertyNames.RESOURCE_ADAPTER_CLASS_NAME,
						"com.metamatrix.common.pooling.resource.GenericResourceAdapter"); //$NON-NLS-1$
		props.setProperty(ResourcePoolPropertyNames.RESOURCE_POOL_CLASS_NAME,
				"com.metamatrix.common.pooling.impl.BasicResourcePool"); //$NON-NLS-1$

		props.setProperty(GenericResourceAdapter.RESOURCE_CLASS_NAME,
				"com.metamatrix.common.pooling.NoOpResource"); //$NON-NLS-1$

		// wait 10 seconds before expiring
		props.setProperty(ResourcePoolPropertyNames.MINIMUM_RESOURCE_POOL_SIZE,
				min);
		props.setProperty(ResourcePoolPropertyNames.MAXIMUM_RESOURCE_POOL_SIZE,
				max);
		props.setProperty(ResourcePoolPropertyNames.NUM_OF_CONCURRENT_USERS,
				users);

		editor.modifyProperties(descriptor, props,
				ConfigurationObjectEditor.ADD);

		GenericResource resource1 = (GenericResource) mgr.getResource(
				descriptor, USER);

		if (resource1 == null) {
			sendException(S2_NO_RESOURCE, scenario);
		}

		for (int i = 1; i < max_size; i++) {

			GenericResource nextResource = (GenericResource) mgr.getResource(
					descriptor, USER);
			rs.add(nextResource);

			if (nextResource == null) {
				sendException(S2_NO_RESOURCE2, scenario);
			}

			if (resource1.getObject() == nextResource.getObject()) {
				sendException(S2_SAME_RESOURCE_REF, scenario);
			}

		}

		validateState(scenario, mgr, min, max, users, max_size);

		validateResourcePoolMgr(scenario, mgr);

		resource1.close();

		for (Iterator it = rs.iterator(); it.hasNext();) {
			GenericResource gr = (GenericResource) it.next();
			gr.close();
		}

		mgr.shutDown();

		validateShutDown(scenario, mgr);

	}

	private static final String S3_NO_RESOURCE = "Scenario \"{0}\" error - did not obtain a resource from the pool."; //$NON-NLS-1$
	private static final String S3_NO_RESOURCE2 = "Scenario \"{0}\" error - did not obtain a second resource from the pool."; //$NON-NLS-1$
	private static final String S3_SAME_RESOURCE_REF = "Scenario \"{0}\" error - obtained the same resource object reference when concurrency is 1."; //$NON-NLS-1$

	/**
	 * Validating that when resource are obtained from different Pool Mgr
	 * instances, the same pool will manage all the resources.
	 */

	public void testScenario3() throws Exception {
		int max_size = 20;

		String min = "1"; //$NON-NLS-1$
		String max = String.valueOf(max_size);
		String users = "1"; //$NON-NLS-1$
		String scenario = "Scenario3"; //$NON-NLS-1$
		Collection rs = new ArrayList(max_size);

		ResourceDescriptor descriptor = ConfigUtil.createDescriptor(
				scenario + " Pool ", compTypeID); //$NON-NLS-1$

		GenericResourcePoolMgr mgr = new GenericResourcePoolMgr();

		Properties props = new Properties();
		props
				.setProperty(
						ResourcePoolPropertyNames.RESOURCE_ADAPTER_CLASS_NAME,
						"com.metamatrix.common.pooling.resource.GenericResourceAdapter"); //$NON-NLS-1$
		props.setProperty(
				ResourcePoolPropertyNames.RESOURCE_POOL_CLASS_NAME,
				"com.metamatrix.common.pooling.impl.BasicResourcePool"); //$NON-NLS-1$

		props.setProperty(GenericResourceAdapter.RESOURCE_CLASS_NAME,
				"com.metamatrix.common.pooling.NoOpResource"); //$NON-NLS-1$

		// wait 10 seconds before expiring
		props.setProperty(
				ResourcePoolPropertyNames.MINIMUM_RESOURCE_POOL_SIZE, min);
		props.setProperty(
				ResourcePoolPropertyNames.MAXIMUM_RESOURCE_POOL_SIZE, max);
		props.setProperty(
				ResourcePoolPropertyNames.NUM_OF_CONCURRENT_USERS, users);

		editor.modifyProperties(descriptor, props,
				ConfigurationObjectEditor.ADD);

		GenericResource resource1 = (GenericResource) mgr.getResource(
				descriptor, USER);

		if (resource1 == null) {
			sendException(S3_NO_RESOURCE, scenario);
		}

		GenericResourcePoolMgr mgr2 = new GenericResourcePoolMgr();

		for (int i = 1; i < max_size; i++) {

			GenericResource nextResource = (GenericResource) mgr2
					.getResource(descriptor, USER);
			rs.add(nextResource);

			if (nextResource == null) {
				sendException(S3_NO_RESOURCE2, scenario);
			}

			if (resource1.getObject() == nextResource.getObject()) {
				sendException(S3_SAME_RESOURCE_REF, scenario);
			}

		}

		validateState(scenario, mgr, min, max, users, max_size);

		validateResourcePoolMgr(scenario, mgr);

		resource1.close();

		for (Iterator it = rs.iterator(); it.hasNext();) {
			GenericResource gr = (GenericResource) it.next();
			gr.close();
		}

		mgr.shutDown();

		validateShutDown(scenario, mgr);
	}

	private static final String S4_NO_RESOURCE = "Scenario \"{0}\" error - did not obtain a resource from the pool."; //$NON-NLS-1$
	private static final String S4_NO_RESOURCE2 = "Scenario \"{0}\" error - did not obtain a second resource from the pool."; //$NON-NLS-1$

	protected Collection performScenario4Validation(String scenario,
			ResourcePoolMgrImpl mgr, ResourceDescriptor descriptor,
			int num_of_users) throws Exception {
		Collection rs = new ArrayList(num_of_users);

		GenericResource resource1 = (GenericResource) mgr.getResource(
				descriptor, USER);

		if (resource1 == null) {
			sendException(S4_NO_RESOURCE, scenario);
		}

		rs.add(resource1);

		for (int k = 1; k < num_of_users; k++) {

			GenericResource nextResource = (GenericResource) mgr.getResource(
					descriptor, USER);
			rs.add(nextResource);

			if (nextResource == null) {
				sendException(S4_NO_RESOURCE2, scenario);
			}

			if (resource1.getObject() != nextResource.getObject()) {
				sendException(S2_NOT_SAME_RESOURCE, scenario);
			}

		}

		return rs;
	}

	private static final String E_STATE = "Scenario \"{0}\" error - \"{1}\" parameter of \"{2}\" does not match operating value \"{3}\" "; //$NON-NLS-1$
	private static final String E_POOL_SIZE = "Scenario \"{0}\" error - \"{1}\" resource pools exist, should only be one pool."; //$NON-NLS-1$
	private static final String E_RESOURCE_SIZE = "Scenario \"{0}\" error - \"{1}\" resource exists in the resource pool, expected \"{2}\" "; //$NON-NLS-1$

	protected void validateState(String scenario, ResourcePoolMgrImpl mgr,
			String min, String max, String users, int numOfResources)
			throws Exception {

		Collection pools = mgr.getResourcePools();
		if (pools.size() != 1) {
			sendException(E_POOL_SIZE, scenario, String.valueOf(pools.size()));
		}

		ResourcePool pool = (ResourcePool) pools.iterator().next();

		Properties poolProps = pool.getResourceDescriptor().getProperties();

		String value;

		value = poolProps
				.getProperty(ResourcePoolPropertyNames.MINIMUM_RESOURCE_POOL_SIZE);
		if (!value.equals(min)) {
			sendException(E_STATE, scenario,
					ResourcePoolPropertyNames.MINIMUM_RESOURCE_POOL_SIZE, min,
					value);
		}

		value = poolProps
				.getProperty(ResourcePoolPropertyNames.MAXIMUM_RESOURCE_POOL_SIZE);
		if (!value.equals(max)) {
			sendException(E_STATE, scenario,
					ResourcePoolPropertyNames.MAXIMUM_RESOURCE_POOL_SIZE, max,
					value);
		}

		value = poolProps
				.getProperty(ResourcePoolPropertyNames.NUM_OF_CONCURRENT_USERS);
		if (!value.equals(users)) {
			sendException(E_STATE, scenario,
					ResourcePoolPropertyNames.NUM_OF_CONCURRENT_USERS, users,
					value);
		}

		// make sure the number of containers in the pool match the number the
		// test expect
		if (pool.getResourcePoolSize() != numOfResources) {
			sendException(E_RESOURCE_SIZE, scenario, String.valueOf(pool
					.getResourcePoolSize()), String.valueOf(numOfResources));
		}

	}

	private static final String E_NODESC = "Scenario \"{0}\" error - resource descriptor id \"{1}\" did not find a descriptor in the manager "; //$NON-NLS-1$
	private static final String E_NOSTAT = "Scenario \"{0}\" error - resource descriptor id \"{1}\" did not find a resource pool statistics in the manager "; //$NON-NLS-1$

	protected void validateResourcePoolMgr(String scenario, ResourcePoolMgr mgr)
			throws Exception {

		Collection cIDS = mgr.getAllResourceDescriptorIDs();
		for (Iterator itIDs = cIDS.iterator(); itIDs.hasNext();) {
			ResourceDescriptorID id = (ResourceDescriptorID) itIDs.next();

			ResourceDescriptor rd = mgr.getResourceDescriptor(id);
			if (rd == null) {
				sendException(E_NODESC, scenario, null);
			}

			ResourcePoolStatistics stats = mgr.getResourcePoolStatistics(id);
			if (stats == null) {
				sendException(E_NOSTAT, scenario, rd.getID().getName());
			}
		}

		Collection cDs = mgr.getAllResourceDescriptors();
		for (Iterator itCDS = cDs.iterator(); itCDS.hasNext();) {
			// ResourceDescriptor desc = (ResourceDescriptor)
			itCDS.next();
		}

		Collection cStats = mgr.getResourcePoolStatistics();
		for (Iterator itStats = cStats.iterator(); itStats.hasNext();) {
			// ResourcePoolStatistics stats = (ResourcePoolStatistics)
			itStats.next();
		}

	}

	private static final String E_SHUTDOWN = "Scenario \"{0}\" error - resource pool(s) still exist after shutdown being called."; //$NON-NLS-1$

	protected void validateShutDown(String scenario, ResourcePoolMgrImpl mgr)
			throws Exception {

		Collection pools = mgr.getResourcePools();
		if (pools.size() != 0) {
			sendException(E_SHUTDOWN, scenario);
		}

	}

	private void sendException(String txt, String argument) throws Exception {
		Object[] args = new Object[1];
		args[0] = argument;

		sendException(txt, args);
	}

	private void sendException(String txt, String arg1, String arg2)
			throws Exception {
		Object[] args = new Object[2];

		args[0] = arg1;
		args[1] = arg2;

		sendException(txt, args);
	}

	private void sendException(String txt, String arg1, String arg2, String arg3)
			throws Exception {
		Object[] args = new Object[3];

		args[0] = arg1;
		args[1] = arg2;
		args[2] = arg3;

		sendException(txt, args);
	}

	private void sendException(String txt, String arg1, String arg2,
			String arg3, String arg4) throws Exception {
		Object[] args = new Object[4];

		args[0] = arg1;
		args[1] = arg2;
		args[2] = arg3;
		args[3] = arg4;

		sendException(txt, args);
	}

	private void sendException(String txt, Object[] args) throws Exception {
		String msg = MessageFormat.format(txt, args);

		throw new Exception(msg);

	}

}
