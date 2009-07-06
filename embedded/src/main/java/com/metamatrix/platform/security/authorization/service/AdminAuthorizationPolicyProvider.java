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
package com.metamatrix.platform.security.authorization.service;

import java.io.IOException;
import java.net.URL;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.protocol.URLHelper;
import com.metamatrix.common.util.LogConstants;
import com.metamatrix.common.util.PropertiesUtils;
import com.metamatrix.dqp.embedded.DQPEmbeddedPlugin;
import com.metamatrix.dqp.service.AuthorizationService;
import com.metamatrix.platform.security.api.AuthorizationPolicy;
import com.metamatrix.platform.security.api.AuthorizationPolicyFactory;

@Singleton
public class AdminAuthorizationPolicyProvider implements Provider<Collection<AuthorizationPolicy>> {

	@Inject @Named("DQPProperties")
	Properties props;
	
	@Inject @Named("BootstrapURL")
	private URL dqpURL;
	
	@Override
	public Collection<AuthorizationPolicy> get() {
		String fileName = this.props.getProperty(AuthorizationService.ADMIN_ROLES_FILE);

		if (fileName != null) {
			try {
	        	URL url = URLHelper.buildURL(this.dqpURL, fileName);
	            Properties roles = PropertiesUtils.loadFromURL(url);
	            return AuthorizationPolicyFactory.buildAdminPolicies(roles);
			}catch(IOException e) {
				LogManager.logError(LogConstants.CTX_AUTHORIZATION, e, DQPEmbeddedPlugin.Util.getString("failed_to_load_admin_roles")); //$NON-NLS-1$
			}
		}
		else {
			LogManager.logDetail(LogConstants.CTX_AUTHORIZATION, DQPEmbeddedPlugin.Util.getString("admin_roles_not_defined")); //$NON-NLS-1$
		}
		return Collections.EMPTY_LIST;
	}

}
