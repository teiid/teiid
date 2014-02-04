/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005, JBoss Inc., and individual contributors as indicated
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.teiid.jboss;

import java.security.acl.Group;

import javax.security.auth.login.LoginException;

import org.jboss.security.SimpleGroup;
import org.jboss.security.auth.spi.UsernamePasswordLoginModule;

/**
 * A simple server login module to creates subject with passed in name and null
 * password
 */
public class SimpleLoginModule extends UsernamePasswordLoginModule {

	@Override
	protected boolean validatePassword(String inputPassword, String expectedPassword) {
		return true;
	}

	@Override
	protected String getUsersPassword() throws LoginException {
		return null;
	}

	@Override
	protected Group[] getRoleSets() throws LoginException {
		SimpleGroup roles = new SimpleGroup("Roles"); //$NON-NLS-1$
		Group[] roleSets = { roles };
		return roleSets;
	}
}