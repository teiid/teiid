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
package org.teiid.resource.spi;

import java.util.Set;

import javax.resource.spi.security.PasswordCredential;
import javax.security.auth.Subject;

/**
 * Thread local class to access the Subject in the Connector code. This is set just before the
 * connector connection is created.
 */
public class ConnectionContext {
	private static ThreadLocal<Subject> SUBJECT = new ThreadLocal<Subject>() {
		@Override
		protected Subject initialValue() {
			return null;
		}
	};

	public static Subject getSubject() {
		return SUBJECT.get();
	}

	public static void setSubject(Subject subject) {
		SUBJECT.set(subject);
	}

	public static String getUserName(Subject subject, BasicManagedConnectionFactory mcf, String defalt) {
		Set<PasswordCredential> creds = subject.getPrivateCredentials(PasswordCredential.class);
		if ((creds != null) && (creds.size() > 0)) {
			for (PasswordCredential cred : creds) {
				if (cred.getManagedConnectionFactory().equals(mcf)) {
					if (cred.getUserName() != null) {
						return cred.getUserName();
					}
				}
			}
		}
		return defalt;
	}

	public static String getPassword(Subject subject, BasicManagedConnectionFactory mcf, String userName, String defalt) {
		Set<PasswordCredential> creds = subject.getPrivateCredentials(PasswordCredential.class);
		if ((creds != null) && (creds.size() > 0)) {
			for (PasswordCredential cred : creds) {
				if (cred.getManagedConnectionFactory().equals(mcf)) {
					if (cred.getUserName().equals(userName)) {
						if (cred.getPassword() != null) {
							return new String(cred.getPassword());
						}
					}
				}
			}
		}
		return defalt;
	}

	// can not associate with MCF, as AS framework only identifies the PasswordCredential as known credential
	// and assigns the MCF. So, we just take the first credential.
	public static <T> T getSecurityCredential(Subject subject, Class<T> clazz) {
		Set<T> creds = subject.getPrivateCredentials(clazz);
		if ((creds != null) && (creds.size() > 0)) {
			return creds.iterator().next();
		}
		return null;
	}
}
