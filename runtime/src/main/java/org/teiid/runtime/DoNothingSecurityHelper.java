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

package org.teiid.runtime;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginException;

import org.teiid.security.Credentials;
import org.teiid.security.GSSResult;
import org.teiid.security.SecurityHelper;

/**
 * A {@link SecurityHelper} that does nothing and always assumes that thread
 * has the proper security context.
 */
public class DoNothingSecurityHelper implements SecurityHelper {
	
	@Override
	public Subject getSubjectInContext(String securityDomain) {
		return new Subject();
	}

	@Override
	public Object getSecurityContext() {
		return new Object();
	}

	@Override
	public void clearSecurityContext() {

	}

	@Override
	public Object associateSecurityContext(Object context) {
		return null;
	}
	
	@Override
	public Object authenticate(String securityDomain, String baseUserName,
			Credentials credentials, String applicationName)
			throws LoginException {
		return new Object();
	}

    @Override
    public GSSResult negotiateGssLogin(String securityDomain, byte[] serviceTicket) throws LoginException {
        return null;
    }

	@Override
	public Subject getSubjectInContext(Object context) {
		return new Subject();
	}
    
}