/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2006, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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


import java.io.IOException;
import java.security.Principal;
import java.security.acl.Group;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.PBEParameterSpec;
import javax.resource.spi.security.PasswordCredential;
import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.login.LoginException;

import org.jboss.logging.Logger;
import org.jboss.security.Base64Utils;
import org.jboss.security.SimplePrincipal;
import org.picketbox.datasource.security.AbstractPasswordCredentialLoginModule;

/**
 * A credential mapping login module that associates currently logged in
 * principle's role name to password mapping from a simple properties file. It
 * is similar to name=password, only here this uses role=password. If user has
 * more than single role that has passwords, then first role with non null
 * password is chosen. This login module must be defined with Managed Connection
 * Factory.
 * 
 * Please note, you can not use this as the primary login module, this should be strictly used
 * to get a role based password, that can be used as credential mapping at data source level. If you
 * are working with a object as trusted token, then do not use the encryption, and provide base64 encoded
 * string of the object as the password and decrypt it in your custom connection factory.  
 * 
 *<pre>{@code
 * <application-policy name = "teiid-security">
 *       <authentication>
 *          <login-module code="org.jboss.security.auth.spi.UsersRolesLoginModule" flag="required">
 *                 <module-option name="usersProperties">props/teiid-security-users.properties</module-option>
 *                 <module-option name="rolesProperties">props/teiid-security-roles.properties</module-option>
 *                 <module-option name="password-stacking">useFirstPass</module-option>
 *          </login-module>      
 *          <login-module code = "org.teiid.jboss.MapIdentityLoginModule" flag = "required">
 *             <module-option name="password-stacking">useFirstPass</module-option>
 *             <module-option name = "credentialMap">config/props/rolepasswords.properties</module-option>
 *             <module-option name = "encryptedPasswords">true</module-option>
 *             
 *             <!-- below properties are only required when passwords are encrypted -->
 *             <module-option name = "pbealgo">PBEWithMD5AndDES</module-option>
 *             <module-option name = "pbepass">testPBEIdentityLoginModule</module-option>
 *             <module-option name = "salt">abcdefgh</module-option>
 *             <module-option name = "iterationCount">19</module-option>
 *             <module-option name = "managedConnectionFactoryName">jboss.jca:service=LocalTxCM,name=DefaultDS</module-option>
 *          </login-module>
 *       </authentication>
 * </application-policy>
 * }</pre>
 * 
 * @see org.jboss.security.SimpleGroup
 * @see org.jboss.security.SimplePrincipal
 * 
 */
public class RoleBasedCredentialMapIdentityLoginModule extends AbstractPasswordCredentialLoginModule {
   private Properties credentialMap;
   private String mappedRole = "mappedRole"; //$NON-NLS-1$
   private static final Logger log = Logger.getLogger(RoleBasedCredentialMapIdentityLoginModule.class);
   
   /** The Blowfish key material */
   private char[] pbepass = "jaas is the way".toCharArray(); //$NON-NLS-1$
   private String pbealgo = "PBEwithMD5andDES"; //$NON-NLS-1$
   private byte[] salt = {1, 7, 2, 9, 3, 11, 4, 13};
   private int iterationCount = 37;
   private boolean encryptionInUse = false;

   public RoleBasedCredentialMapIdentityLoginModule(){
   }

	public void initialize(Subject subject, CallbackHandler handler,	Map sharedState, Map options) {
		super.initialize(subject, handler, sharedState, options);

		String file = (String) options.get("credentialMap"); //$NON-NLS-1$
		if (file == null) {
			throw new IllegalArgumentException("Must supply credentialMap file name!"); //$NON-NLS-1$
		}

		try {
			credentialMap = Util.loadProperties(file, log);
		} catch (IOException e) {
			log.error("failed to load credentail map"); //$NON-NLS-1$
		}
		
		String tmp = (String) options.get("encryptedPasswords"); //$NON-NLS-1$
		if (tmp != null && tmp.equalsIgnoreCase("true")) { //$NON-NLS-1$
			this.encryptionInUse = true;
		
			// Look for the cipher password and algo parameters
			tmp = (String) options.get("pbepass"); //$NON-NLS-1$
			if (tmp != null) {
				try {
					this.pbepass = org.jboss.security.Util.loadPassword(tmp);
				} catch (Exception e) {
					throw new IllegalStateException(e);
				}
			}
			tmp = (String) options.get("pbealgo"); //$NON-NLS-1$
			if (tmp != null) {
				this.pbealgo = tmp;
			}
			tmp = (String) options.get("salt"); //$NON-NLS-1$
			if (tmp != null) {
				this.salt = tmp.substring(0, 8).getBytes();
			}
			tmp = (String) options.get("iterationCount"); //$NON-NLS-1$
			if (tmp != null) {
				this.iterationCount = Integer.parseInt(tmp);
			}
		}
	}

	public boolean login() throws LoginException {

		if (credentialMap == null) {
			throw new LoginException(	"Credential Map properties file failed to load"); //$NON-NLS-1$
		}
				
		return super.login();
	}
	
	public boolean commit() throws LoginException {

		String userRole =  null;
		String rolePassword = null;
		
		Set<String> roles = getRoles();
		for (String role:roles) {
			String password = this.credentialMap.getProperty(role);
			if (password != null) {
				userRole = role;
				rolePassword = password;
			}
		}
		
		try {
			if (userRole != null && rolePassword != null) {
				this.mappedRole = userRole;
				PasswordCredential cred = new PasswordCredential(userRole, decode(rolePassword));
				SecurityActions.addCredentials(this.subject, cred);		
			}
			return super.commit();
		} catch (Exception e) {
			throw new LoginException("Failed to decode password: "+e.getMessage()); //$NON-NLS-1$
		}
	}	

	protected Principal getIdentity() {
		Principal principal = new SimplePrincipal(this.mappedRole);
		return principal;				
	}

   /** 
    * This method simply returns an empty array of Groups which means that
    * no role based permissions are assigned.
    */
	protected Group[] getRoleSets() throws LoginException {
		return new Group[] {};
	}
	
	private Set<String> getRoles() {
		Set<String> roles = new HashSet<String>();
		
		Set<Principal> principals = this.subject.getPrincipals();
		for(Principal p: principals) {
			if ((p instanceof Group) && p.getName().equals("Roles")){ //$NON-NLS-1$
				Group g = (Group)p;
				Enumeration<? extends Principal> rolesPrinciples = g.members();
				while(rolesPrinciples.hasMoreElements()) {
					roles.add(rolesPrinciples.nextElement().getName());	
				}
			}
		}
		return roles;
	}
	
	private char[] decode(String secret) throws Exception {
		if (!this.encryptionInUse) {
			return secret.toCharArray();
		}
		// Create the PBE secret key
		PBEParameterSpec cipherSpec = new PBEParameterSpec(this.salt, this.iterationCount);
		PBEKeySpec keySpec = new PBEKeySpec(this.pbepass);
		SecretKeyFactory factory = SecretKeyFactory.getInstance(this.pbealgo);
		SecretKey cipherKey = factory.generateSecret(keySpec);
		// Decode the secret
		byte[] encoding = Base64Utils.fromb64(secret);
		Cipher cipher = Cipher.getInstance(this.pbealgo);
		cipher.init(Cipher.DECRYPT_MODE, cipherKey, cipherSpec);
		byte[] decode = cipher.doFinal(encoding);
		return new String(decode).toCharArray();
	}
}
