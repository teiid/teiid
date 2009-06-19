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

package com.metamatrix.platform.security.membership.service;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;

import org.teiid.dqp.internal.process.DQPWorkContext;

import com.metamatrix.admin.api.exception.security.MetaMatrixSecurityException;
import com.metamatrix.api.exception.security.InvalidPrincipalException;
import com.metamatrix.api.exception.security.InvalidUserException;
import com.metamatrix.api.exception.security.LogonException;
import com.metamatrix.api.exception.security.MembershipServiceException;
import com.metamatrix.api.exception.security.UnsupportedCredentialException;
import com.metamatrix.common.config.CurrentConfiguration;
import com.metamatrix.common.config.api.AuthenticationProvider;
import com.metamatrix.common.config.api.AuthenticationProviderType;
import com.metamatrix.common.config.model.ComponentCryptoUtil;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.util.LogConstants;
import com.metamatrix.common.util.crypto.CryptoException;
import com.metamatrix.common.util.crypto.CryptoUtil;
import com.metamatrix.core.util.StringUtil;
import com.metamatrix.platform.PlatformPlugin;
import com.metamatrix.platform.security.api.BasicMetaMatrixPrincipal;
import com.metamatrix.platform.security.api.Credentials;
import com.metamatrix.platform.security.api.MetaMatrixPrincipal;
import com.metamatrix.platform.security.api.MetaMatrixPrincipalName;
import com.metamatrix.platform.security.api.service.MembershipServiceInterface;
import com.metamatrix.platform.security.membership.spi.MembershipDomain;
import com.metamatrix.platform.security.membership.spi.MembershipSourceException;
import com.metamatrix.platform.service.api.exception.ServiceException;
import com.metamatrix.platform.service.api.exception.ServiceStateException;
import com.metamatrix.platform.service.controller.AbstractService;
import com.metamatrix.platform.util.ErrorMessageKeys;
import com.metamatrix.platform.util.LogMessageKeys;

/**
 * This class serves as the primary implementation of the
 * Membership Service, and logically consists of a set of one or
 * more MembershipDomainInterface instances.  The Service is
 * responsible for creating and intializing the set of domains,
 * receiving requests from clients, and determining
 * which domain or domains are to handle those requests.
 */
public class MembershipServiceImpl extends AbstractService implements MembershipServiceInterface {

	static class MembershipDomainHolder {

        private MembershipDomain membershipDomain;
        private String domainName;
        
        public MembershipDomainHolder(MembershipDomain domain, String domainName) {
            this.membershipDomain = domain;
            this.domainName = domainName;
        }
        
        /**
         * Get the unique name of this Membership domain.
         * @return the domain name.
         */
        public String getDomainName() {
            return domainName;
        }

        /** 
         * @return Returns the membershipDomain.
         */
        public MembershipDomain getMembershipDomain() {
            return this.membershipDomain;
        }

    }
    
    //holds an ordered map of membershipdomainholders
    private List domains = new ArrayList();
    
    private String adminUsername = DEFAULT_ADMIN_USERNAME;
    private String adminCredentials;
    
    private Pattern allowedAddresses;
    
    private boolean isSecurityEnabled = true;
    
    public MembershipServiceImpl() {
        super();
    }

    // -----------------------------------------------------------------------------------
    //                 S E R V I C E - R E L A T E D    M E T H O D S
    // -----------------------------------------------------------------------------------

    /**
     * Perform initialization and commence processing. This method is called only once.
     * <p>Note: In order to perform the chaining of membership domains, this method assumes
     * there exists a property in the given environment properties named
     * {@link com.metamatrix.platform.security.api.service.MembershipServiceInterface#DOMAIN_ORDER DOMAIN_ORDER}
     * that has an ordered value in the form of "A, X, ..., D"
     */
    protected void initService(Properties env) {

        adminUsername = env.getProperty(ADMIN_USERNAME, DEFAULT_ADMIN_USERNAME); 
        
        adminCredentials = env.getProperty(ADMIN_PASSWORD); 
        if (adminCredentials == null || adminCredentials.length() == 0) {
            throw new ServiceException(PlatformPlugin.Util.getString("MembershipServiceImpl.Root_password_required")); //$NON-NLS-1$
        }
        
        String property = env.getProperty(ADMIN_HOSTS);
        if (property != null && property.length() > 0) {
        	this.allowedAddresses = Pattern.compile(property);
        }
        
        isSecurityEnabled = Boolean.valueOf(env.getProperty(SECURITY_ENABLED)).booleanValue();
        LogManager.logDetail(LogConstants.CTX_MEMBERSHIP, "Security Enabled: " + isSecurityEnabled); //$NON-NLS-1$
        
        try {
            //TODO: my caller should have already decrypted this for me
            adminCredentials = CryptoUtil.stringDecrypt(adminCredentials);
        } catch (CryptoException err) {
            LogManager.logCritical(LogConstants.CTX_MEMBERSHIP, err, PlatformPlugin.Util.getString("MembershipServiceImpl.Root_password_decryption_failed")); //$NON-NLS-1$
            throw new ServiceException(err);
        }
        
        String domainNameOrder = env.getProperty(MembershipServiceInterface.DOMAIN_ORDER);
        if (domainNameOrder == null || domainNameOrder.trim().length()==0) {
            return;
        }
        
        List domainNames = StringUtil.split(domainNameOrder, ","); //$NON-NLS-1$
        
        Iterator domainNameItr = domainNames.iterator();
        while ( domainNameItr.hasNext() ) {
            String domainName = ((String) domainNameItr.next()).trim();
            MembershipDomain newDomain = null;

            try {
                AuthenticationProvider provider = CurrentConfiguration.getInstance().getConfiguration().getAuthenticationProvider(domainName);
                // Create the domain...
                if(provider!=null) {
                    Properties props = ComponentCryptoUtil.getDecryptedProperties(provider);
                    
                    if (!Boolean.valueOf(props.getProperty(DOMAIN_ACTIVE)).booleanValue()) {
                        LogManager.logDetail(LogConstants.CTX_MEMBERSHIP, "Skipping initilization of inactive domain " + domainName); //$NON-NLS-1$
                        continue;
                    }
                	newDomain = createDomain(domainName, props);
                }
                LogManager.logInfo(LogConstants.CTX_MEMBERSHIP, PlatformPlugin.Util.getString(LogMessageKeys.SEC_MEMBERSHIP_0005, domainName));
                if(newDomain!=null) {
                    this.domains.add(new MembershipDomainHolder(newDomain, domainName));
                }
            } catch (Throwable e) {
                String msg = PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_MEMBERSHIP_0021, domainName);
                LogManager.logCritical(LogConstants.CTX_MEMBERSHIP, e, msg);
                setInitException(new ServiceException(e, msg));
            }
        }
    }

    /**
     * Close the service to new work if applicable. After this method is called
     * the service should no longer accept new work to perform but should continue
     * to process any outstanding work. This method is called by die().
     */
    protected void closeService() throws Exception {
        String instanceName = this.getInstanceName();
        LogManager.logInfo(LogConstants.CTX_MEMBERSHIP, PlatformPlugin.Util.getString(LogMessageKeys.SEC_MEMBERSHIP_0001, instanceName));
        this.shutdownDomains();
        LogManager.logInfo(LogConstants.CTX_MEMBERSHIP, PlatformPlugin.Util.getString(LogMessageKeys.SEC_MEMBERSHIP_0002, instanceName));
    }

    /**
     * Create the membership domain of the given name with the given properties. A domain is created
     * by instantiating the domain factory found in <code>env</code>.
     * @param domainName The domain to be instantiated.
     * @param env All properties that the domain needs to be established <i>especially>/i> the domain
     * factory class name.
     * @return The newly instantiated domain.
     */
    private MembershipDomain createDomain(String domainName, Properties properties) {

        MembershipDomain domain = null;
        String className = properties.getProperty(AuthenticationProviderType.Attributes.AUTHDOMAIN_CLASS); 
        
        if (className != null && className.length() > 0) {
            try {
                domain = (MembershipDomain) Thread.currentThread().getContextClassLoader().loadClass(className).newInstance();
            } catch (Throwable e) {
                String msg = PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_MEMBERSHIP_0023, className);
                throw new ServiceException(e, ErrorMessageKeys.SEC_MEMBERSHIP_0023, msg);
            }
        } else {
            String msg =  PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_MEMBERSHIP_0024, domainName);
            throw new ServiceException(ErrorMessageKeys.SEC_MEMBERSHIP_0024, msg);
        }
        
        properties.setProperty(DOMAIN_NAME, domainName);
        
        String propsString = properties.getProperty(DOMAIN_PROPERTIES);
        
        if (propsString != null) {
            Properties customProps = loadFile(propsString, domain.getClass());
            properties.putAll(customProps);
        }
        
        domain.initialize(properties);

        return domain;
    }

    private void shutdownDomains() {
        if ( ! this.isClosed() ) {
            // Shut down the domain(s) ...
            Iterator iter = this.domains.iterator();
            while ( iter.hasNext() ) {
                MembershipDomainHolder domainHolder = (MembershipDomainHolder) iter.next();
                try {
                    domainHolder.getMembershipDomain().shutdown();
                } catch ( Exception e ) {
                    LogManager.logError(LogConstants.CTX_MEMBERSHIP, e, ErrorMessageKeys.SEC_MEMBERSHIP_0026);
                }
            }
            this.domains.clear();
        }
    }

    /**
     * Wait until the service has completed all outstanding work. This method
     * is called by die() just before calling dieNow().
     */
    protected void waitForServiceToClear() throws Exception {
        this.shutdownDomains();
    }

    /**
     * Terminate all processing and reclaim resources. This method is called by dieNow()
     * and is only called once.
     */
    protected void killService() {
        this.shutdownDomains();
    }
    
    void setAllowedAddresses(Pattern allowedAddresses) {
		this.allowedAddresses = allowedAddresses;
	}
    
    void setAdminCredentials(String adminCredentials) {
		this.adminCredentials = adminCredentials;
	}

    /**
     * Authenticate a user with the specified username and credential
     * for use with the specified application. The application name may also
     * be used by the Membership Service to determine the appropriate authentication
     * mechanism.
     * <p>
     * This method iterates through the domains and, on each domain, attempts to authenticate
     * the principal with the specified credentials.  Authentication is successful
     * upon the first domain encountered that authenticates the principals.
     * @param username
     * @param credential
     * @param trustedPayload
     * @param applicationName
     * @return
     * @throws MetaMatrixSecurityException
     * @throws MembershipServiceException 
     */
    public AuthenticationToken authenticateUser(String username, Credentials credential, Serializable trustedPayload, String applicationName) {
        
        LogManager.logTrace(LogConstants.CTX_MEMBERSHIP, new Object[] {"authenticateUser", username, applicationName}); //$NON-NLS-1$
        
        if (credential != null) {
            String password = new String(credential.getCredentialsAsCharArray());
            if (CryptoUtil.isEncryptionEnabled() && CryptoUtil.isValueEncrypted(password)) {
                try {
                    credential = new Credentials(CryptoUtil.stringDecrypt(password).toCharArray());
                } catch (CryptoException err) {
                    //just log and allow the normal authentication flow
                    String msg = PlatformPlugin.Util.getString("MembershipServiceImpl.Decrypt_failed", username); //$NON-NLS-1$
                    LogManager.logWarning(LogConstants.CTX_MEMBERSHIP, err, msg);
                }
            }
        }
        
        if (!isSecurityEnabled) {
        	return new SuccessfulAuthenticationToken(trustedPayload, username);
        }
        
        if (isSuperUser(username)) {
        	if (isSecurityEnabled && allowedAddresses != null) {
	        	String address = DQPWorkContext.getWorkContext().getClientAddress();
	        	if (address == null) {
	        		LogManager.logWarning(LogConstants.CTX_MEMBERSHIP, PlatformPlugin.Util.getString("MembershipServiceImpl.unknown_host")); //$NON-NLS-1$
	        		return new FailedAuthenticationToken();
	        	}
	        	if (!allowedAddresses.matcher(address).matches() || address.equals(CurrentConfiguration.getInstance().getHostAddress().getHostAddress())) {
	        		LogManager.logWarning(LogConstants.CTX_MEMBERSHIP, PlatformPlugin.Util.getString("MembershipServiceImpl.invalid_host", address, allowedAddresses.pattern())); //$NON-NLS-1$
	        		return new FailedAuthenticationToken();
	        	}
        	}
        	// decrypt admin password for comparison
            if ((credential != null && adminCredentials.equals(String.valueOf(credential.getCredentialsAsCharArray())))) {
                return new SuccessfulAuthenticationToken(trustedPayload, username);
            }
            return new FailedAuthenticationToken();
        }
        
        if (isWsdlUser(username)) {
        	// No need to check credentials. There is no password for the wsdl user. 
            return new SuccessfulAuthenticationToken(trustedPayload, username);
        }
        
        String baseUsername = getBaseUsername(username);
           
        // If username specifies a domain (user@domain) only that domain is authenticated against.
        // If username specifies no domain, then all domains are tried in order.
        Iterator iter = getDomainsForUser(username).iterator();
        while ( iter.hasNext() ) {
            MembershipDomainHolder entry = (MembershipDomainHolder)iter.next();
            try {
                
                SuccessfulAuthenticationToken auth = entry.getMembershipDomain().authenticateUser(baseUsername, credential, trustedPayload, applicationName);
                
                if (auth != null) {
                    baseUsername = escapeName(auth.getUserName());
                    String domain = entry.getDomainName();
                    
                    if (auth.getDomainName() != null) {
                        domain = auth.getDomainName();
                    }
                    return new SuccessfulAuthenticationToken(auth.getPayload(), baseUsername + MembershipServiceInterface.AT + domain);
                }
                String msg = PlatformPlugin.Util.getString("MembershipServiceImpl.Null_authentication", entry.getDomainName(), username ); //$NON-NLS-1$
                LogManager.logError(LogConstants.CTX_MEMBERSHIP, msg);
                return new FailedAuthenticationToken();
            } catch (LogonException le) {
                String msg = PlatformPlugin.Util.getString("MembershipServiceImpl.Logon_failed", entry.getDomainName(), username ); //$NON-NLS-1$
                LogManager.logWarning(LogConstants.CTX_MEMBERSHIP, le, msg);
                return new FailedAuthenticationToken();
            } catch (InvalidUserException e) {
                String msg = PlatformPlugin.Util.getString("MembershipServiceImpl.Invalid_user", entry.getDomainName(), username ); //$NON-NLS-1$
                LogManager.logDetail(LogConstants.CTX_MEMBERSHIP, e, msg);
            } catch (UnsupportedCredentialException e) {
                String msg = PlatformPlugin.Util.getString("MembershipServiceImpl.Unsupported_credentials", entry.getDomainName(), username ); //$NON-NLS-1$
                LogManager.logDetail(LogConstants.CTX_MEMBERSHIP, e, msg);
            } catch (MembershipSourceException e) {
                //just skip this domain for now
                String msg = PlatformPlugin.Util.getString("MembershipServiceImpl.source_exception", entry.getDomainName()); //$NON-NLS-1$
                LogManager.logError(LogConstants.CTX_MEMBERSHIP, e, msg); 
            }
        }
        String msg = PlatformPlugin.Util.getString("MembershipServiceImpl.Failed_authentication", username ); //$NON-NLS-1$
        LogManager.logDetail(LogConstants.CTX_MEMBERSHIP, msg);
        return new FailedAuthenticationToken();
    }
    
    static String getBaseUsername(String username) {
        if (username == null) {
            return username;
        }
        
        int index = getQualifierIndex(username);

        String result = username;
        
        if (index != -1) {
            result = username.substring(0, index);
        }
        
        //strip the escape character from the remaining ats
        return result.replaceAll("\\\\"+MembershipServiceInterface.AT, MembershipServiceInterface.AT); //$NON-NLS-1$
    }
    
    static String escapeName(String name) {
        if (name == null) {
            return name;
        }
        
        return name.replaceAll(MembershipServiceInterface.AT, "\\\\"+MembershipServiceInterface.AT); //$NON-NLS-1$
    }
    
    static String getDomainName(String username) {
        if (username == null) {
            return username;
        }
        
        int index = getQualifierIndex(username);
        
        if (index != -1) {
            return username.substring(index + 1);
        }
        
        return null;
    }
    
    static int getQualifierIndex(String username) {
        int index = username.length();
        while ((index = username.lastIndexOf(MembershipServiceInterface.AT, --index)) != -1) {
            if (index > 0 && username.charAt(index - 1) != '\\') {
                return index;
            }
        }
        
        return -1;
    }
    
    private Collection getDomainsForUser(String username) {
    	// If username is null, return all domains
        if (username == null) {
            return domains;
        // If username is admin account, return empty domain list
        } else if (isSuperUser(username) || !isSecurityEnabled) {
        	return Collections.EMPTY_LIST;
        } 
        
        String domain = getDomainName(username);
        
        if (domain == null) {
            return domains;
        }
        
        // ------------------------------------------
        // Handle usernames having @ sign
        // ------------------------------------------
        
        MembershipDomainHolder domainHolder = null;
        Iterator iter = domains.iterator();
        while(iter.hasNext()) {
        	MembershipDomainHolder currentHolder = (MembershipDomainHolder)iter.next();
        	if(domain.equalsIgnoreCase(currentHolder.getDomainName())) {
        		domainHolder = currentHolder;
        		break;
        	}
        }
        
        if (domainHolder == null) {
            return Collections.EMPTY_LIST;
        }
        
        LinkedList result = new LinkedList();
        
        result.add(domainHolder);
        
        return result;
    }
    
    /**
     * Obtain the principal object that is representative of the principal
     * @param principalName
     * @param type
     * @return
     * @throws MetaMatrixSecurityException
     * @throws InvalidPrincipalException 
     */
    public MetaMatrixPrincipal getPrincipal(MetaMatrixPrincipalName principal)
    		throws MembershipServiceException, InvalidPrincipalException {
        LogManager.logTrace( LogConstants.CTX_MEMBERSHIP, new Object[] { "getPrincipal", principal }); //$NON-NLS-1$

    	String name = principal.getName();

        if (principal.getType() != MetaMatrixPrincipal.TYPE_GROUP) {
	        // Handle admin account user
	        if(isSuperUser(name) || !isSecurityEnabled) {
	        	return new BasicMetaMatrixPrincipal(name, MetaMatrixPrincipal.TYPE_ADMIN,Collections.EMPTY_SET);
	        }
	        
	        if (isWsdlUser(name)) {
	        	return new BasicMetaMatrixPrincipal(name, MetaMatrixPrincipal.TYPE_USER,Collections.EMPTY_SET);
	        }
        }
        
        // Get base username (strip off everything after @)
        String baseName = getBaseUsername(name);
        
        // Get domains for the user
        Collection userDomains = getDomainsForUser(name);
        
        // If baseName is null, or domain cannot be uniquely determined throw exception
        if (baseName==null || userDomains.size() != 1) {
            throw new InvalidPrincipalException(ErrorMessageKeys.SEC_MEMBERSHIP_0031,PlatformPlugin.Util.getString(ErrorMessageKeys.SEC_MEMBERSHIP_0031, name));
        }
        
        MembershipDomainHolder domain = (MembershipDomainHolder) userDomains.iterator().next();
        try {
        	if (principal.getType() != MetaMatrixPrincipal.TYPE_GROUP) {
	            Set results = getDomainSpecificGroups(domain.getMembershipDomain().getGroupNamesForUser(baseName), domain.getDomainName());
	            // Get the principal from this domain
	            BasicMetaMatrixPrincipal result = new BasicMetaMatrixPrincipal(name, MetaMatrixPrincipal.TYPE_USER, results);
	            // If there is a result, then return the principal ...
	            LogManager.logTrace(LogConstants.CTX_MEMBERSHIP, new Object[]{"The user \"",name,"\" was obtained from domain \"",domain.getDomainName(),"\""} ); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
	            return result;
        	} 
        	if (domain.getMembershipDomain().getGroupNames().contains(baseName)) {
	            return new BasicMetaMatrixPrincipal(name, MetaMatrixPrincipal.TYPE_GROUP);
	        }
        } catch ( InvalidPrincipalException e ) {
            String msg = PlatformPlugin.Util.getString("MembershipServiceImpl.principal_does_not_exist", name, domain.getDomainName()); //$NON-NLS-1$
            LogManager.logError(LogConstants.CTX_MEMBERSHIP, e, msg); 
            throw new InvalidPrincipalException(msg);
        } catch (Throwable e) {
            String msg = PlatformPlugin.Util.getString("MembershipServiceImpl.source_exception", domain.getDomainName()); //$NON-NLS-1$
            LogManager.logError(LogConstants.CTX_MEMBERSHIP, e, msg); 
            throw new MembershipServiceException(msg);
        } 
    	throw new InvalidPrincipalException(PlatformPlugin.Util.getString("MembershipServiceImpl.principal_does_not_exist", name, domain.getDomainName())); //$NON-NLS-1$
    }
    
    private Set<String> getDomainSpecificGroups(Set<String> groups, String domainName) {
        if (groups == null) {
            return Collections.emptySet();
        }
        Set<String> results = new HashSet<String>();
        
        for (Iterator<String> i = groups.iterator(); i.hasNext();) {
            results.add(escapeName(i.next()) + MembershipServiceInterface.AT + domainName);
        }
        return results;
    }

    /**
     * Obtain the set of groups to which this user belongs to.
     * The result will come from the first domain that has the specified user.
     * 
     * @return a set of Strings
     */
    public Set getGroupsForUser(String userName)
    throws MembershipServiceException, InvalidPrincipalException {
        LogManager.logTrace( LogConstants.CTX_MEMBERSHIP, new Object[] { "getGroupsForUser", userName}); //$NON-NLS-1$
        
        MetaMatrixPrincipal principal = getPrincipal(new MetaMatrixPrincipalName(userName, MetaMatrixPrincipal.TYPE_USER)); 
        
        return principal.getGroupNames();
    }

    /**
     * Display contents of this membership service by outputting all its domains.
     */
    public String toString() {
        StringBuffer membershipDomains = new StringBuffer();
        membershipDomains.append("\n*** MembershipService: " + super.getInstanceName() + " ***\n"); //$NON-NLS-1$ //$NON-NLS-2$
        Iterator domainItr = this.domains.iterator();
        while ( domainItr.hasNext() ) {
            membershipDomains.append((domainItr.next()).toString());
        }
        return membershipDomains.toString();
    }

    /** 
     * @throws MembershipServiceException 
     * @see com.metamatrix.platform.security.api.service.MembershipServiceInterface#getGroupNames()
     */
    public Set getGroupNames() throws MembershipServiceException {
        LogManager.logTrace(LogConstants.CTX_MEMBERSHIP, new Object[] {"getGroupNames"}); //$NON-NLS-1$
        
        Set result = new HashSet();
        Iterator iter = this.domains.iterator();
        while ( iter.hasNext() ) {
            MembershipDomainHolder domain = (MembershipDomainHolder) iter.next();
            try {
                result.addAll( getDomainSpecificGroups(domain.getMembershipDomain().getGroupNames(), domain.getDomainName()) );
            } catch (Throwable e) {
                String msg = PlatformPlugin.Util.getString("MembershipServiceImpl.source_exception", domain.getDomainName()); //$NON-NLS-1$
                LogManager.logError(LogConstants.CTX_MEMBERSHIP, e, msg); 
                throw new MembershipServiceException(msg);
            }
        }
        return result;
    }

    /** 
     * @return Returns the domains.
     */
    protected List getDomains() {
        return this.domains;
    }
    
    public List getDomainNames() {
        
        LogManager.logTrace(LogConstants.CTX_MEMBERSHIP, new Object[] {"getDomainNames"}); //$NON-NLS-1$
        
    	List names = new ArrayList();
    	Iterator iter = this.domains.iterator();
    	while(iter.hasNext()) {
    		MembershipDomainHolder domainHolder = (MembershipDomainHolder)iter.next();
    		String domainName = domainHolder.getDomainName();
    		if(domainName!=null) {
    			names.add( domainName );
    		}
    	}
    	return names;
    }
    
    public Set<String> getGroupsForDomain(String domainName) throws MembershipServiceException {
    	
        LogManager.logTrace(LogConstants.CTX_MEMBERSHIP, new Object[] {"getGroupsForDomain", domainName}); //$NON-NLS-1$
        
        MembershipDomainHolder dHolder = null;
    	Iterator iter = this.domains.iterator();
    	while(iter.hasNext()) {
    		MembershipDomainHolder domainHolder = (MembershipDomainHolder)iter.next();
    		String holderName = domainHolder.getDomainName();
    		if(holderName!=null && holderName.equalsIgnoreCase(domainName)) {
    			dHolder = domainHolder;
    		}
    	}
    	if(dHolder==null) {
            return Collections.emptySet();
        }
		try {
            return getDomainSpecificGroups(dHolder.getMembershipDomain().getGroupNames(), domainName);
        } catch (Throwable e) {
            String msg = PlatformPlugin.Util.getString("MembershipServiceImpl.source_exception", dHolder.getDomainName()); //$NON-NLS-1$
            LogManager.logError(LogConstants.CTX_MEMBERSHIP, e, msg); 
            throw new MembershipServiceException(msg);
        }
    }

    public boolean isSuperUser(String username) {
        return adminUsername.equalsIgnoreCase(username);
    }
    
    public boolean isWsdlUser(String username) {
        return DEFAULT_WSDL_USERNAME.equalsIgnoreCase(username);
    }
    
    /** 
     * @see com.metamatrix.platform.security.api.service.MembershipServiceInterface#isSecurityEnabled()
     */
    public boolean isSecurityEnabled() throws MembershipServiceException{
        return isSecurityEnabled;
    }
    
    /**
     * Loads the given file by searching multiple loading mechanisms
     *  
     * @param fileName
     * @param clazz
     * @return
     * @throws ServiceStateException
     */
    public static Properties loadFile(String fileName, Class clazz) throws ServiceStateException {
        Properties result = new Properties();

        //try the classpath
        InputStream is = clazz.getResourceAsStream(fileName);
        
        if (is == null) {
            try {
                //try the filesystem
                is = new FileInputStream(fileName);
            } catch (FileNotFoundException err) {
                try {
                    //try a url
                    is = new URL(fileName).openStream();
                } catch (MalformedURLException err1) {
                    throw new ServiceStateException(err, PlatformPlugin.Util.getString("MembershipServiceImpl.load_error", fileName)); //$NON-NLS-1$
                } catch (IOException err1) {
                    throw new ServiceStateException(err1, PlatformPlugin.Util.getString("MembershipServiceImpl.load_error", fileName)); //$NON-NLS-1$
                }
            } 
        }
        
        try {
            result.load(is);
        } catch (IOException err) {
            throw new ServiceStateException(err, PlatformPlugin.Util.getString("MembershipServiceImpl.load_error", fileName)); //$NON-NLS-1$
        } finally {
            try {
                is.close();
            } catch (IOException err) {
            }
        }
        return result;
    }

}
