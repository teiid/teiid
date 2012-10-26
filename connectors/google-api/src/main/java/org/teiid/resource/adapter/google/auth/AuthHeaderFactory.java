package org.teiid.resource.adapter.google.auth;

/**
 * Google services are authenticated using Http headers. Format and content
 * of this header differs based on authentication mechanism. 
 * 
 * Implementors of this interface should choose provide the header for auth purposes.
 * 
 * @author fnguyen
 *
 */
public interface AuthHeaderFactory {
	
	/**
	 * Gets the authorization header. Typically performs the login (interaction
	 * with google services). Should be called only when necessary (first login, google session expires) 
	 */
	public void login();
	public String getAuthHeader();
}
