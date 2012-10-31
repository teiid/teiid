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

package org.teiid.resource.adapter.google.auth;

import java.util.Date;

public class OAuth2Tokens {
	private String accessToken = null;
	private String refreshToken = null;
	private Integer expiresIn = null;
	private Date created = null;
	
	public String getAccessToken() {
		return accessToken;
	}
	public String getRefreshToken() {
		return refreshToken;
	}
	public Integer getExpiresIn() {
		return expiresIn;
	}
	
	
	public Date getCreated() {
		return created;
	}
	public OAuth2Tokens(Object accessToken, Object refreshToken,
			Object expiresIn) {
		super();
		this.accessToken = (String)accessToken;
		this.refreshToken =(String) refreshToken;
		Number n = (Number)expiresIn;
		if (n != null) {
			this.expiresIn = n.intValue();
		}
		created = new Date();
	}

	@SuppressWarnings("nls")
	@Override
	public String toString() {
		return "AccessTokens [accessToken=" + accessToken
				+ ", refreshToken=" + refreshToken + ", expiresIn="
				+ expiresIn + "]";
	}
}