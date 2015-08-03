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

package org.teiid.resource.adapter.google.integration;

import org.junit.Ignore;
import org.junit.Test;
import org.teiid.resource.adapter.google.auth.ClientLoginHeaderFactory;
import org.teiid.resource.adapter.google.auth.OAuth2HeaderFactory;
import org.teiid.resource.adapter.google.common.SheetRow;
import org.teiid.resource.adapter.google.dataprotocol.GoogleDataProtocolAPI;
import org.teiid.resource.adapter.google.gdata.GDataClientLoginAPI;
import org.teiid.resource.adapter.google.result.RowsResult;

/**
 * Testing ClientLogin and OAuth2. We need to authenticate with both GData protocol and Google Visualization Protocol.
 * @author fnguyen
 *
 */
@Ignore
@SuppressWarnings("nls")
public class AuthenticationTest extends IntegrationTest {
//	private String refreshToken ="1/j7FBwqkdGxCwSFAT1VxZAev7HC2qOamO9UXX_Xxz2nQ";
	private String refreshToken ="1/A6ifXgNxCYVGTkPUTnD6Y35v_GyfmuRAsKKL4eww8xs";
	
	@Test
	public void clientLoginGData(){
		GDataClientLoginAPI cla = new GDataClientLoginAPI();
		cla.setHeaderFactory(new ClientLoginHeaderFactory(USERNAME, PASSWORD));
		//TODO
		
	}
	
	@Test
	public void clientLoginVisualization(){
		GDataClientLoginAPI cla = new GDataClientLoginAPI();
		cla.setHeaderFactory(new ClientLoginHeaderFactory(USERNAME, PASSWORD));
		GoogleDataProtocolAPI gapi = new GoogleDataProtocolAPI();
		gapi.setHeaderFactory(new ClientLoginHeaderFactory(USERNAME, PASSWORD));
		gapi.setSpreadSheetBrowser(cla);
		RowsResult rresult = gapi.executeQuery("spreadsheet1", "sheet1", "select 1", 10, null, null);
		for (SheetRow sr : rresult){
			
		}
	}

	@Test
	public void oauth2GData(){
//		OAuth2Authenticator oauth = new OAuth2Authenticator();
//		AuthUrlResponse authResponse = oauth.getAuthUrl();	
		
		GDataClientLoginAPI cla = new GDataClientLoginAPI();
		OAuth2HeaderFactory fact =  new OAuth2HeaderFactory(refreshToken);
		fact.login();
		cla.setHeaderFactory(fact);
//		cla.getSpreadsheetInfo("people");
		//TODO
	}

	@Test
	public void oauth2Visualization(){
		OAuth2HeaderFactory hfa = new OAuth2HeaderFactory(refreshToken);
		hfa.login();
		GDataClientLoginAPI cla = new GDataClientLoginAPI();
		cla.setHeaderFactory(hfa);
		GoogleDataProtocolAPI gapi = new GoogleDataProtocolAPI();
		gapi.setHeaderFactory( hfa);
		gapi.setSpreadSheetBrowser(cla);
		RowsResult rresult = gapi.executeQuery("spreadsheet1", "sheet1", "select 1", 10, null, null);
		for (SheetRow sr : rresult){
			System.out.println(sr);
		}
	}
}
