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
package org.teiid.stateservice;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.teiid.stateservice.jaxb.StateInfo;

public class StateData {
	
	Map<String, StateInfo> data;
	
	public StateData() {
		data = new HashMap<String, StateInfo>();
		data.put("AL", new StateInfo("Alabama","AL","1819","Montgomery"));
		data.put("AK", new StateInfo("Alaska","AK","1959","Juneau"));
		data.put("AZ", new StateInfo("Arizona","AZ","1912","Phoenix"));
		data.put("AR", new StateInfo("Arkansas","AR","1836","Little Rock"));
		data.put("CA", new StateInfo("California","CA","1850","Sacramento"));
		data.put("CO", new StateInfo("Colorado","CO","1876","Denver"));
		data.put("CT", new StateInfo("Connecticut","CT","1788","Hartford"));
		data.put("DE", new StateInfo("Delaware","DE","1787","Dover"));
		data.put("FL", new StateInfo("Florida","FL","1845","Tallahassee"));
		data.put("GA", new StateInfo("Georgia","GA","1788","Atlanta"));
		data.put("HI", new StateInfo("Hawaii","HI","1959","Honolulu"));
		data.put("ID", new StateInfo("Idaho","ID","1890","Boise"));
		data.put("IL", new StateInfo("Illinois","IL","1818","Springfield"));
		data.put("IN", new StateInfo("Indiana","IN","1816","Indianapolis"));
		data.put("IA", new StateInfo("Iowa","IA","1846","Des Moines"));
		data.put("KS", new StateInfo("Kansas","KS","1861","Topeka"));
		data.put("KY", new StateInfo("Kentucky","KY","1792","Frankfort"));
		data.put("LA", new StateInfo("Louisiana","LA","1812","Baton Rouge"));
		data.put("ME", new StateInfo("Maine","ME","1820","Augusta"));
		data.put("MD", new StateInfo("Maryland","MD","1788","Annapolis"));
		data.put("MA", new StateInfo("Massachusetts","MA","1788","Boston"));
		data.put("MI", new StateInfo("Michigan","MI","1837","Lansing"));
		data.put("MN", new StateInfo("Minnesota","MN","1858","Saint Paul"));
		data.put("MS", new StateInfo("Mississippi","MS","1817","Jackson"));
		data.put("MO", new StateInfo("Missouri","MO","1821","Jefferson City"));
		data.put("MT", new StateInfo("Montana","MT","1889","Helena"));
		data.put("NE", new StateInfo("Nebraska","NE","1867","Lincoln"));
		data.put("NV", new StateInfo("Nevada","NV","1864","Carson City"));
		data.put("NH", new StateInfo("New Hampshire","NH","1788","Concord"));
		data.put("NJ", new StateInfo("New Jersey","NJ","1787","Trenton"));
		data.put("NM", new StateInfo("New Mexico","NM","1912","Santa Fe"));
		data.put("NY", new StateInfo("New York","NY","1788","Albany"));
		data.put("NC", new StateInfo("North Carolina","NC","1789","Raleigh"));
		data.put("ND", new StateInfo("North Dakota","ND","1889","Bismarck"));
		data.put("OH", new StateInfo("Ohio","OH","1803","Columbus"));
		data.put("OK", new StateInfo("Oklahoma","OK","1907","Oklahoma City"));
		data.put("OR", new StateInfo("Oregon","OR","1859","Salem"));
		data.put("PA", new StateInfo("Pennsylvania","PA","1787","Harrisburg"));
		data.put("RI", new StateInfo("Rhode Island","RI","1790","Providence"));
		data.put("SC", new StateInfo("South Carolina","SC","1788","Columbia"));
		data.put("SD", new StateInfo("South Dakota","SD","1889","Pierre"));
		data.put("TN", new StateInfo("Tennessee","TN","1796","Nashville"));
		data.put("TX", new StateInfo("Texas","TX","1845","Austin"));
		data.put("UT", new StateInfo("Utah","UT","1896","Salt Lake City"));
		data.put("VT", new StateInfo("Vermont","VT","1791","Montpelier"));
		data.put("VA", new StateInfo("Virginia","VA","1788","Richmond"));
		data.put("WA", new StateInfo("Washington","WA","1889","Olympia"));
		data.put("WV", new StateInfo("West Virginia","WV","1863","Charleston"));
		data.put("WI", new StateInfo("Wisconsin","WI","1848","Madison"));
		data.put("WY", new StateInfo("Wyoming","WY","1890","Cheyenne"));

	}

	public List<StateInfo> getAll() {
		return new ArrayList<StateInfo>(data.values());
	}

	public StateInfo getData(String stateCode) {
		return data.get(stateCode.toUpperCase());
	}

}
