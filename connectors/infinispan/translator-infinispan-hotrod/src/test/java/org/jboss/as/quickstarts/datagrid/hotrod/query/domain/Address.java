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
package org.jboss.as.quickstarts.datagrid.hotrod.query.domain;

import org.infinispan.protostream.annotations.ProtoDoc;
import org.infinispan.protostream.annotations.ProtoField;

/**
 * @author vhalbert
 *
 */
@ProtoDoc("@Indexed")
public class Address {

	private String address;
	private String city;
	private String state;
	/**
	 * @return address
	 */
	@ProtoField(number = 1, required = true)
	public String getAddress() {
		return address;
	}
	/**
	 * @param address Sets address to the specified value.
	 */
	public void setAddress(String address) {
		this.address = address;
	}
	/**
	 * @return city
	 */
	@ProtoField(number = 2, required = true)
	public String getCity() {
		return city;
	}
	/**
	 * @param city Sets city to the specified value.
	 */
	public void setCity(String city) {
		this.city = city;
	}
	/**
	 * @return state
	 */
	@ProtoField(number = 3, required = true)
	public String getState() {
		return state;
	}
	/**
	 * @param state Sets state to the specified value.
	 */
	public void setState(String state) {
		this.state = state;
	}
	
	
}