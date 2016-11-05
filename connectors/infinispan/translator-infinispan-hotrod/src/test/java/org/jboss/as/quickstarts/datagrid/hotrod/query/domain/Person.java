/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013, Red Hat, Inc. and/or its affiliates, and individual
 * contributors by the @authors tag. See the copyright.txt in the
 * distribution for a full listing of individual contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.jboss.as.quickstarts.datagrid.hotrod.query.domain;

import java.util.ArrayList;

import org.infinispan.protostream.annotations.ProtoDoc;
import org.infinispan.protostream.annotations.ProtoField;

/**
 * Copied from JDG quickstart - remote-query
 * 
 * @author Adrian Nistor
 */
@ProtoDoc("@Indexed")
public class Person {

   public String name;
   public int id;
   public String email;
   public Address address;

	public ArrayList<PhoneNumber> phones;

	@ProtoField(number = 2, required = true)
   public String getName() {
      return name;
   }

   public void setName(String name) {
      this.name = name;
   }

	@ProtoField(number = 1, required = true)
   public int getId() {
      return id;
   }

   public void setId(int id) {
      this.id = id;
   }

   
	@ProtoField(number = 3)
   public String getEmail() {
      return email;
   }

   public void setEmail(String email) {
      this.email = email;
   }

	@ProtoField(number = 4)
   public ArrayList<PhoneNumber> getPhones() {
      return phones;
   }

   public void setPhones(ArrayList<PhoneNumber> phones) {
      this.phones = phones;
   }
   
	@ProtoField(number = 5)
   public Address getAddress() {
	   return this.address;
   }
   
   public void setAddress(Address address) {
	   this.address = address;
   }

   @Override
   public String toString() {
      return "Person{" +
            "name='" + name + '\'' +
            ", id=" + id +
            ", email='" + email + '\'' +
            ", phones=" + phones +
            '}';
   }
}
