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
package org.teiid.translator.object.testdata.person;


/**
 *  * Copied from JDG quickstart - remote-query
 *  
 * @author Adrian Nistor
 */
public class PhoneNumber {

   private String number;
   private PhoneType type;
   
   public PhoneNumber() {
	   
   }
   
   public PhoneNumber(String number, PhoneType type) {
	   this.number = number;
	   this.type = type;
   }

   public String getNumber() {
      return number;
   }

   public void setNumber(String number) {
      this.number = number;
   }

   public PhoneType getType() {
      return type;
   }

   public void setType(PhoneType type) {
      this.type = type;
   }

   @Override
   public String toString() {
      return "PhoneNumber{" +
            "number='" + number + '\'' +
            ", type=" + type +
            '}';
   }
}
