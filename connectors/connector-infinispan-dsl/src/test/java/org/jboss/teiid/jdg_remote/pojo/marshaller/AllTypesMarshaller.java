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
package org.jboss.teiid.jdg_remote.pojo.marshaller;


import java.io.IOException;

import org.infinispan.protostream.MessageMarshaller;
import org.jboss.teiid.jdg_remote.pojo.AllTypes;

/**
 * @author Adrian Nistor
 */
public class AllTypesMarshaller implements MessageMarshaller<AllTypes> {

   @Override
   public String getTypeName() {
      return "quickstart.Person";
   }

   @Override
   public Class<AllTypes> getJavaClass() {
      return AllTypes.class;
   }

   @Override
   public AllTypes readFrom(ProtoStreamReader reader) throws IOException {	
//	private char charValue;
//	private BigInteger bigIntegerValue;
//	private Short shortValue;
//	private Float floatNum;
//	private byte[]  objectValue;
//	
//	private Integer intNum;
//	private BigDecimal bigDecimalValue;
//	
//	private Long longNum;
//	private Boolean booleanValue;
//	private Timestamp timeStampValue;
//	
//	private Time timeValue;
//	private Date dateValue;
	
      String stringNum = reader.readString("stringNum");
      String stringKey = reader.readString("stringKey");
      int intKey = reader.readInt("intKey");
      Double doubleNum = reader.readDouble("doubleNum");

      AllTypes person = new AllTypes();
      person.setStringNum(stringNum);
      person.setStringKey(stringKey);
      person.setDoubleNum(doubleNum);
      person.setIntKey(intKey);
      return person;
   }

   @Override
   public void writeTo(ProtoStreamWriter writer, AllTypes at) throws IOException {
      writer.writeString("stringNum", at.getStringNum());
      writer.writeInt("intKey", at.getIntKey());
      writer.writeString("stringKey", at.getStringKey());
      writer.writeDouble("doubleNum", at.getDoubleNum());
   }
}
