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
package org.jboss.teiid.jdg_remote.pojo;

import java.io.IOException;

import org.infinispan.protostream.MessageMarshaller;

/**
 */
public class AllTypesMarshaller implements MessageMarshaller<AllTypes> {

   @Override
   public String getTypeName() {
      return "org.jboss.qe.jdg_remote.protobuf.AllTypes";
   }

   @Override
   public Class<AllTypes> getJavaClass() {
      return AllTypes.class;
   }


   @Override
   public AllTypes readFrom(ProtoStreamReader reader) throws IOException {
      String stringkey = reader.readString("stringKey");
      int intkey = reader.readInt("intKey");
      byte[] byteArrayValue = reader.readBytes("byteArrayValue");
      boolean booleanValue = reader.readBoolean("booleanValue");

      
      AllTypes at = new AllTypes();
      at.setStringKey(stringkey);
      at.setIntKey(intkey);
      at.setByteArrayValue(byteArrayValue);
      at.setBooleanValue(booleanValue);

      return at;
   }

   @Override
   public void writeTo(ProtoStreamWriter writer, AllTypes alltypes) throws IOException {
	   writer.writeInt("intKey", alltypes.getIntKey());
	   writer.writeString("stringKey", alltypes.getStringKey());
	   writer.writeBytes("byteArrayValue", alltypes.getByteArrayValue());
	   writer.writeBoolean("booleanValue", alltypes.getBooleanValue());

   }
}
