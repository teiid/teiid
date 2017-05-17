/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.teiid.translator.marshallers;

import java.io.IOException;

import org.infinispan.protostream.MessageMarshaller;

public class G3Marshaller implements MessageMarshaller<G3> {

    @Override
    public String getTypeName() {
        return "pm1.G3";
    }

    @Override
    public Class<G3> getJavaClass() {
        return G3.class;
    }

    @Override
    public G3 readFrom(ProtoStreamReader reader) throws IOException {
        int e1 = reader.readInt("e1");
        String e2 = reader.readString("e2");

        G3 g3 = new G3();
        g3.setE1(e1);
        g3.setE2(e2);
        return g3;
    }

    @Override
    public void writeTo(ProtoStreamWriter writer, G3 g3) throws IOException {
        writer.writeInt("e1", g3.getE1());
        writer.writeString("e2", g3.getE2());
    }
}