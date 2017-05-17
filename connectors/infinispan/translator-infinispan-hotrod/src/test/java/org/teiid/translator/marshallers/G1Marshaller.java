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

public class G1Marshaller implements MessageMarshaller<G1> {

    @Override
    public String getTypeName() {
        return "pm1.G1";
    }

    @Override
    public Class<G1> getJavaClass() {
        return G1.class;
    }

    @Override
    public G1 readFrom(ProtoStreamReader reader) throws IOException {
        int e1 = reader.readInt("e1");
        String e2 = reader.readString("e2");
        float e3 = reader.readFloat("e3");
        String[] e4 = reader.readArray("e4", String.class);
        String[] e5 = reader.readArray("e5", String.class);

        G1 g1 = new G1();
        g1.setE1(e1);
        g1.setE2(e2);
        g1.setE3(e3);
        if (e4.length > 0) {
            g1.setE4(e4);
        }
        if (e5.length > 0) {
            g1.setE5(e5);
        }
        return g1;
    }

    @Override
    public void writeTo(ProtoStreamWriter writer, G1 g1) throws IOException {
        writer.writeInt("e1", g1.getE1());
        writer.writeString("e2", g1.getE2());
        writer.writeFloat("e3", g1.getE3());
        writer.writeArray("e4", g1.getE4(), String.class);
        writer.writeArray("e5", g1.getE5(), String.class);
    }
}