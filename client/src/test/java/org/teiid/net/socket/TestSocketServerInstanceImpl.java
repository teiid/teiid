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

package org.teiid.net.socket;

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.InetAddress;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import org.junit.Test;
import org.teiid.client.security.ILogon;
import org.teiid.client.util.ResultsFuture;
import org.teiid.net.CommunicationException;
import org.teiid.net.HostInfo;

@SuppressWarnings("nls")
public class TestSocketServerInstanceImpl {

    private static class FakeObjectChannel implements ObjectChannel, ObjectChannelFactory {
        List<Object> msgs = new ArrayList<Object>();
        List<? extends Object> readMsgs;
        int readCount;

        public FakeObjectChannel(List<? extends Object> readMsgs) {
            this.readMsgs = readMsgs;
        }

        @Override
        public void close() {

        }

        @Override
        public boolean isOpen() {
            return true;
        }

        @Override
        public Future<?> write(Object msg) {
            msgs.add(msg);
            ResultsFuture<?> result = new ResultsFuture<Void>();
            result.getResultsReceiver().receiveResults(null);
            return result;
        }

        @Override
        public Object read() throws IOException,
                ClassNotFoundException {
            if (readCount >= readMsgs.size()) {
            return "";
            }

            Object msg = readMsgs.get(readCount++);
            if (msg instanceof IOException) {
                if (msg instanceof SocketTimeoutException) {
                    try {
                        Thread.sleep(5);
                    } catch (InterruptedException e) {
                    }
                }
                throw (IOException)msg;
            }
            return msg;
        }

        @Override
        public SocketAddress getRemoteAddress() {
            return null;
        }

        @Override
        public ObjectChannel createObjectChannel(HostInfo info)
                throws CommunicationException, IOException {
            return this;
        }

        @Override
        public int getSoTimeout() {
            return 1;
        }

        @Override
        public InetAddress getLocalAddress() {
            return null;
        }

    }

    @Test public void testHandshakeTimeout() throws Exception {
        SocketTimeoutException[] exs = new SocketTimeoutException[1];
        Arrays.fill(exs, new SocketTimeoutException());
        final FakeObjectChannel channel = new FakeObjectChannel(Arrays.asList(exs));

        try {
            createInstance(channel);
            fail("Exception expected"); //$NON-NLS-1$
        } catch (IOException e) {

        }
    }

    private SocketServerInstanceImpl createInstance(ObjectChannelFactory channelFactory)
            throws CommunicationException, IOException {
        HostInfo info = new HostInfo("0.0.0.0", 1);
        info.getInetAddress();
        SocketServerInstanceImpl ssii = new SocketServerInstanceImpl(info, 1, 1);
        ssii.connect(channelFactory);
        return ssii;
    }

    @Test public void testSuccessfulHandshake() throws Exception {
        final FakeObjectChannel channel = new FakeObjectChannel(Arrays.asList(new Handshake(), new SocketTimeoutException()));

        SocketServerInstanceImpl instance = createInstance(channel);

        //no remote server is hooked up, so this will timeout
        ILogon logon = instance.getService(ILogon.class);
        try {
            logon.logon(new Properties());
            fail("Exception expected"); //$NON-NLS-1$
        } catch (SingleInstanceCommunicationException e) {
            assertTrue(e.getCause() instanceof TimeoutException);
        }
    }

}
