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

package org.teiid.net;

import static org.junit.Assert.*;

import java.net.MalformedURLException;
import java.util.List;

import org.junit.Test;


public class TestTeiidURL {

    @Test
    public final void testTeiidURL() throws Exception {
        String SERVER_URL = "mm://localhost:31000"; //$NON-NLS-1$
        assertTrue(TeiidURL.isValidServerURL(SERVER_URL));

        TeiidURL url = new TeiidURL(SERVER_URL);
        List hosts = url.getHostInfo();
        assertNotNull("MMURL should have 1 Host", hosts );  //$NON-NLS-1$
        assertEquals(1,hosts.size());
    }

    @Test
    public final void testTeiidURLIPv6() throws Exception {
        String SERVER_URL = "mm://[3ffe:ffff:0100:f101::1]:31000"; //$NON-NLS-1$
        assertTrue(TeiidURL.isValidServerURL(SERVER_URL));

        TeiidURL url = new TeiidURL(SERVER_URL);
        List<HostInfo> hosts = url.getHostInfo();
        assertNotNull("TeiidURL should have 1 Host", hosts );  //$NON-NLS-1$
        assertEquals(1,hosts.size());
        assertEquals("3ffe:ffff:0100:f101::1", hosts.get(0).getHostName()); //$NON-NLS-1$
        assertEquals(31000, hosts.get(0).getPortNumber());
    }

    @Test
    public final void testBogusProtocol() throws Exception {
        String SERVER_URL = "foo://localhost:31000"; //$NON-NLS-1$
        assertFalse(TeiidURL.isValidServerURL(SERVER_URL));
    }

    @Test
    public final void testBogusProtocol1() {
        String SERVER_URL = "foo://localhost:31000"; //$NON-NLS-1$
        assertFalse(TeiidURL.isValidServerURL(SERVER_URL));
    }

    @Test
    public final void testTeiidURLSecure() throws Exception {
        String SERVER_URL = "mms://localhost:31000"; //$NON-NLS-1$
        assertTrue(TeiidURL.isValidServerURL(SERVER_URL));

        TeiidURL url = new TeiidURL(SERVER_URL);
        List hosts = url.getHostInfo();
        assertNotNull("MMURL should have 1 Host", hosts );  //$NON-NLS-1$
        assertEquals(1,hosts.size());
    }

    @Test
    public final void testTeiidURLBadProtocolMM() {
        String SERVER_URL = "mmm://localhost:31000"; //$NON-NLS-1$
        assertFalse(TeiidURL.isValidServerURL(SERVER_URL));
    }

    @Test
    public final void testTeiidURLWrongSlash() {
        String SERVER_URL = "mm:\\\\localhost:31000"; //$NON-NLS-1$
        assertFalse(TeiidURL.isValidServerURL(SERVER_URL));
    }

    @Test
    public final void testTeiidURLOneSlash() {
        String SERVER_URL = "mm:/localhost:31000"; //$NON-NLS-1$
        assertFalse(TeiidURL.isValidServerURL(SERVER_URL));
    }

    @Test(expected=MalformedURLException.class)
    public final void testTeiidURLNoHost() throws Exception {
        String SERVER_URL = "mm://:31000"; //$NON-NLS-1$
        assertFalse(TeiidURL.isValidServerURL(SERVER_URL));

        new TeiidURL(SERVER_URL);
    }

    @Test(expected=MalformedURLException.class)
    public final void testTeiidURLNoHostAndPort() throws Exception {
        String SERVER_URL = "mm://:"; //$NON-NLS-1$
        assertFalse(TeiidURL.isValidServerURL(SERVER_URL));

        new TeiidURL(SERVER_URL);
    }

    @Test
    public final void testTeiidURLNoHostAndPort2() {
        String SERVER_URL = "mm://"; //$NON-NLS-1$
        assertFalse(TeiidURL.isValidServerURL(SERVER_URL));
    }

    @Test
    public final void testTeiidURLBadPort() {
        String SERVER_URL = "mm://localhost:port"; //$NON-NLS-1$
        assertFalse(TeiidURL.isValidServerURL(SERVER_URL));
    }

    @Test
    public final void testTeiidURL2Hosts() throws Exception {
        String SERVER_URL = "mm://localhost:31000,localhost:31001"; //$NON-NLS-1$
        assertTrue(TeiidURL.isValidServerURL(SERVER_URL));

        TeiidURL url = new TeiidURL(SERVER_URL);
        List hosts = url.getHostInfo();
        assertNotNull("MMURL should have 2 Host", hosts );  //$NON-NLS-1$
        assertEquals(2,hosts.size());
    }

    @Test
    public final void testTeiidIPv6URL2Hosts() throws Exception {
        String SERVER_URL = "mm://[3ffe:ffff:0100:f101::1]:31000,[::1]:31001, 127.0.0.1:31003"; //$NON-NLS-1$
        assertTrue(TeiidURL.isValidServerURL(SERVER_URL));

        TeiidURL url = new TeiidURL(SERVER_URL);
        List<HostInfo> hosts = url.getHostInfo();
        assertNotNull("TeiidURL should have 3 Host", hosts );  //$NON-NLS-1$
        assertEquals(3, hosts.size());

        assertEquals("3ffe:ffff:0100:f101::1", hosts.get(0).getHostName());//$NON-NLS-1$
        assertEquals(31001, hosts.get(1).getPortNumber());
        assertEquals("127.0.0.1", hosts.get(2).getHostName());//$NON-NLS-1$
    }

    @Test
    public final void testTeiidURL3Hosts() throws Exception {
        String SERVER_URL = "mm://localhost:31000,localhost:31001,localhost:31002"; //$NON-NLS-1$
        assertTrue(TeiidURL.isValidServerURL(SERVER_URL));

        TeiidURL url = new TeiidURL(SERVER_URL);
        List hosts = url.getHostInfo();
        assertNotNull("MMURL should have 3 Host", hosts );  //$NON-NLS-1$
        assertEquals(3,hosts.size());
    }

    @Test
    public final void testGetHostInfo() throws Exception {
        String SERVER_URL = "mm://localhost:31000"; //$NON-NLS-1$
        assertTrue(TeiidURL.isValidServerURL(SERVER_URL));

        TeiidURL url = new TeiidURL(SERVER_URL);
        assertNotNull(url.getHostInfo() );
    }

    @Test
    public final void testGetProtocolStandalone() throws Exception {
        TeiidURL url = new TeiidURL("mm://localhost:31000"); //$NON-NLS-1$
        assertNotNull(url);
        assertEquals("mm://localhost:31000",url.getAppServerURL()); //$NON-NLS-1$
    }

    @Test
    public final void testHasMoreElements() throws Exception {
        TeiidURL url = new TeiidURL("mm://localhost:31000,localhost:31001"); //$NON-NLS-1$
        assertNotNull(url);
        assertFalse(url.getHostInfo().isEmpty());
    }

    @Test
    public final void testNextElement() throws Exception {
        TeiidURL url = new TeiidURL("mm://localhost:31000,localhost:31001"); //$NON-NLS-1$
        assertEquals(2, url.getHostInfo().size());
    }

    @Test
    public final void testHostInfoEquals() throws Exception {
        HostInfo expectedResults = new HostInfo("localhost",31000);  //$NON-NLS-1$
        TeiidURL url = new TeiidURL("mm://localhost:31000"); //$NON-NLS-1$
        HostInfo actualResults = url.getHostInfo().get(0);
        assertEquals(expectedResults,actualResults);
    }

    @Test
    public final void testWithEmbeddedSpaces() throws Exception {
        HostInfo expectedResults = new HostInfo("localhost",12345);  //$NON-NLS-1$

        TeiidURL url = new TeiidURL("mm://localhost : 12345"); //$NON-NLS-1$
        List hosts = url.getHostInfo();
        assertNotNull("MMURL should have 1 Host", hosts );  //$NON-NLS-1$
        assertEquals(1,hosts.size());
        HostInfo actualResults = url.getHostInfo().get(0);
        assertEquals(expectedResults,actualResults);
    }

    @Test
    public final void testHostPortConstructor() {
        HostInfo expectedResults = new HostInfo("myhost", 12345);  //$NON-NLS-1$

        TeiidURL url = new TeiidURL("myhost", 12345, false); //$NON-NLS-1$
        List hosts = url.getHostInfo();
        assertNotNull("MMURL should have 1 Host", hosts );  //$NON-NLS-1$
        assertEquals(1,hosts.size());
        HostInfo actualResults = url.getHostInfo().get(0);
        assertEquals(expectedResults,actualResults);
        assertEquals("mm://myhost:12345", url.getAppServerURL()); //$NON-NLS-1$
    }

    @Test
    public final void testHostPortConstructorSSL() {
        HostInfo expectedResults = new HostInfo("myhost",12345);  //$NON-NLS-1$

        TeiidURL url = new TeiidURL("myhost", 12345, true); //$NON-NLS-1$
        List hosts = url.getHostInfo();
        assertNotNull("MMURL should have 1 Host", hosts );  //$NON-NLS-1$
        assertEquals(1,hosts.size());
        HostInfo actualResults = url.getHostInfo().get(0);
        assertEquals(expectedResults,actualResults);
        assertEquals("mms://myhost:12345", url.getAppServerURL()); //$NON-NLS-1$
    }

}
