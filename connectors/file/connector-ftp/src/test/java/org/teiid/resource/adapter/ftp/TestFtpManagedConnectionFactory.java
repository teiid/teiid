package org.teiid.resource.adapter.ftp;

import static org.junit.Assert.*;

import org.junit.Test;

public class TestFtpManagedConnectionFactory {

    @Test
    public void testConfigProperty() {
        FtpManagedConnectionFactory mcf = new FtpManagedConnectionFactory();
        String cipherSuites = "RSA,DH,ECDH,ECDHE"; //$NON-NLS-1$
        mcf.setCipherSuites(cipherSuites);
        assertEquals(cipherSuites, mcf.getCipherSuites());
    }

}
