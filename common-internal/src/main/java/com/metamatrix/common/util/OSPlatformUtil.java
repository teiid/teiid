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

package com.metamatrix.common.util;

import java.io.File;
import java.io.PrintStream;
import java.util.Properties;

import com.metamatrix.core.util.Assertion;

/**
 * PlatformAwareFileSystemView
 */
public class OSPlatformUtil {
    public static String OS_PROPERTY_NAME = "os.name"; //$NON-NLS-1$

    /** Operating system is Windows NT. */
    public static final int OS_WINNT = 1;
    /** Operating system is Windows 95. */
    public static final int OS_WIN95 = 2;
    /** Operating system is Windows 98. */
    public static final int OS_WIN98 = 4;
    /** Operating system is Solaris. */
    public static final int OS_SOLARIS = 8;
    /** Operating system is Linux. */
    public static final int OS_LINUX = 16;
    /** Operating system is HP-UX. */
    public static final int OS_HP = 32;
    /** Operating system is IBM AIX. */
    public static final int OS_AIX = 64;
    /** Operating system is SGI IRIX. */
    public static final int OS_IRIX = 128;
    /** Operating system is Sun OS. */
    public static final int OS_SUNOS = 256;
    /** Operating system is Compaq TRU64 Unix */
    public static final int OS_TRU64 = 512;
    /** @deprecated please use OS_TRU64 instead */
    public static final int OS_DEC = OS_TRU64;
    /** Operating system is OS/2. */
    public static final int OS_OS2 = 1024;
    /** Operating system is Mac. */
    public static final int OS_MAC = 2048;
    /** Operating system is Windows 2000. */
    public static final int OS_WIN2000 = 4096;
    /** Operating system is Compaq OpenVMS */
    public static final int OS_VMS = 8192;
    /** Operating system is Compaq OpenVMS */
    public static final int OS_WINXP = 16384;
    /**
     *Operating system is one of the Windows variants but we don't know which
     *one it is
     */
    public static final int OS_WIN_OTHER = 16384;

    /** Operating system is unknown. */
    public static final int OS_OTHER = 65536;

    /** A mask for Windows platforms. */
    public static final int OS_WINDOWS_MASK = OS_WINNT | OS_WIN95 | OS_WIN98 | OS_WIN2000 | OS_WINXP | OS_WIN_OTHER;
    /** A mask for Unix platforms. */
    public static final int OS_UNIX_MASK = OS_SOLARIS | OS_LINUX | OS_HP | OS_AIX | OS_IRIX | OS_SUNOS | OS_TRU64 | OS_MAC;
    /** A mask for Linux platform **/
    public static final int OS_LINUX_MASK = OS_LINUX;
    
    /** The operating system on which MetaMatrix runs*/
    private static int operatingSystem = -1;

   /** Get the operating system on which the IDE is running.
    * @return one of the <code>OS_*</code> constants (such as {@link #OS_WINNT})
    */
    public static final int getOperatingSystem () {
        if (operatingSystem == -1) {
        	String osName = System.getProperty(OS_PROPERTY_NAME);
            if ("Windows NT".equals (osName)) //$NON-NLS-1$
                operatingSystem = OS_WINNT;
            else if ("Windows 95".equals (osName)) //$NON-NLS-1$
                operatingSystem = OS_WIN95;
            else if ("Windows 98".equals (osName)) //$NON-NLS-1$
                operatingSystem = OS_WIN98;
            else if ("Windows 2000".equals (osName)) //$NON-NLS-1$
                operatingSystem = OS_WIN2000;
            else if ("Windows XP".equals (osName)) //$NON-NLS-1$
                operatingSystem = OS_WINXP;
            else if (osName.startsWith("Windows ")) //$NON-NLS-1$
                operatingSystem = OS_WIN_OTHER;
            else if ("Solaris".equals (osName)) //$NON-NLS-1$
                operatingSystem = OS_SOLARIS;
            else if (osName.startsWith ("SunOS")) //$NON-NLS-1$
                operatingSystem = OS_SOLARIS;
            // JDK 1.4 b2 defines os.name for me as "Redhat Linux" -jglick
            else if (osName.endsWith ("Linux")) //$NON-NLS-1$
                operatingSystem = OS_LINUX;
            else if ("HP-UX".equals (osName)) //$NON-NLS-1$
                operatingSystem = OS_HP;
            else if ("AIX".equals (osName)) //$NON-NLS-1$
                operatingSystem = OS_AIX;
            else if ("Irix".equals (osName)) //$NON-NLS-1$
                operatingSystem = OS_IRIX;
            else if ("SunOS".equals (osName)) //$NON-NLS-1$
                operatingSystem = OS_SUNOS;
            else if ("Digital UNIX".equals (osName)) //$NON-NLS-1$
                operatingSystem = OS_TRU64;
            else if ("OS/2".equals (osName)) //$NON-NLS-1$
                operatingSystem = OS_OS2;
            else if ("OpenVMS".equals (osName)) //$NON-NLS-1$
                operatingSystem = OS_VMS;
            else if (osName.equals ("Mac OS X")) //$NON-NLS-1$
                operatingSystem = OS_MAC;
            else if (osName.startsWith ("Darwin")) //$NON-NLS-1$
                operatingSystem = OS_MAC;
            else
                operatingSystem = OS_OTHER;
        }
        return operatingSystem;
    }

   /** Test whether the IDE is running on some variant of Windows.
    * @return <code>true</code> if Windows, <code>false</code> if some other manner of operating system
    */
    public static final boolean isWindows () {
        return (getOperatingSystem () & OS_WINDOWS_MASK) != 0;
    }

   /** Test whether the IDE is running on some variant of Unix.
    * Linux is included as well as the commercial vendors.
    * @return <code>true</code> some sort of Unix, <code>false</code> if some other manner of operating system
    */
    public static final boolean isUnix () {
        return (getOperatingSystem () & OS_UNIX_MASK) != 0;
    }
    
    /** Test whether the IDE is running on some variant of Unix.
     * Linux is included as well as the commercial vendors.
     * @return <code>true</code> some sort of Unix, <code>false</code> if some other manner of operating system
     */
     public static final boolean isLinux () {
         return (getOperatingSystem () & OS_LINUX_MASK) != 0;
     }    

    static void printSystemProperties( PrintStream stream ) {
        Assertion.isNotNull(stream,"The PrintStream reference may not be null"); //$NON-NLS-1$
        Properties p = System.getProperties();
        p.list(stream);
    }
    static void printPermissions(File f, String action) {
      System.out.println("\nFile \"" + f + "\" action = " + action); //$NON-NLS-1$ //$NON-NLS-2$
      System.out.println("exists  = "+f.exists()+" canRead = "+f.canRead()+" canWrite = "+f.canWrite()); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }
    static void printRuntimeCmd(String methodName, String cmd) {
      System.out.println("\n[" + methodName + "] Runtime.exec( " + cmd + " )"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

}


