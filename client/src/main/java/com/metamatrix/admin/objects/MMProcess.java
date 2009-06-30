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

 package com.metamatrix.admin.objects;

import java.net.InetAddress;

import org.teiid.adminapi.AdminObject;
import org.teiid.adminapi.ProcessObject;
import org.teiid.adminapi.QueueWorkerPool;

import com.metamatrix.admin.AdminPlugin;

/**
 * Data holder for information about a Process.
 */
public final class MMProcess extends MMAdminObject implements ProcessObject {
	private long freeMemory = 0;
    private String hostIdentifier = "";  //$NON-NLS-1$
    private InetAddress inetAddress = null;
    private int port = 0;

	private boolean running = false;
	private int threadCount = 0;
	private long totalMemory = 0;
    
    private int sockets = 0;
    private int maxSockets = 0;
    private int virtualSockets = 0;
    private int maxVirtualSockets = 0;
    private long objectsRead = 0;
    private long objectsWritten = 0;
    
    private QueueWorkerPool queueWorkerPool = null;
    

	
    
    /**
     * Contruct a new MMProcess.
     * @param identifierParts
     */
    public MMProcess(String[] identifierParts) {
        super(identifierParts);
        
        hostIdentifier = identifierParts[0];
    }
    

    /**
     * Build the Identifer, as an array of its parts.
     * Contains special handling for host, because it may contain dots.
     * @param identifier
     *  
     * @return the Identifer, as an array of its parts
     * @since 4.3
     */
    public static String[] buildIdentifierArray(String identifier) {
        int index = identifier.lastIndexOf(AdminObject.DELIMITER_CHAR);
        
        String host = identifier.substring(0, index); 
        String process = identifier.substring(index+1);
        
        return new String[] {host, process};
   
    }
    
    
    
	/**
	 * @return amount of free memory for this Java process.
	 */
	public long getFreeMemory() {
		return freeMemory;
	}
    
    
    /**
     * @return host name for this process.
     */
    public String getHostIdentifier() {
        return hostIdentifier;
    }
    
    /**
     * @return port for this process.
     */
    public InetAddress getInetAddress() {
        return inetAddress;
    }
    
    /**
     * @return port for this process.
     */
    public int getPort() {
        return port;
    }

	/**
     * @return thread count for this Java process.
	 */
	public int getThreadCount() {
		return threadCount;
	}

	/**
	 * @return total memory allocated for this Java process.
	 */
	public long getTotalMemory() {
		return totalMemory;
	}

	
	/**
     * @return whether this process is running.
	 * @since 4.3
	 */
	public boolean isRunning() {
		return running;
	}
    
    
    
    /**
     * @param freeMemory The freeMemory to set.
     * @since 4.3
     */
    public void setFreeMemory(long freeMemory) {
        this.freeMemory = freeMemory;
    }


    /**
     * @param inetAddress The inetAddress to set.
     * @since 4.3
     */
    public void setInetAddress(InetAddress inetAddress) {
        this.inetAddress = inetAddress;
    }

    /**
     * @param port The port to set.
     * @since 4.3
     */
    public void setPort(int port) {
        this.port = port;
    }
    
    /**
     * Set whether this process is running. 
     * @param running
     * @since 4.3
     */
    public void setRunning(boolean running) {
        this.running = running;
    }

    /**
     * @param threadCount The threadCount to set.
     * @since 4.3
     */
    public void setThreadCount(int threadCount) {
        this.threadCount = threadCount;
    }

    /**
     * @param totalMemory The totalMemory to set.
     * @since 4.3
     */
    public void setTotalMemory(long totalMemory) {
        this.totalMemory = totalMemory;
    }

	
	
	/**
     * Return string for display. 
	 * @see java.lang.Object#toString()
	 * @since 4.3
	 */
	public String toString() {
		StringBuffer result = new StringBuffer();
		result.append(AdminPlugin.Util.getString("MMProcess.MMProcess")).append(getIdentifier()); //$NON-NLS-1$
        result.append(AdminPlugin.Util.getString("MMProcess.Properties")).append(getPropertiesAsString()); //$NON-NLS-1$
		result.append(AdminPlugin.Util.getString("MMProcess.Created")).append(getCreatedDate()); //$NON-NLS-1$
		result.append(AdminPlugin.Util.getString("MMProcess.CreatedBy")).append(getCreatedBy()); //$NON-NLS-1$
		result.append(AdminPlugin.Util.getString("MMProcess.Updated")).append(getLastChangedDate()); //$NON-NLS-1$
		result.append(AdminPlugin.Util.getString("MMProcess.UpdatedBy")).append(getLastChangedBy()); //$NON-NLS-1$
		result.append(AdminPlugin.Util.getString("MMProcess.IsRunning")).append(isRunning()); //$NON-NLS-1$
        result.append(AdminPlugin.Util.getString("MMProcess.TotalMemory")).append(totalMemory); //$NON-NLS-1$
		result.append(AdminPlugin.Util.getString("MMProcess.FreeMemory")).append(freeMemory); //$NON-NLS-1$
		result.append(AdminPlugin.Util.getString("MMProcess.ThreadCount")).append(threadCount); //$NON-NLS-1$
        result.append(AdminPlugin.Util.getString("MMProcess.HostIdentifier")).append(hostIdentifier); //$NON-NLS-1$
        result.append(AdminPlugin.Util.getString("MMProcess.Port")).append(port); //$NON-NLS-1$
        result.append(AdminPlugin.Util.getString("MMProcess.InetAddress")).append(inetAddress); //$NON-NLS-1$
        result.append(AdminPlugin.Util.getString("MMProcess.Sockets")).append(sockets); //$NON-NLS-1$
        result.append(AdminPlugin.Util.getString("MMProcess.MaxSockets")).append(maxSockets); //$NON-NLS-1$
        result.append(AdminPlugin.Util.getString("MMProcess.VirtualSockets")).append(virtualSockets); //$NON-NLS-1$
        result.append(AdminPlugin.Util.getString("MMProcess.MaxVirtualSockets")).append(maxVirtualSockets); //$NON-NLS-1$
        result.append(AdminPlugin.Util.getString("MMProcess.ObjectsRead")).append(objectsRead); //$NON-NLS-1$
        result.append(AdminPlugin.Util.getString("MMProcess.ObjectsWritten")).append(objectsWritten); //$NON-NLS-1$
        if (queueWorkerPool != null) {
            result.append(AdminPlugin.Util.getString("MMProcess.QueueWorkerPool")).append(queueWorkerPool.toString()); //$NON-NLS-1$
        }
		return result.toString();
	}
    
    
    /** 
     * @return Returns the maxSockets.
     * @since 4.3
     */
    public int getMaxSockets() {
        return this.maxSockets;
    }
    /** 
     * @param maxSockets The maxSockets to set.
     * @since 4.3
     */
    public void setMaxSockets(int maxSockets) {
        this.maxSockets = maxSockets;
    }
    /** 
     * @return Returns the maxVirtualSockets.
     * @since 4.3
     */
    public int getMaxVirtualSockets() {
        return this.maxVirtualSockets;
    }
    /** 
     * @param maxVirtualSockets The maxVirtualSockets to set.
     * @since 4.3
     */
    public void setMaxVirtualSockets(int maxVirtualSockets) {
        this.maxVirtualSockets = maxVirtualSockets;
    }
    /** 
     * @return Returns the objectsRead.
     * @since 4.3
     */
    public long getObjectsRead() {
        return this.objectsRead;
    }
    /** 
     * @param objectsRead The objectsRead to set.
     * @since 4.3
     */
    public void setObjectsRead(long objectsRead) {
        this.objectsRead = objectsRead;
    }
    /** 
     * @return Returns the objectsWritten.
     * @since 4.3
     */
    public long getObjectsWritten() {
        return this.objectsWritten;
    }
    /** 
     * @param objectsWritten The objectsWritten to set.
     * @since 4.3
     */
    public void setObjectsWritten(long objectsWritten) {
        this.objectsWritten = objectsWritten;
    }
    /** 
     * @return Returns the sockets.
     * @since 4.3
     */
    public int getSockets() {
        return this.sockets;
    }
    /** 
     * @param sockets The sockets to set.
     * @since 4.3
     */
    public void setSockets(int sockets) {
        this.sockets = sockets;
    }
    /** 
     * @return Returns the virtualSockets.
     * @since 4.3
     */
    public int getVirtualSockets() {
        return this.virtualSockets;
    }
    /** 
     * @param virtualSockets The virtualSockets to set.
     * @since 4.3
     */
    public void setVirtualSockets(int virtualSockets) {
        this.virtualSockets = virtualSockets;
    }
    /** 
     * @return Returns the queueWorkerPool.
     * @since 4.3
     */
    public QueueWorkerPool getQueueWorkerPool() {
        return this.queueWorkerPool;
    }
    /** 
     * @param queueWorkerPool The queueWorkerPool to set.
     * @since 4.3
     */
    public void setQueueWorkerPool(QueueWorkerPool queueWorkerPool) {
        this.queueWorkerPool = queueWorkerPool;
    }
    
    
    /** 
     * @return Returns the processID.
     * @since 4.3
     */
    public String getProcessName() {
        return identifierParts[1];
    }
    
    /** 
     * @return Returns the hostName.
     * @since 4.3
     */
    public String getHostName() {
        return identifierParts[0];
    }
}
