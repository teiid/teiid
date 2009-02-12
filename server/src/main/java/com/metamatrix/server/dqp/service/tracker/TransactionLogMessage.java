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

package com.metamatrix.server.dqp.service.tracker;

import java.io.Serializable;

import com.metamatrix.core.util.DateUtil;
import com.metamatrix.dqp.spi.TrackerLogConstants;

/**
 * 
 */
public class TransactionLogMessage implements Serializable{
	public static final short DEST_TXNLOG = 0;
	public static final short DEST_MMXCMDLOG = 1;
	public static final short DEST_SRCCMDLOG = 2;
	
	private String txnUid;
	private short point;
	private short status;
	private String sessionUid;
    private String applicationName;
	private String principal;
	private String vdbName;
	private String vdbVersion;
	private String beginTimeStamp;
	private String endTimeStamp;
	private String requestId;
	private String sql;
	private long nodeID; //subcommand ID
	private String modelName;
	private String cbName; //connector binding name
	private int rowCount;
	private String subTxnUid;
	private short tableDest;
	
	public TransactionLogMessage(String txnUid, short txnPoint, short status,
            String sessionUid, String principal, String vdbName, String vdbVersion, long timestamp){
		
		this.txnUid = txnUid;
		this.point = txnPoint;
		this.status = status;
		this.sessionUid = sessionUid;
		this.principal = principal;
		this.vdbName = vdbName;
		this.vdbVersion = vdbVersion;
		if(point == TrackerLogConstants.CMD_POINT.BEGIN){
			beginTimeStamp = DateUtil.getDateAsString(timestamp);
		}else if(point == TrackerLogConstants.CMD_POINT.END){
			endTimeStamp = DateUtil.getDateAsString(timestamp);
		}
		tableDest = DEST_TXNLOG;
	}
	
	public TransactionLogMessage(String requestId, String txnUid, short cmdPoint, short status,
            String sessionUid, String applicationName, String principal, String vdbName,
            String vdbVersion, String sql, int rowCount, long timestamp){
		
		this.txnUid = txnUid;
		this.point = cmdPoint;
		this.sessionUid = sessionUid;
        this.applicationName = applicationName;
		this.principal = principal;
		this.vdbName = vdbName;
		this.vdbVersion = vdbVersion;
		this.sql = sql;
		this.requestId = requestId;
		this.status = status;
        this.rowCount = rowCount;
		if(point == TrackerLogConstants.CMD_POINT.BEGIN){
			beginTimeStamp = DateUtil.getDateAsString(timestamp);
		}else if(point == TrackerLogConstants.CMD_POINT.END){
			endTimeStamp = DateUtil.getDateAsString(timestamp);
		}
		tableDest = DEST_MMXCMDLOG;
	}
	
	public TransactionLogMessage(String requestId, long nodeID, String subTxnUid, 
			short status, String modelName, String cbName, short cmdPoint, 
            String sessionUid, String principal, String sql, int rowCount, long timestamp){
		
		this.subTxnUid = subTxnUid;
		this.point = cmdPoint;
		this.sessionUid = sessionUid;
		this.principal = principal;
		this.cbName = cbName;
		this.status = status;
		this.nodeID = nodeID;
		this.modelName = modelName;
		this.rowCount = rowCount;
		this.requestId = requestId;
		this.sql = sql;
		if(point == TrackerLogConstants.CMD_POINT.BEGIN){
			beginTimeStamp = DateUtil.getDateAsString(timestamp);
		}else if(point == TrackerLogConstants.CMD_POINT.END){
			endTimeStamp = DateUtil.getDateAsString(timestamp);
		}
		tableDest = DEST_SRCCMDLOG;
	}
	
	public String getTxnUid(){
		return this.txnUid;	
	}
	
	public short getPoint(){
		return this.point;	
	}
	
	public short getStatus(){
		return this.status;	
	}
	
    public String getSessionUid(){
        return this.sessionUid; 
    }
    
    public String getApplicationName(){
        return this.applicationName; 
    }
    
	public String getPrincipal(){
		return this.principal;	
	}
	
	public String getVdbName(){
		return this.vdbName;	
	}
	
	public String getVdbVersion(){
		return this.vdbVersion;	
	}
	
	public String getBeginTimeStamp(){
		return this.beginTimeStamp;	
	}
	
	public String getEndTimeStamp(){
		return this.endTimeStamp;	
	}
	
	public String getRequestId(){
		return this.requestId;	
	}
	
	public String getSql(){
		return this.sql;	
	}
	
	public long getType(){
		return this.nodeID;	
	}
	
	public String getModelName(){
		return this.modelName;	
	}
	
	public String getConnectorBindingName(){
		return this.cbName;	
	}
	
	public int getRowCount(){
		return this.rowCount;	
	}
	
	public String getSubTxnUid(){
		return this.subTxnUid;	
	}
	
	public short getDestinationTable(){
		return this.tableDest;	
	}
    
    /** 
     * @see java.lang.Object#toString()
     */
    public String toString() {
        StringBuffer sb = new StringBuffer();
        
        sb.append("TXNUID"); //$NON-NLS-1$
        sb.append(": "); //$NON-NLS-1$
        sb.append(this.txnUid);
        sb.append(" "); //$NON-NLS-1$
        sb.append("REQUESTID"); //$NON-NLS-1$
        sb.append(": "); //$NON-NLS-1$
        sb.append(this.requestId);
        sb.append(" "); //$NON-NLS-1$
        sb.append("SQL_CMD"); //$NON-NLS-1$
        sb.append(": "); //$NON-NLS-1$
        sb.append(this.sql);
        sb.append(" "); //$NON-NLS-1$
        sb.append("CREATED_TS"); //$NON-NLS-1$
        sb.append(": "); //$NON-NLS-1$
        sb.append(this.beginTimeStamp);
        sb.append(" "); //$NON-NLS-1$
        sb.append("ENDED_TS"); //$NON-NLS-1$
        sb.append(": "); //$NON-NLS-1$
        sb.append(this.endTimeStamp);
        sb.append(" "); //$NON-NLS-1$
        sb.append("CNCTRNAME"); //$NON-NLS-1$
        sb.append(": "); //$NON-NLS-1$
        sb.append(this.cbName);
        sb.append(" "); //$NON-NLS-1$
        sb.append("MDL_NM"); //$NON-NLS-1$
        sb.append(": "); //$NON-NLS-1$
        sb.append(this.modelName);
        sb.append(" "); //$NON-NLS-1$
        sb.append("NODEID"); //$NON-NLS-1$
        sb.append(": "); //$NON-NLS-1$
        sb.append(this.nodeID);
        sb.append(" "); //$NON-NLS-1$
        sb.append("APP_NAME"); //$NON-NLS-1$
        sb.append(": "); //$NON-NLS-1$
        sb.append(this.applicationName);    
        sb.append(" "); //$NON-NLS-1$
        sb.append("CMDPOINT"); //$NON-NLS-1$
        sb.append(": "); //$NON-NLS-1$
        sb.append(this.point);
        sb.append(" "); //$NON-NLS-1$
        sb.append("STATUS"); //$NON-NLS-1$
        sb.append(": "); //$NON-NLS-1$
        sb.append(this.status);
        sb.append(" "); //$NON-NLS-1$
        sb.append("SESSIONUID"); //$NON-NLS-1$
        sb.append(": "); //$NON-NLS-1$
        sb.append(this.sessionUid);
        sb.append(" "); //$NON-NLS-1$
        sb.append("PRINCIPAL_NA"); //$NON-NLS-1$
        sb.append(": "); //$NON-NLS-1$
        sb.append(this.principal);
        sb.append(" "); //$NON-NLS-1$
        sb.append("VDBNAME"); //$NON-NLS-1$
        sb.append(": "); //$NON-NLS-1$
        sb.append(this.vdbName);
        sb.append(" "); //$NON-NLS-1$
        sb.append("VDBVERSION"); //$NON-NLS-1$
        sb.append(": "); //$NON-NLS-1$
        sb.append(this.vdbVersion);
        sb.append(" "); //$NON-NLS-1$
        sb.append("FINL_ROWCNT"); //$NON-NLS-1$
        sb.append(": "); //$NON-NLS-1$
        sb.append(this.rowCount);
        sb.append(" "); //$NON-NLS-1$
        sb.append("SUBTXNUID"); //$NON-NLS-1$
        sb.append(": "); //$NON-NLS-1$
        sb.append(this.subTxnUid);
        sb.append(" "); //$NON-NLS-1$
        sb.append("DESTINATION_TABLE"); //$NON-NLS-1$
        sb.append(": "); //$NON-NLS-1$
        sb.append(this.tableDest);
        return sb.toString();
    }
}
