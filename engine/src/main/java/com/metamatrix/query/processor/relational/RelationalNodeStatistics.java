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

package com.metamatrix.query.processor.relational;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.metamatrix.common.buffer.TupleBatch;
import com.metamatrix.query.processor.Describable;


/** 
 * @since 4.2
 */
public class RelationalNodeStatistics implements Describable {

    //  Statistics
    static final String NODE_OUTPUT_ROWS = "nodeOutputRows"; //$NON-NLS-1$
    static final String NODE_PROCESS_TIME = "nodeProcessingTime"; //$NON-NLS-1$
    static final String NODE_CUMULATIVE_PROCESS_TIME = "nodeCumulativeProcessingTime"; //$NON-NLS-1$
    static final String NODE_CUMULATIVE_NEXTBATCH_PROCESS_TIME = "nodeCumulativeNextBatchProcessingTime"; //$NON-NLS-1$
    static final String NODE_NEXT_BATCH_CALLS = "nodeNextBatchCalls"; //$NON-NLS-1$
    static final String NODE_BLOCKS = "nodeBlocks"; //$NON-NLS-1$
    static final int BATCHCOMPLETE_STOP = 0;
    static final int BLOCKEDEXCEPTION_STOP = 1;
    
    private Map statisticsProperties;
    private List statisticsList;
    private boolean setNodeStartTime;
    
    // The total amount of rows output by this node
    private int nodeOutputRows;
    
    // Start and End system time for the node
    private long nodeStartTime;
    private long nodeEndTime;
    
    // Start and End timestamps for the node
    private Timestamp nodeStartTimestamp;
    private Timestamp nodeEndTimestamp;
    
    // Start and End system time for each batch
    private long batchStartTime;
    private long batchEndTime;
     
    // The processing time for the node (does not include child processing)
    private long nodeProcessingTime;
    
    // the total processing time for the node (indcludes child processing (lastEndTime - firstStartTime)
    private long nodeCumulativeProcessingTime;
    
    // The total processing time of the nextBatch method for this node (includes child processing)
    private long nodeCumulativeNextBatchProcessingTime;
    
    // The total amount of calls to next batch in this node
    private int nodeNextBatchCalls;
    
    // The amount of times a Block or Componenet Exception occurs for this node
    private int nodeBlocks;
    
    public RelationalNodeStatistics() {
        this.statisticsProperties = new HashMap();
        this.statisticsList = new ArrayList();
        this.setNodeStartTime = false;
    }
    
    public void startBatchTimer() {
        this.batchStartTime = System.currentTimeMillis();
    }
    
    public void stopBatchTimer() {
        this.batchEndTime = System.currentTimeMillis();
    }
    
    public void collectCumulativeNodeStats(TupleBatch batch, int stopType) {
        this.nodeNextBatchCalls++;
        if(!this.setNodeStartTime) {
            this.nodeStartTime = this.batchStartTime;
            this.setNodeStartTime = true;
        }
        this.nodeCumulativeNextBatchProcessingTime += (this.batchEndTime - this.batchStartTime);
        switch (stopType) {
            case BATCHCOMPLETE_STOP:
                this.nodeOutputRows += batch.getRowCount();
                break;
            case BLOCKEDEXCEPTION_STOP:
                this.nodeBlocks++;
                break;
        }
        //this.reset();
    }
    
    public void collectNodeStats(RelationalNode[] relationalNodes, String className) {
        // set nodeEndTime to the time gathered at the end of the last batch
        this.nodeEndTime = this.batchEndTime;
        this.nodeCumulativeProcessingTime = this.nodeEndTime - this.nodeStartTime;
        this.nodeEndTimestamp = new Timestamp(this.nodeEndTime);
        this.nodeStartTimestamp = new Timestamp(this.nodeStartTime);
        if(relationalNodes[0] != null) {
            long maxUnionChildCumulativeProcessingTime = 0;
            for (int i = 0; i < relationalNodes.length; i++) {
                if(relationalNodes[i] != null) {
                    maxUnionChildCumulativeProcessingTime = Math.max(maxUnionChildCumulativeProcessingTime, relationalNodes[i].getNodeStatistics().getNodeCumulativeProcessingTime());
                    this.nodeProcessingTime = this.nodeCumulativeProcessingTime - relationalNodes[i].getNodeStatistics().getNodeCumulativeProcessingTime();                      
                }
            }
            if(className.equals("UnionAllNode")){ //$NON-NLS-1$
                this.nodeProcessingTime = this.nodeCumulativeProcessingTime - maxUnionChildCumulativeProcessingTime;
            }
        }else {
            this.nodeProcessingTime = this.nodeCumulativeProcessingTime;
        }
    }
    
    public void setDescriptionProperties() {
        this.statisticsProperties.put(NODE_OUTPUT_ROWS, new Integer(this.nodeOutputRows));
        this.statisticsProperties.put(NODE_PROCESS_TIME, new Long(this.nodeProcessingTime));
        this.statisticsProperties.put(NODE_CUMULATIVE_PROCESS_TIME, new Long(this.nodeCumulativeProcessingTime));
        this.statisticsProperties.put(NODE_CUMULATIVE_NEXTBATCH_PROCESS_TIME, new Long(this.nodeCumulativeNextBatchProcessingTime));
        this.statisticsProperties.put(NODE_NEXT_BATCH_CALLS, new Integer(this.nodeNextBatchCalls));
        this.statisticsProperties.put(NODE_BLOCKS, new Integer(this.nodeBlocks));
    }
    
    public Map getDescriptionProperties() {
        return this.statisticsProperties;
    }
    
    public void setStatisticsList() {
        this.statisticsList.clear();
        this.statisticsList.add("Node Output Rows: " + this.nodeOutputRows); //$NON-NLS-1$
        this.statisticsList.add("Node Process Time: " + this.nodeProcessingTime); //$NON-NLS-1$
        this.statisticsList.add("Node Cumulative Process Time: " + this.nodeCumulativeProcessingTime); //$NON-NLS-1$
        this.statisticsList.add("Node Cumulative Next Batch Process Time: " + this.nodeCumulativeNextBatchProcessingTime); //$NON-NLS-1$
        this.statisticsList.add("Node Next Batch Calls: " + this.nodeNextBatchCalls); //$NON-NLS-1$
        this.statisticsList.add("Node Blocks: " + this.nodeBlocks); //$NON-NLS-1$
    }
    
    public List getStatisticsList() {
        return this.statisticsList;
    }
    
    public void reset() {
        this.batchStartTime = 0;
        this.batchEndTime = 0;
    }
    
    /** 
     * @return Returns the nodeBlocks.
     * @since 4.2
     */
    public int getNodeBlocks() {
        return this.nodeBlocks;
    }
    /** 
     * @return Returns the nodeCumulativeNextBatchProcessingTime.
     * @since 4.2
     */
    public long getNodeCumulativeNextBatchProcessingTime() {
        return this.nodeCumulativeNextBatchProcessingTime;
    }
    /** 
     * @return Returns the nodeCumulativeProcessingTime.
     * @since 4.2
     */
    public long getNodeCumulativeProcessingTime() {
        return this.nodeCumulativeProcessingTime;
    }
    /** 
     * @return Returns the nodeEndTime.
     * @since 4.2
     */
    public long getNodeEndTime() {
        return this.nodeEndTime;
    }
    /** 
     * @return Returns the nodeNextBatchCalls.
     * @since 4.2
     */
    public int getNodeNextBatchCalls() {
        return this.nodeNextBatchCalls;
    }
    /** 
     * @return Returns the nodeOutputRows.
     * @since 4.2
     */
    public int getNodeOutputRows() {
        return this.nodeOutputRows;
    }
    /** 
     * @return Returns the nodeProcessingTime.
     * @since 4.2
     */
    public long getNodeProcessingTime() {
        return this.nodeProcessingTime;
    }
    /** 
     * @return Returns the nodeStartTime.
     * @since 4.2
     */
    public long getNodeStartTime() {
        return this.nodeStartTime;
    }
    /** 
     * @return Returns the batchEndTime.
     * @since 4.2
     */
    public long getBatchEndTime() {
        return this.batchEndTime;
    }
    /** 
     * @return Returns the batchStartTime.
     * @since 4.2
     */
    public long getBatchStartTime() {
        return this.batchStartTime;
    }
}
