package org.teiid.systemmodel;

import org.jgroups.JChannel;
import org.jgroups.blocks.RequestOptions;
import org.jgroups.blocks.ResponseMode;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.util.RspList;
import org.jgroups.util.Util;
import org.teiid.core.util.UnitTestUtil;

public class RpcDispatcherTest {

    public static int print(int number) throws Exception {
        return number * 2;
    }

    public static int print2(int number) throws Exception {
        return number * 20;
    }
    
    public void start(String name, String configFile) throws Exception {
        final RequestOptions opts=new RequestOptions(ResponseMode.GET_ALL, 5000);
        
        JChannel channel1=new JChannel(UnitTestUtil.getTestDataFile(configFile));
        channel1.setDiscardOwnMessages(true);
        channel1.name(name+" foo");
        channel1.connect("cluster-1");
        
        JChannel channel2=new JChannel(UnitTestUtil.getTestDataFile(configFile));
        channel2.setDiscardOwnMessages(true);
        channel2.name(name+" bar");
        channel2.connect("cluster-2");
        
        final RpcDispatcher rpc1 = new RpcDispatcher(channel1, this);
        final RpcDispatcher rpc2 = new RpcDispatcher(channel2, this);
        
        Work t1 = new Work(rpc1, opts, "print");
        t1.start();
        
        Work t2 = new Work(rpc2, opts, "print2");
        t2.start();
        
        t1.join();
        t2.join();        

        channel1.close();
        channel2.close();
    }
    
    public class Work extends Thread {
        RpcDispatcher rpc; 
        RequestOptions opts;
        String methodName;
        
        public Work(RpcDispatcher rpc, RequestOptions opts, String methodName) {
            this.rpc = rpc;
            this.opts = opts;
            this.methodName = methodName;
        }
        
        @Override
        public void run() {
            for(int i=0; i < 10; i++) {
                Util.sleep(1000);
                try {
                    RspList rsp_list=rpc.callRemoteMethods(null,
                                                    methodName,
                                                    new Object[]{i},
                                                    new Class[]{int.class},
                                                    opts);
                    System.out.println("Responses: " + rsp_list);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            this.rpc.stop();
        }        
    }

    public static void main(String[] args) throws Exception {
        System.setProperty("jgroups.bind_addr", "127.0.0.1");
        new RpcDispatcherTest().start(args[0], args[1]);
    }
}