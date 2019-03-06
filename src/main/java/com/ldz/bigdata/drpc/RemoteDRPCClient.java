package com.ldz.bigdata.drpc;

import org.apache.storm.Config;
import org.apache.storm.utils.DRPCClient;

/**
 * @Author: Dazhou Li
 * @Description:远程DRPC客户端测试类
 * @CreateDate: 2019/2/13 0013 13:30
 */
public class RemoteDRPCClient {
    public static void main(String[] args) throws Exception{
        Config config = new Config();
        config.put("storm.thrift.transport","org.apache.storm.securty.auth.SimpleTransforation");
        config.put(Config.STORM_NIMBUS_RETRY_TIMES,3);
        config.put(Config.STORM_NIMBUS_RETRY_INTERVAL,10);
        config.put(Config.STORM_NIMBUS_RETRY_INTERVAL_CEILING,20);
        config.put(Config.DRPC_MAX_BUFFER_SIZE,1048576);

        DRPCClient client = new DRPCClient(config, "hadoop000", 3772);
        String result = client.execute("addUser", "zhaoliu");
        System.out.println("Client invoked "+result);
    }
}
