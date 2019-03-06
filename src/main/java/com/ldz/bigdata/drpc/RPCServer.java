package com.ldz.bigdata.drpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;

/**
 * @Author: Dazhou Li
 * @Description:RPC Server端服务
 * @CreateDate: 2019/2/12 0012 08:37
 */
public class RPCServer {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        RPC.Builder builder = new RPC.Builder(configuration);
        RPC.Server server = builder.setProtocol(UserService.class)
                .setInstance(new UserServiceImpl())
                .setBindAddress("localhost")
                .setPort(9999)
                .build();

        server.start();
    }
}
