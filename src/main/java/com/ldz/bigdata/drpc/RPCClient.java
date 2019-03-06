package com.ldz.bigdata.drpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * @Author: Dazhou Li
 * RPC 客户端
 * @CreateDate: 2019/2/12 0012 08:45
 */
public class RPCClient {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        long clientVersion = 88888888;
        //此时本地已经拿到远程的服务了
        UserService userService = RPC.getProxy(UserService.class, clientVersion,
                new InetSocketAddress("localhost", 9999),
                configuration);

        userService.addUser("lisi",40);
        System.out.println("From client...invoked");

        RPC.stopProxy(userService);
    }
}
