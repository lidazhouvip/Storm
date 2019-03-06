package com.ldz.bigdata.drpc;

/**
 * @Author: Dazhou Li
 * @Description:部署在服务端
 * @CreateDate: 2019/2/12 0012 08:34
 */
public class UserServiceImpl implements UserService {
    @Override
    public void addUser(String name, int age) {
        System.out.println("From Server Invoked : add User Success,name is "+name);
    }
}
