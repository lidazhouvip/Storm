package com.ldz.bigdata.drpc;

/**
 * @Author: Dazhou Li
 * @Description:
 * @CreateDate: 2019/2/12 0012 08:32
 */
public interface UserService {

    public static final long versionID = 88888888;

    //添加用户
    void addUser(String name, int age);
}
