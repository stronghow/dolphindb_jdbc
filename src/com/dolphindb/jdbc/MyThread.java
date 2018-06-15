package com.dolphindb.jdbc;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class MyThread extends Thread {

    private int start;
    private int end;
    private PreparedStatement statement;
    public MyThread(PreparedStatement statement, int start, int end){
        this.statement = statement;
        this.start = start;
        this.end = end;
    }

    @Override
    public void run() {
        try {
            //System.out.println(System.currentTimeMillis());
            for(int i = start; i<=end; ++i){
                statement.setObject(1,97);
                statement.setObject(2,97);
                statement.setObject(3,97);
                statement.setObject(4,97);
                statement.setObject(5,97);
                statement.execute();
            }
            //System.out.println(System.currentTimeMillis());
        }catch (SQLException e){}
    }
}
