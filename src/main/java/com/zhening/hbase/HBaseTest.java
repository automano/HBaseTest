package com.zhening.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.*;
import java.util.*;

public class HBaseTest {
    private Configuration conf = null;
    private Connection conn = null;

    @Before
    public void init() throws Exception {
        /*
          初始化Configuration对象
         */
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "emr-worker-2,emr-worker-1,emr-header-1");
        /*
          初始化连接
         */
        Connection conn = ConnectionFactory.createConnection(conf);
    }

    @Test
    public void createTable() throws Exception {
        // 获取表管理器对象
        Admin admin = conn.getAdmin();
        // 创建表的描述对象，并指定表名
        TableName tname = TableName.valueOf("lizhening:student");
        TableDescriptorBuilder tableDescBuilder = TableDescriptorBuilder.newBuilder(tname);
        ColumnFamilyDescriptorBuilder columnDescBuilder = ColumnFamilyDescriptorBuilder
                .newBuilder(Bytes.toBytes(family)).setBlocksize(32 * 1024)
                .setCompressionType(Compression.Algorithm.SNAPPY).setDataBlockEncoding(DataBlockEncoding.NONE);
        tableDescBuilder.setColumnFamily(columnDescBuilder.build());
        tableDescBuilder.build();
    }
}