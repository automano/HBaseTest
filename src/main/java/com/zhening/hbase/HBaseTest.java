package com.zhening.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Zhening Li
 */
public class HBaseTest {

    public static void main(String[] args) throws IOException {
        // 1. 建立连接
        //获取Configuration对象
        Configuration configuration = HBaseConfiguration.create();
        //对hbase客户端来说，只需知道hbase所经过的Zookeeper集群地址即可
        //因为hbase的客户端找hbase读写数据完全不用经过HMaster
        configuration.set("hbase.zookeeper.quorum", "emr-worker-2");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        //获取连接
        Connection conn = ConnectionFactory.createConnection(configuration);

        // 2. 建表
        //获取表管理器对象
        Admin admin = conn.getAdmin();
        // 定义表名和列族名
        TableName tableName = TableName.valueOf("lizhening:student");
        String colFamily1 = "info";
        String colFamily2 = "score";

        if (admin.tableExists(tableName)) {
            System.out.println("Table already exists");
        } else {
            //创建表的描述对象，并指定表名
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
            //构造第一个列族描述对象，并指定列族名
            HColumnDescriptor hcd1 = new HColumnDescriptor(colFamily1);
            //构造第二个列族描述对象，并指定列族名
            HColumnDescriptor hcd2 = new HColumnDescriptor(colFamily2);
            //将列族描述对象添加到表描述对象中
            hTableDescriptor.addFamily(hcd1).addFamily(hcd2);
            //利用表管理器来创建表
            admin.createTable(hTableDescriptor);
            System.out.println("Table created successfully.");
        }


        // 3. 插入数据
        //创建table对象，通过table对象来添加数据
        Table table = conn.getTable(tableName);
        //创建一个集合，用于存放Put对象
        ArrayList<Put> puts = new ArrayList<Put>();
        //构建put对象（KV形式），并指定其行键
        Put put1 = assembleData("Tom","20210000000001","1","75","82");
        Put put2 = assembleData("Jerry","20210000000002","1","85","67");
        Put put3 = assembleData("Jack","20210000000003","2","80","80");
        Put put4 = assembleData("Rose","20210000000004","2","60","61");
        Put put5 = assembleData("lizhening","G20210607020319","1","100","100");
        //把所有的put对象添加到一个集合中
        puts.add(put1);
        puts.add(put2);
        puts.add(put3);
        puts.add(put4);
        puts.add(put5);
        //提交所有的插入数据的记录
        table.put(puts);
        System.out.println("All data insert successfully.");

        // 4.查看数据
        // 创建get查询参数对象，指定要获取的是哪一行
        Get get = new Get(Bytes.toBytes("lizhening"));
        if (!get.isCheckExistenceOnly()) {
            //返回查询结果的数据
            Result result = conn.getTable(tableName).get(get);
            //遍历所有的cell
            for (Cell cell : result.rawCells()) {
                String colName = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                System.out.println("Data get success, colName: " + colName + ", value: " + value);
            }
        }

        // 5.扫描数据
        //创建scan对象
        Scan scan = new Scan();
        //获取查询的数据
        ResultScanner scanner = table.getScanner(scan);
        //获取ResultScanner所有数据，返回迭代器
        //遍历迭代器
        for (Result result : scanner) {
            //获取当前每一行结果数据
            //获取当前每一行中所有的cell对象
            List<Cell> cells = result.listCells();
            //迭代所有的cell
            for (Cell c : cells) {
                //获取行键
                byte[] rowArray = c.getRowArray();
                //获取列族
                byte[] familyArray = c.getFamilyArray();
                //获取列族下的列名称
                byte[] qualifierArray = c.getQualifierArray();
                //列字段的值
                byte[] valueArray = c.getValueArray();
                //打印rowArray、familyArray、qualifierArray、valueArray
                System.out.println("行键:" + new String(rowArray, c.getRowOffset(), c.getRowLength()));
                System.out.print("列族:" + new String(familyArray, c.getFamilyOffset(), c.getFamilyLength()));
                System.out.print(" " + "列:" + new String(qualifierArray, c.getQualifierOffset(), c.getQualifierLength()));
                System.out.println(" " + "值:" + new String(valueArray, c.getValueOffset(), c.getValueLength()));
            }
            System.out.println("-----------------------");
        }

        // 6.删除数据
        // 获取delete对象,需要一个 Row Key
        Delete delete = new Delete(Bytes.toBytes("Tom"));
        table.delete(delete);
        System.out.println("Delete Successfully");

        // 7.删除表
        if (admin.tableExists(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            System.out.println("Table Delete Successful");
        } else {
            System.out.println("Table does not exist!");
        }

        // 关闭连接，释放资源
        table.close();
        conn.close();
    }

    public static Put assembleData(String rowKey, String val1, String val2, String val3, String val4){
        //构建put对象（KV形式），并指定其行键
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("student_id"), Bytes.toBytes(val1));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("class"), Bytes.toBytes(val2));
        put.addColumn(Bytes.toBytes("score"), Bytes.toBytes("understanding"), Bytes.toBytes(val3));
        put.addColumn(Bytes.toBytes("score"), Bytes.toBytes("programming"), Bytes.toBytes(val4));
        return put;
    }
}
