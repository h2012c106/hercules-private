package com.xiaohongshu.db.hercules.hbase;

import com.xiaohongshu.db.hercules.hbase.option.HBaseOutputOptionsConf;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class GenerateTestData {


    /**
     * Only for testing purpose, will be deleted.
     */
//    public void InsertTestDataToHBaseTable() throws IOException {
//        Connection conn = getConnection();
//        long writeBufferSize = conf.getLong(HBaseOutputOptionsConf.WRITE_BUFFER_SIZE, HBaseOutputOptionsConf.DEFAULT_WRITE_BUFFER_SIZE);
//        BufferedMutator bufferedMutator = conn.getBufferedMutator(
//                new BufferedMutatorParams(TableName.valueOf("hercules_test_table"))
//                        .pool(HTable.getDefaultExecutor(conf))
//                        .writeBufferSize(writeBufferSize));
//        int INSERT_COUNT = 200000;
//        List<String> keys = Arrays.asList("01","02","03","04","05","06","07","08","09","10",
//                "11","12","13","14","15","16","17","18","19","20","21","22","23","24","25","26");
//        Random rand = new Random();
//        Put put;
//        Integer c1 = 1232;
//        Short c2 = 15;
//        Long c3 = 123L;
//        Double c4 = 3.1415926;
//        Float c5 = 2.14153f;
//        BigDecimal c6 = new BigDecimal(2.321312);
//        String c7 = "This is a short string";
//        String c8 = "This is a long string: tringBuilder rowkey = new StringBuilder(keys.get(rand.nexttringBuilder rowkey =" +
//                " new StringBuilder(keys.get(rand.nexttringBuilder rowkey = new StringBuilder(keys.get" +
//                "(rand.nexttringBuilder rowkey = new StringBuilder(keys.get(rand.next";
//        byte[] c9 = c7.getBytes();
//        boolean c10 = false;
//
//        for(int i=0;i<=INSERT_COUNT;i++){
//            if(i%50000==0){
//                LOG.info("Insert into table hercules_test_table, progress: "+i);
//            }
//            StringBuilder rowkey = new StringBuilder(keys.get(rand.nextInt(keys.size())));
//            rowkey.append(System.currentTimeMillis());
//            rowkey.append(i);
//            put = new Put(Bytes.toBytes(rowkey.toString()));
//            byte[] family = Bytes.toBytes("cf");
//            put.addColumn(family,Bytes.toBytes("c1"),Bytes.toBytes(c1));
//            put.addColumn(family,Bytes.toBytes("c2"),Bytes.toBytes(c2));
//            put.addColumn(family,Bytes.toBytes("c3"),Bytes.toBytes(c3));
//            put.addColumn(family,Bytes.toBytes("c4"),Bytes.toBytes(c4));
//            put.addColumn(family,Bytes.toBytes("c5"),Bytes.toBytes(c5));
//            put.addColumn(family,Bytes.toBytes("c6"),Bytes.toBytes(c6));
//            put.addColumn(family,Bytes.toBytes("c7"),Bytes.toBytes(c7));
//            put.addColumn(family,Bytes.toBytes("c8"),Bytes.toBytes(c8));
//            put.addColumn(family,Bytes.toBytes("c9"),c9);
//            put.addColumn(family,Bytes.toBytes("c10"),Bytes.toBytes(c10));
//            bufferedMutator.mutate(put);
//        }
//        bufferedMutator.flush();
//        bufferedMutator.close();
//    }
}
