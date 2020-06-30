package com.xiaohongshu.db.hercules.core.parser;

import com.xiaohongshu.db.hercules.common.parser.CommonParser;
import com.xiaohongshu.db.hercules.mongodb.parser.MongoDBOutputParser;
import com.xiaohongshu.db.hercules.mysql.parser.MysqlInputParser;
import com.xiaohongshu.db.hercules.tidb.parser.TiDBInputParser;
import org.junit.jupiter.api.Test;

public class CmdParserTest {

    @Test
    public void testParseQuote() {
        String[] args = new String[]{"--source-connection", "jdbc:mysql://tidb-growth2-prod-re.int.xiaohongshu.com:4000/kratos?serverTimezone=Asia/Shanghai&characterEncoding=utf8", "--source-user", "root", "--source-password", "tidb!@#", "--source-table", "baidu_sem_search_word_daily", "--source-fetch-size", "10000", "--source-condition", "date >= \"2020-06-01\" and date < \"2020-06-01\"", "--source-balance", "--target-connection", "10.4.12.12:27018", "--target-user", "fulishe", "--target-password", "$uQKq5Nr2M&8", "--target-authdb", "admin", "--target-database", "test_hercules", "--target-collection", "coupon_template", "--target-export-type", "INSERT", "--num-mapper", "8", "--source-balance-sample-max-row", "1000000", "--log-level", "DEBUG"};
        BaseParser parser = new MysqlInputParser();
        System.out.println(parser.parse(args));
        parser=new MongoDBOutputParser();
        System.out.println(parser.parse(args));
        parser=new CommonParser();
        System.out.println(parser.parse(args));
    }

}
