import com.alibaba.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.druid.sql.ast.statement.SQLTableElement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlCreateTableParser;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * @author: Nan JIAO
 * @create: 2020-11-13 16:59
 **/
public class TestDruid {

    @Test
    public void testDruid() {
        String createTable = "CREATE TABLE `note_00` (\n" +
                "  `note_id` bigint(20) NOT NULL,\n" +
                "  `user_id` bigint(20) NOT NULL,\n" +
                "  `reserved1` bigint(20) unsigned NOT NULL DEFAULT '0' COMMENT '保留',\n" +
                "  `type` tinyint(1) NOT NULL DEFAULT '0' COMMENT '笔记类型: 1-图文 2-视频 3-长笔记',\n" +
                "  `audit_level` tinyint(1) NOT NULL DEFAULT '0' COMMENT '审核状态: -2:仅粉丝可见 -1:自嗨 0:隐藏 1-未审核 2-已关闭 3-推荐(作废) 4-机器关闭',\n" +
                "  `reserved2` tinyint(1) NOT NULL DEFAULT '0' COMMENT '保留',\n" +
                "  `title` varchar(256) DEFAULT NULL COMMENT '标题',\n" +
                "  `content` text GENERATED ALWAYS AS (CONCAT(reserved3, title)) COMMENT '正文',\n" +
                "  `reserved3` text COMMENT '保留',\n" +
                "  `tag_list` json DEFAULT NULL COMMENT '素材以及正文中的TAG列表',\n" +
                "  `at_list` json DEFAULT NULL COMMENT '素材以及正文中@的人列表',\n" +
                "  `metadata` json DEFAULT NULL COMMENT '描述笔记信息的字段',\n" +
                "  `label` json DEFAULT NULL COMMENT '运营/算法为笔记打扩展标记字段',\n" +
                "  `topic` json DEFAULT NULL COMMENT '[topic_id1,...] - 发布时选择参与话题, 目前唯一',\n" +
                "  `geo` json DEFAULT NULL COMMENT '发布时选中的地理位置',\n" +
                "  `flag` bigint(20) unsigned NOT NULL DEFAULT '0',\n" +
                "  `commercial_flag` bigint(20) unsigned NOT NULL DEFAULT '0',\n" +
                "  `activity_flag` bigint(20) unsigned NOT NULL DEFAULT '0',\n" +
                "  `extra_flag` bigint(20) unsigned NOT NULL DEFAULT '0',\n" +
                "  `reserved5` bigint(20) unsigned NOT NULL DEFAULT '0' COMMENT '保留',\n" +
                "  `is_enabled` tinyint(1) NOT NULL DEFAULT '1' COMMENT '是否启用, 笔记被删除标记为0',\n" +
                "  `modify_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '用户修改时间',\n" +
                "  `verify_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '审核时间',\n" +
                "  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n" +
                "  `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,\n" +
                "  PRIMARY KEY (`note_id`),\n" +
                "  KEY `update_time` (`update_time`),\n" +
                "  KEY `idx_create_time` (`create_time`)\n" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='笔记表'";


        List<SQLTableElement> tempList = new MySqlCreateTableParser(createTable)
                .parseCreateTable()
                .getTableElementList();

        List<String> generatedColumn = tempList.stream()
                .filter(ele -> ele instanceof SQLColumnDefinition)
                .map(ele -> (SQLColumnDefinition) ele)
                .filter(ele -> ele.getGeneratedAlawsAs() == null)
                .map(ele -> unwrapBacktick(ele.getColumnName()))
                .collect(Collectors.toList());
        System.out.println(generatedColumn);
    }


    private static final Pattern BACKTICK_PATTERN = Pattern.compile("^`(.*)`$");

    public static String unwrapBacktick(String s) {
        Matcher matcher = BACKTICK_PATTERN.matcher(s);
        if (matcher.find()) {
            return matcher.group(1);
        } else {
            return s;
        }
    }
}
