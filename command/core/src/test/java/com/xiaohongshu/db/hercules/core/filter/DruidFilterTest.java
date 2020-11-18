package com.xiaohongshu.db.hercules.core.filter;

import com.xiaohongshu.db.hercules.core.filter.expr.Expr;
import com.xiaohongshu.db.hercules.core.filter.parser.DruidParser;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.OptionsType;
import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import com.xiaohongshu.db.hercules.core.serialize.entity.ExtendedDate;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.DateWrapper;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.IntegerWrapper;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.MapWrapper;
import com.xiaohongshu.db.hercules.core.utils.DateUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DruidFilterTest {

    @Test
    public void testFilter() {
        DateUtils.setFormats(new GenericOptions(OptionsType.SOURCE), new GenericOptions(OptionsType.TARGET));
        DruidParser parser = new DruidParser();
        MapWrapper row = new MapWrapper(2);
        row.put("id", IntegerWrapper.get(1000));
        row.put("date", DateWrapper.getDate(ExtendedDate.initialize("2020-09-01")));
        HerculesWritable herculesWritable = new HerculesWritable(row);
        Expr expr = parser.parse("id > 0 and date >= kast('2020-08-24', 'date') and id = 1000 and id > 0 and (1 > 0 or x in (2,3,4,5)) and 1 > -1");
        Assertions.assertEquals(expr.getResult(herculesWritable).asBoolean(), true);
    }

}
