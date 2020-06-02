package com.xiaohongshu.db.hercules.hbase.option;

import com.google.common.collect.Lists;
import com.xiaohongshu.db.hercules.core.option.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.xiaohongshu.db.hercules.core.option.BaseDataSourceOptionsConf.COLUMN_DELIMITER;

public class HBaseInputOptionsConf extends BaseOptionsConf {

    /** Scan start row */
    public static final String SCAN_ROW_START = "hbase.mapreduce.scan.row.start";
    /** Scan stop row */
    public static final String SCAN_ROW_STOP = "hbase.mapreduce.scan.row.stop";
    /** Column Family to Scan */
    public static final String SCAN_COLUMN_FAMILY = "hbase.mapreduce.scan.column.family";
    /** The timestamp used to filter columns with a specific timestamp. */
    public static final String SCAN_TIMESTAMP = "hbase.mapreduce.scan.timestamp";
    /** The starting timestamp used to filter columns with a specific range of versions. */
    public static final String SCAN_TIMERANGE_START = "hbase.mapreduce.scan.timerange.start";
    /** The ending timestamp used to filter columns with a specific range of versions. */
    public static final String SCAN_TIMERANGE_END = "hbase.mapreduce.scan.timerange.end";
    /** The number of rows for caching that will be passed to scanners. */
    public static final String SCAN_CACHEDROWS = "hbase.mapreduce.scan.cachedrows";
    /** Set the maximum number of values to return for each call to next(). */
    public static final String SCAN_BATCHSIZE = "hbase.mapreduce.scan.batchsize";

    @Override
    protected List<BaseOptionsConf> generateAncestorList() {
        return Lists.newArrayList(
                new BaseInputOptionsConf(),
                new HBaseOptionsConf()
        );
    }

    @Override
    protected List<SingleOptionConf> innerGenerateOptionConf() {
        List<SingleOptionConf> tmpList = new ArrayList<>();
        tmpList.add(SingleOptionConf.builder()
                .name(SCAN_ROW_START)
                .needArg(true)
                .description("Scan start row, the start row of the table will be taken if it is not explicitly given.")
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(SCAN_ROW_STOP)
                .needArg(true)
                .description("Scan stop row, the stop row of the table will be taken if it is not explicitly given.")
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(SCAN_COLUMN_FAMILY)
                .needArg(true)
                .necessary(true)
                .description("Column Family to Scan.")
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(SCAN_TIMESTAMP)
                .needArg(true)
                .description(" The timestamp used to filter columns with a specific timestamp.")
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(SCAN_TIMERANGE_START)
                .needArg(true)
                .description("The starting timestamp used to filter columns with a specific range of versions.")
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(SCAN_TIMERANGE_END)
                .needArg(true)
                .description("The ending timestamp used to filter columns with a specific range of versions.")
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(SCAN_CACHEDROWS)
                .needArg(true)
                .description("The number of rows for caching that will be passed to scanners.")
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(SCAN_BATCHSIZE)
                .needArg(true)
                .description("Set the maximum number of values to return for each call to next().")
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(HBaseOptionsConf.ROW_KEY_COL_NAME)
                .needArg(true)
                .description("Specify the name of the row key col passed to the recordWriter.")
                .build());
        return tmpList;
    }

    @Override
    public void innerValidateOptions(GenericOptions options) {
        String rowKeyCol = options.getString(HBaseOptionsConf.ROW_KEY_COL_NAME, null);
        if(rowKeyCol!=null){
            List<String> columnNameList = Arrays.asList(options.getStringArray(BaseDataSourceOptionsConf.COLUMN, null));
            if(columnNameList.size()>0&&!columnNameList.contains(rowKeyCol)){
                throw new RuntimeException("Missing row key col in column name list: "+columnNameList);
            }
        }
    }

}
