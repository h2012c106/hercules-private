package com.xiaohongshu.db.hercules.hbase.option;

import com.xiaohongshu.db.hercules.core.option.SingleOptionConf;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;

import java.util.List;

public class HBaseInputOptionsConf extends HBaseOptionsConf {

    /** Job parameter that specifies the input table. */
    public static final String INPUT_TABLE = "hbase.mapreduce.inputtable";
    /**
     * If specified, use start keys of this table to split.
     * This is useful when you are preparing data for bulkload.
     */
    private static final String SPLIT_TABLE = "hbase.mapreduce.splittable";
    /** Base-64 encoded scanner. All other SCAN_ confs are ignored if this is specified.
     * See {@link TableMapReduceUtil#convertScanToString(Scan)} for more details.
     */
    public static final String SCAN = "hbase.mapreduce.scan";
    /** Scan start row */
    public static final String SCAN_ROW_START = "hbase.mapreduce.scan.row.start";
    /** Scan stop row */
    public static final String SCAN_ROW_STOP = "hbase.mapreduce.scan.row.stop";
    /** Column Family to Scan */
    public static final String SCAN_COLUMN_FAMILY = "hbase.mapreduce.scan.column.family";
    /** Space delimited list of columns and column families to scan. */
    public static final String SCAN_COLUMNS = "hbase.mapreduce.scan.columns";
    /** The timestamp used to filter columns with a specific timestamp. */
    public static final String SCAN_TIMESTAMP = "hbase.mapreduce.scan.timestamp";
    /** The starting timestamp used to filter columns with a specific range of versions. */
    public static final String SCAN_TIMERANGE_START = "hbase.mapreduce.scan.timerange.start";
    /** The ending timestamp used to filter columns with a specific range of versions. */
    public static final String SCAN_TIMERANGE_END = "hbase.mapreduce.scan.timerange.end";
    /** The maximum number of version to return. */
    public static final String SCAN_MAXVERSIONS = "hbase.mapreduce.scan.maxversions";
    /** Set to false to disable server-side caching of blocks for this scan. */
    public static final String SCAN_CACHEBLOCKS = "hbase.mapreduce.scan.cacheblocks";
    /** The number of rows for caching that will be passed to scanners. */
    public static final String SCAN_CACHEDROWS = "hbase.mapreduce.scan.cachedrows";
    /** Set the maximum number of values to return for each call to next(). */
    public static final String SCAN_BATCHSIZE = "hbase.mapreduce.scan.batchsize";
    /** Specify if we have to shuffle the map tasks. */
    public static final String SHUFFLE_MAPS = "hbase.mapreduce.inputtable.shufflemaps";

    /** Specify if we enable auto-balance to set number of mappers in M/R jobs. */
    public static final String MAPREDUCE_INPUT_AUTOBALANCE = "hbase.mapreduce.tif.input.autobalance";
    /** In auto-balance, we split input by ave region size, if calculated region size is too big, we can set it. */
    public static final String MAX_AVERAGE_REGION_SIZE = "hbase.mapreduce.tif.ave.regionsize";

    /** Set the number of Mappers for each region, all regions have same number of Mappers */
    public static final String NUM_MAPPERS_PER_REGION = "hbase.mapreduce.tableinput.mappers.per.region";

    @Override
    protected List<SingleOptionConf> setOptionConf() {
        List<SingleOptionConf> tmpList = super.setOptionConf();
        tmpList.add(SingleOptionConf.builder()
                .name(INPUT_TABLE)
                .needArg(true)
                .necessary(true)
                .description("Job parameter that specifies the input table.")
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(SPLIT_TABLE)
                .needArg(true)
                .description("This is useful when you are preparing data for bulkload.")
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(SCAN)
                .needArg(true)
                .description("Base-64 encoded scanner. All other SCAN_ confs are ignored if this is specified.")
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(SCAN_ROW_START)
                .needArg(true)
                .description("Scan start row.")
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(SCAN_ROW_STOP)
                .needArg(true)
                .description("Scan stop row.")
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(SCAN_COLUMN_FAMILY)
                .needArg(true)
                .necessary(true)
                .description("Column Family to Scan.")
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(SCAN_COLUMNS)
                .needArg(true)
                .listDelimiter(COLUMN_DELIMITER)
                .description("Space delimited list of columns and column families to scan.")
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
                .name(SCAN_MAXVERSIONS)
                .needArg(true)
                .description("The maximum number of version to return..")
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(SCAN_CACHEBLOCKS)
                .needArg(true)
                .description("Set to false to disable server-side caching of blocks for this scan.")
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
                .name(SHUFFLE_MAPS)
                .needArg(true)
                .description("Specify if we have to shuffle the map tasks.")
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(MAPREDUCE_INPUT_AUTOBALANCE)
                .needArg(true)
                .description("Specify if we enable auto-balance to set number of mappers in M/R jobs.")
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(MAX_AVERAGE_REGION_SIZE)
                .needArg(true)
                .description("In auto-balance, we split input by ave region size, " +
                        "if calculated region size is too big, we can set it.")
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(NUM_MAPPERS_PER_REGION)
                .needArg(true)
                .description("Specify if we have to shuffle the map tasks.")
                .build());
        return tmpList;
    }
}
