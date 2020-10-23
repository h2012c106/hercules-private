package com.xiaohongshu.db.hercules.elasticsearch.mr;

import com.xiaohongshu.db.hercules.core.datasource.DataSourceRole;
import com.xiaohongshu.db.hercules.core.mr.output.wrapper.BaseTypeWrapperSetter;
import com.xiaohongshu.db.hercules.core.mr.output.wrapper.WrapperSetterFactory;
import org.elasticsearch.action.bulk.BulkRequest;

public class ElasticsearchOutputWrapperManager extends WrapperSetterFactory<BulkRequest> {

    public ElasticsearchOutputWrapperManager() {
        super(DataSourceRole.TARGET);
    }

    @Override
    protected BaseTypeWrapperSetter.ByteSetter<BulkRequest> getByteSetter() {
        return null;
    }

    @Override
    protected BaseTypeWrapperSetter.ShortSetter<BulkRequest> getShortSetter() {
        return null;
    }

    @Override
    protected BaseTypeWrapperSetter.IntegerSetter<BulkRequest> getIntegerSetter() {
        return null;
    }

    @Override
    protected BaseTypeWrapperSetter.LongSetter<BulkRequest> getLongSetter() {
        return null;
    }

    @Override
    protected BaseTypeWrapperSetter.LonglongSetter<BulkRequest> getLonglongSetter() {
        return null;
    }

    @Override
    protected BaseTypeWrapperSetter.FloatSetter<BulkRequest> getFloatSetter() {
        return null;
    }

    @Override
    protected BaseTypeWrapperSetter.DoubleSetter<BulkRequest> getDoubleSetter() {
        return null;
    }

    @Override
    protected BaseTypeWrapperSetter.DecimalSetter<BulkRequest> getDecimalSetter() {
        return null;
    }

    @Override
    protected BaseTypeWrapperSetter.BooleanSetter<BulkRequest> getBooleanSetter() {
        return null;
    }

    @Override
    protected BaseTypeWrapperSetter.StringSetter<BulkRequest> getStringSetter() {
        return null;
    }

    @Override
    protected BaseTypeWrapperSetter.DateSetter<BulkRequest> getDateSetter() {
        return null;
    }

    @Override
    protected BaseTypeWrapperSetter.TimeSetter<BulkRequest> getTimeSetter() {
        return null;
    }

    @Override
    protected BaseTypeWrapperSetter.DatetimeSetter<BulkRequest> getDatetimeSetter() {
        return null;
    }

    @Override
    protected BaseTypeWrapperSetter.BytesSetter<BulkRequest> getBytesSetter() {
        return null;
    }

    @Override
    protected BaseTypeWrapperSetter.NullSetter<BulkRequest> getNullSetter() {
        return null;
    }
}
