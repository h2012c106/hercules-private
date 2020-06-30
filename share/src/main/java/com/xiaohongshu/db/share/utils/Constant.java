package com.xiaohongshu.db.share.utils;

public final class Constant {

    public static final String ID_COL_NAME = "id";

    public static final String D_PARAM_MAP_KEY = "-D";
    public static final String SOURCE_PARAM_MAP_KEY = "source";
    public static final String TARGET_PARAM_MAP_KEY = "target";
    public static final String COMMON_PARAM_MAP_KEY = "common";

    public static String nodeRestBaseUrl(String nodeHost, long nodePort) {
        return String.format("http://%s:%d/rest/node", nodeHost, nodePort);
    }

    public static String managerRestBaseUrl(String managerHost, long managerPort) {
        return String.format("http://%s:%d/rest/manager", managerHost, managerPort);
    }

}
