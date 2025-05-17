package org.demir.utils;

import org.apache.flink.api.java.utils.ParameterTool;

public class Params {
    private static ParameterTool params;

    public static ParameterTool getParams() {
            return params;
    }

    public static void setParams(String[] args) {

        Params.params = ParameterTool.fromArgs(args);

        Log.info("Program Arguments:");
        for (String arg : args) {
            Log.info("ARG: " + arg);
        }

    }
}
