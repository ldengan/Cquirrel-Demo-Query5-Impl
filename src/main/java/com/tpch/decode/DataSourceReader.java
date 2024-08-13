package com.tpch.decode;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.BufferedReader;
import java.io.FileReader;

public class DataSourceReader implements SourceFunction<String> {
    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        String testPath = "/Users/ldengan/Desktop/ip/Cquirrel-Demo-Query5-Impl/data/running/input_data_sample_600000.csv";
        String runningPath = "/Users/ldengan/Desktop/ip/Cquirrel-Demo-Query5-Impl/data/running/input_data_sample_600000.csv";
        System.out.println("Datasource running path:" + runningPath);
        try (BufferedReader reader = new BufferedReader(new FileReader(runningPath))) {
            String line = reader.readLine();
            while (line != null) {
                sourceContext.collect(line);
                line = reader.readLine();
            }
        }
    }

    @Override
    public void cancel() {

    }
}