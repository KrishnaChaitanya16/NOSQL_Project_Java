package com.etl.pipeline;

import java.util.List;

public interface Pipeline {
    void runQ1(int runId, List<String[]> batches) throws Exception;
    void runQ2(int runId, List<String[]> batches) throws Exception;
    void runQ3(int runId, List<String[]> batches) throws Exception;
}