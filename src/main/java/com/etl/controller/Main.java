package com.etl.controller;

import com.etl.pipeline.MapReducePipeline;
import com.etl.pipeline.PigPipeline;
import com.etl.pipeline.Pipeline;

public class Main {

    public static void main(String[] args) throws Exception {

        if (args.length < 2) {
            System.out.println("Usage: java com.etl.controller.Main <pipeline> <query>");
            System.out.println("Pipelines : mapreduce | pig");
            System.out.println("Queries   : Q1 | Q2 | Q3");
            System.out.println("Example   : java com.etl.controller.Main pig Q1");
            return;
        }

        String pipelineType = args[0];
        String query = args[1];

        Pipeline pipeline = null;

        // -------- Select Pipeline --------
        if (pipelineType.equalsIgnoreCase("mapreduce")) {
            pipeline = new MapReducePipeline();
        } else if (pipelineType.equalsIgnoreCase("pig")) {
            pipeline = new PigPipeline();
        }
        // future: else if (pipelineType.equalsIgnoreCase("hive")) { ... }

        if (pipeline == null) {
            System.out.println("Invalid pipeline.");
            return;
        }

        // -------- Execute Query --------
        switch (query.toUpperCase()) {
            case "Q1":
                pipeline.runQ1();
                break;

            case "Q2":
                pipeline.runQ2();
                break;

            case "Q3":
                pipeline.runQ3();
                break;

            default:
                System.out.println("Invalid query.");
        }
    }
}