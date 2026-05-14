package com.etl.controller;

import com.etl.db.ResultLoader;
import com.etl.pipeline.MapReducePipeline;
// import com.etl.pipeline.MongoPipeline;
import com.etl.pipeline.HivePipeline;
import com.etl.pipeline.PigPipeline;
import com.etl.pipeline.Pipeline;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class Main {

    public static void main(String[] args) throws Exception {

        Scanner scanner = new Scanner(System.in);

        System.out.println("==========================================");
        System.out.println("  NASA Logs ETL Pipeline");
        System.out.println("==========================================");

        System.out.println("\nSelect Pipeline Engine:");
        System.out.println("  1. MapReduce");
        System.out.println("  2. Pig");
        System.out.println("  3. MongoDB");
        System.out.println("  4. Hive");
        System.out.print("Enter choice (1, 2, 3, or 4): ");
        String engineChoice = scanner.nextLine().trim();

        Pipeline pipeline = null;
        String pipelineName = "unknown";
        if (engineChoice.equals("1") || engineChoice.equalsIgnoreCase("mapreduce")) {
            pipeline = new MapReducePipeline();
            pipelineName = "mapreduce";
        } else if (engineChoice.equals("2") || engineChoice.equalsIgnoreCase("pig")) {
            pipeline = new PigPipeline();
            pipelineName = "pig";
        } else if (engineChoice.equals("3") || engineChoice.equalsIgnoreCase("mongodb")) {
            System.out.println("Mongo pipeline is temporarily disabled to allow compilation without Mongo driver. Exiting.");
            System.exit(0);
            // pipeline = new MongoPipeline();
            // pipelineName = "mongo";
        } else if (engineChoice.equals("4") || engineChoice.equalsIgnoreCase("hive")) {
            pipeline = new HivePipeline();
            pipelineName = "hive";
        } else {
            System.out.println("Invalid engine selected. Exiting.");
            System.exit(1);
        }

        System.out.println("\nSelect Dataset (Batches):");
        System.out.println("  1. July 1995 Only");
        System.out.println("  2. August 1995 Only");
        System.out.println("  3. Both (July + August sequentially)");
        System.out.print("Enter choice (1, 2, or 3): ");
        String batchChoice = scanner.nextLine().trim();

        List<String[]> batches = new ArrayList<>();
        if (batchChoice.equals("1")) {
            batches.add(new String[]{"/etl/input/NASA_access_log_Jul95", "July 1995"});
        } else if (batchChoice.equals("2")) {
            batches.add(new String[]{"/etl/input/NASA_access_log_Aug95", "August 1995"});
        } else if (batchChoice.equals("3")) {
            batches.add(new String[]{"/etl/input/NASA_access_log_Jul95", "July 1995"});
            batches.add(new String[]{"/etl/input/NASA_access_log_Aug95", "August 1995"});
        } else {
            System.out.println("Invalid batch selection. Exiting.");
            System.exit(1);
        }

        System.out.println("\nSelect Query to Run:");
        System.out.println("  1. Q1 - Daily request count & total bytes");
        System.out.println("  2. Q2 - Top 20 most-requested resources");
        System.out.println("  3. Q3 - Error rate per hour");
        System.out.println("  4. All Queries (Q1, Q2, and Q3 sequentially)");
        System.out.print("Enter choice (1, 2, 3, or 4): ");
        String queryChoice = scanner.nextLine().trim();

        System.out.println("\n==========================================");

        // Generate a new Run ID
        int runId = ResultLoader.createRun(pipelineName);

        // -------- Execute Query --------
        switch (queryChoice) {
            case "1":
            case "Q1":
            case "q1":
                pipeline.runQ1(runId, batches);
                break;

            case "2":
            case "Q2":
            case "q2":
                pipeline.runQ2(runId, batches);
                break;

            case "3":
            case "Q3":
            case "q3":
                pipeline.runQ3(runId, batches);
                break;

            case "4":
            case "ALL":
            case "all":
                System.out.println("Running ALL queries sequentially...");
                pipeline.runQ1(runId, batches);
                pipeline.runQ2(runId, batches);
                pipeline.runQ3(runId, batches);
                break;

            default:
                System.out.println("Invalid query selected. Exiting.");
        }

        scanner.close();
        System.out.println("==========================================");
        System.out.println("Pipeline Execution Completed.");
    }
}