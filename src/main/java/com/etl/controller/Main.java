package com.etl.controller;

import com.etl.pipeline.MapReducePipeline;
import com.etl.pipeline.MongoPipeline;
import com.etl.pipeline.PigPipeline;
import com.etl.pipeline.Pipeline;

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
        System.out.print("Enter choice (1, 2, or 3): ");
        String engineChoice = scanner.nextLine().trim();
        
        Pipeline pipeline = null;
        if (engineChoice.equals("1") || engineChoice.equalsIgnoreCase("mapreduce")) {
            pipeline = new MapReducePipeline();
        } else if (engineChoice.equals("2") || engineChoice.equalsIgnoreCase("pig")) {
            pipeline = new PigPipeline();
        } else if (engineChoice.equals("3") || engineChoice.equalsIgnoreCase("mongodb")) {
            pipeline = new MongoPipeline();
        } else {
            System.out.println("Invalid engine selected. Exiting.");
            System.exit(1);
        }

        System.out.println("\nSelect Query to Run:");
        System.out.println("  1. Q1 - Daily request count & total bytes");
        System.out.println("  2. Q2 - Top 20 most-requested resources");
        System.out.println("  3. Q3 - Error rate per hour");
        System.out.print("Enter choice (1, 2, or 3): ");
        String queryChoice = scanner.nextLine().trim();

        System.out.println("\n==========================================");
        
        // -------- Execute Query --------
        switch (queryChoice) {
            case "1":
            case "Q1":
            case "q1":
                pipeline.runQ1();
                break;

            case "2":
            case "Q2":
            case "q2":
                pipeline.runQ2();
                break;

            case "3":
            case "Q3":
            case "q3":
                pipeline.runQ3();
                break;

            default:
                System.out.println("Invalid query selected. Exiting.");
        }
        
        scanner.close();
    }
}