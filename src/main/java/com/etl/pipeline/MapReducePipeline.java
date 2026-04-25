
package com.etl.pipeline;
import com.etl.db.ResultLoader;
import java.io.BufferedReader;
import java.io.InputStreamReader;

public class MapReducePipeline implements Pipeline {

    @Override
public void runQ1() throws Exception {

    System.out.println("Running MapReduce Q1...");

    long start = System.currentTimeMillis();

    // delete old output
    Runtime.getRuntime().exec("hdfs dfs -rm -r /etl/output/mapreduce/q1");

    // 🔥 Run MapReduce properly
    ProcessBuilder pb = new ProcessBuilder(
            "hadoop", "jar",
            "scripts/mapreduce/q1.jar",
            "Q1Driver",
            "/etl/input",
            "/etl/output/mapreduce/q1"
    );

    pb.redirectErrorStream(true);
    Process p = pb.start();

    // 🔥 print logs
    BufferedReader reader = new BufferedReader(
            new InputStreamReader(p.getInputStream())
    );

    String line;
    while ((line = reader.readLine()) != null) {
        System.out.println(line);
    }

    int exitCode = p.waitFor();
    System.out.println("MapReduce Job Exit Code: " + exitCode);

    long end = System.currentTimeMillis();
    long runtime = end - start;
    System.out.println("Runtime calculated: " + runtime);

    // load into DB
    ResultLoader.loadQ1(runtime);

    System.out.println("MapReduce Q1 completed.");
}
    
    @Override
public void runQ2() throws Exception {

    System.out.println("Running MapReduce Q2...");

    long start = System.currentTimeMillis();

    // -------- Delete old output --------
    Runtime.getRuntime().exec("hdfs dfs -rm -r /etl/output/mapreduce/q2");

    // -------- Run MapReduce properly --------
    ProcessBuilder pb = new ProcessBuilder(
            "hadoop", "jar",
            "scripts/mapreduce/q2.jar",
            "Q2Driver",
            "/etl/input",
            "/etl/output/mapreduce/q2"
    );

    pb.redirectErrorStream(true);
    Process p = pb.start();

    // -------- Print logs --------
    BufferedReader reader = new BufferedReader(
            new InputStreamReader(p.getInputStream())
    );

    String line;
    while ((line = reader.readLine()) != null) {
        System.out.println(line);
    }

    int exitCode = p.waitFor();
    System.out.println("MapReduce Job Exit Code: " + exitCode);

    if (exitCode != 0) {
        throw new RuntimeException("Q2 MapReduce job failed!");
    }

    long end = System.currentTimeMillis();
    long runtime = end - start;

    System.out.println("Runtime calculated: " + runtime);

    // -------- Load into DB --------
    ResultLoader.loadQ2(runtime);

    System.out.println("MapReduce Q2 completed.");
}

    @Override
    public void runQ3() throws Exception {
        System.out.println("Q3 not implemented yet.");
    }
}