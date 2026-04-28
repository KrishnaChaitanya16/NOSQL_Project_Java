# NASA Logs Dual-Engine ETL Pipeline

A highly scalable ETL (Extract, Transform, Load) pipeline utilizing a dual-engine processing architecture: **Apache Hadoop MapReduce** and **Apache Pig**. Both engines process massive NASA HTTP access logs (3.46 million records) to derive analytical insights regarding traffic, most-requested resources, and system error rates.

The entire ecosystem is orchestrated by a unified interactive **Java Controller** that reads raw logs from HDFS, batches them into 500,000-line chunks, processes them, and loads the output metrics directly into a PostgreSQL database.

---

## Prerequisites
Ensure you have the following installed and running:
* **Java 11+**
* **Hadoop 3.x** (HDFS & MapReduce)
* **Apache Pig 0.17+**
* **PostgreSQL** 

---

## 🛠️ Compilation Instructions

Whenever you make changes to the source code, you must recompile the MapReduce JAR files and the Java Controller.

### 1. Compile MapReduce Engine JARs
Run these commands from the root directory to compile the Java MapReduce jobs and package them into `.jar` files:

```bash
# Compile the mapper/reducer/driver classes
javac -cp ".:lib/postgresql-42.7.3.jar:$(hadoop classpath)" scripts/mapreduce/*.java -d scripts/mapreduce/

# Package them into execution JARs
cd scripts/mapreduce
jar uf q1.jar Q1Mapper.class Q1Reducer.class Q1Driver.class
jar uf q2.jar Q2Mapper.class Q2Reducer.class Q2Driver.class Q2MergeDriver.class Q2MergeMapper.class Q2MergeReducer.class
jar uf q3.jar Q3Mapper.class Q3Reducer.class Q3Driver.class
cd ../..
```

### 2. Compile the Java Controller
Run this command to compile the main orchestration system (including `Main.java`, `BatchSplitter.java`, and `ResultLoader.java`):

```bash
javac -cp ".:lib/postgresql-42.7.3.jar:$(hadoop classpath)" $(find src -name "*.java") -d .
```

---

## Running the Pipeline

The project features a fully interactive Command Line Interface (CLI) menu.

### Important: Clear Previous Data
Because the pipeline loads data into PostgreSQL incrementally, you **must** clear the database before running a fresh query to prevent duplicate records:
```bash
psql -U postgres -d postgres -c "TRUNCATE TABLE results;"
```

### Start the Pipeline
Launch the interactive orchestrator using this command:
```bash
java -cp ".:lib/postgresql-42.7.3.jar:$(hadoop classpath)" com.etl.controller.Main
```

You will be presented with an interactive menu to select your Big Data Engine (MapReduce or Pig) and your desired Query (Q1, Q2, or Q3).

---

## Available Queries

* **Query 1 (Daily Request Count & Total Bytes):** Calculates total requests and bytes transferred per day, grouped by HTTP status code.
* **Query 2 (Top 20 Most-Requested Resources):** A complex **two-stage pipeline** that calculates the global top 20 resources and determines exactly how many *globally distinct hosts* accessed them across all batches.
* **Query 3 (Hourly HTTP Error Rate):** Analyzes the distribution of 4xx and 5xx errors by hour of the day, calculating the strict error percentage and tracking distinct host infractions.

---

## Architecture

1. **HDFS BatchSplitter:** Raw logs (`/etl/input/`) are strictly partitioned in HDFS into 500,000-line chunks.
2. **Dual-Engine Execution:** The chosen engine (Pig or MapReduce) is triggered programmatically by the controller to process the chunks.
3. **Strict Parity Auditing:** A standardized regex strategy ensures strict parity between MapReduce (Counters) and Pig (HDFS splitting), reporting exactly 1 malformed record globally.
4. **JDBC Loader:** Final outputs are parsed and loaded into PostgreSQL using `ResultLoader.java` for long-term analytical querying.
