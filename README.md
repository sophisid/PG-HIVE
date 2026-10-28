## PG-HIVE: Hybrid Incremental Schema Discovery for Property Graphs

The full paper is available here:  
https://openproceedings.org/2026/conf/edbt/paper-201.pdf  

---

### Run PG-HIVE in a few steps

1. Download the datasets from Zenodo and follow the provided instructions.
2. Load a dataset into Neo4j (version 4.4.0).
3. Run the following Cypher query:
```cypher
   CALL { MATCH (n) SET n.original_label = labels(n) } IN TRANSACTIONS OF 1000 ROWS
````

4. Navigate to the project:

   ```bash
   cd schemadiscovery
   ```
5. Run PG-HIVE:

   ```bash
   sbt "run LSH"
   ```

   or for incremental schema discovery:

   ```bash
   sbt "run LSH INCREMENTAL"
   ```

---

### Memory usage

Full-graph loading streams Neo4j records to temporary files and reads them through
Spark, instead of collecting the graph on the driver. Large cluster caches use
disk storage; merged results are checkpointed before their parent cache is
released. Temporary files are removed when the application exits.

Set `PG_HIVE_TEMP_DIR` to a directory with enough free disk space (the default is
the JVM temporary directory). Neo4j connection settings can be supplied through
`NEO4J_URI`, `NEO4J_USER`, and `NEO4J_PASSWORD`; existing defaults still apply.
The queries and inference parameters are unchanged. Neo4j's random-order queries
still require server-side memory independently of Spark's reduced driver usage.

Run the loader and LSH/MinHash equivalence tests from `schemadiscovery` with
`sbt test` (JDK 11).

## Citation

If you use **PG-HIVE**, please cite:

```bibtex
@inproceedings{sideri2026pghive,
  title={PG-HIVE: Hybrid Incremental Schema Discovery for Property Graphs},
  author={Sideri, Sophia and Troullinou, Georgia and Ymeralli, Elisjana and Efthymiou, Vasilis and Plexousakis, Dimitris and Kondylakis, Haridimos},
  booktitle={Proceedings of the EDBT 2026 Conference},
  year={2026},
  url={https://openproceedings.org/2026/conf/edbt/paper-201.pdf}
}
```

```
