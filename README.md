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
