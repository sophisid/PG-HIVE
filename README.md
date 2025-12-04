# PG-HIVE: Hybrid Incremental Schema Discovery for Property Graphs
This repository contains the implementation accompanying research paper:

**Sofia Sideri, Georgia Troullinou, Elisjana Ymeralli, Vasilis Efthymiou, Dimitris Plexousakis, and Haridimos Kondylakis.**
**“PG-HIVE: Hybrid Incremental Schema Discovery for Property Graphs”**, arXiv preprint, 2025.
Preprint: [https://arxiv.org/abs/2512.01092](https://arxiv.org/abs/2512.01092)

If you use **PG-HIVE** in your research, please consider citing the paper (BibTeX below).

---
## Overview

**PG-HIVE** is a tool designed to discover schemas within **Property Graph Databases**. It supports incremental schema discovery and helps identify the structure, patterns, and relationships in graph data, even in the absence of type labels, facilitating the understanding and exploration of datasets.

The project supports schema discovery on popular datasets like **LDBC**, **FIB25**, **MB6** and **Cord-19**, and integrates with **Neo4j** for seamless graph data management.

---

## Table of Contents

1. [Installation](#installation)  
2. [Neo4j Setup](#neo4j-setup)  
3. [Dataset Preparation](#dataset-preparation)
   - [Create Noisy Datasets](#noisy)
4. [Usage](#usage)  
   - [Option 1: Using the Automated Script](#option-1-using-the-automated-script)  
   - [Option 2: Manual Execution](#option-2-manual-execution)  
   - [Incremental Script](#incremental-script)  
5. [Post-Import Steps](#post-import-steps)  
6. [Notes](#notes)  
7. [License](#license)  
8. [Contributions](#contributions)

---

## Installation

To install and set up **PG-HIVE**, follow these steps:

### 1. Clone the Repository.

### 2. Build the Project

Navigate to the **`schemadiscovery`** directory and build the project using `sbt` (Scala Build Tool):

```
cd schemadiscovery
sbt compile
```
in order for PG-HIVE to evaluate the data, you need to perform this cypher query:
```
CALL { MATCH (n) SET n.original_label = labels(n) } IN TRANSACTIONS OF 1000 ROWS
```

---

## Neo4j Setup

**PG-HIVE** relies on Neo4j for managing and querying the property graphs. To set up Neo4j:

### 1. Download and Install Neo4j Community Edition 4.4.0

```
wget https://dist.neo4j.org/neo4j-community-4.4.0-unix.tar.gz
tar -xzf neo4j-community-4.4.0-unix.tar.gz
cd neo4j-community-4.4.0
```

You can set the `NEO4J_DIR` environment variable to this directory:

```
export NEO4J_DIR=$(pwd)
```

### 2. Start the Neo4j Server

```
bin/neo4j start
```

Neo4j will be available at [http://localhost:7474](http://localhost:7474).

### 3. Configure Neo4j

1. Set the **initial password** for the default user `neo4j` when prompted. The default is "password" - the code has hardcoded the dafault you can change it in the **DataLaoder.scala** file, if you change it on setup.
2. Access the Neo4j browser and verify the connection.

---

## Dataset Preparation

The project includes **evaluation datasets** (FIB25, LDBC, MB6) that need to be unzipped and loaded into Neo4j.

### 1. Unzip the Datasets 

```
cd datasets
unzip FIB25/fib25_neo4j_inputs.zip
unzip LDBC/ldbc_neo4j_inputs1.zip
unzip LDBC/ldbc_neo4j_inputs2.zip
unzip LDBC/ldbc_neo4j_inputs3.zip
unzip LDBC/ldbc_neo4j_inputs4.zip
unzip MB6/mb6_neo4j_inputs1.zip
```
if you want to test the rest of the datasets:
-- [Cord19 graph codebase](https://github.com/covidgraph/data_cord19)
-- [The panama papers (icji) dataset](https://www.kaggle.com/datasets/zusmani/paradisepanamapapers)
-- [het.io dataset](https://het.io/)
-- [The internet yellow pages](https://iyp.iijlab.net/)
-- [POLE](https://github.com/neo4j-graph-examples/pole)

### 2. Load Datasets into Neo4j

Before importing, ensure that:

1. Neo4j is stopped if currently running.
2. `NEO4J_DIR` is set to your Neo4j installation directory.
3. `current_dataset_dir` is set to the path containing the dataset CSV files.

**General Preparation Steps:**

```
cd $NEO4J_DIR
bin/neo4j stop   # Stop Neo4j if running
rm -rf $NEO4J_DIR/data/databases/neo4j  # Delete old database if needed

export current_dataset_dir=<path-to-datasets>
```

---

### **LDBC Dataset Import (using '|' delimiter)**

```
$NEO4J_DIR/bin/neo4j-admin import --database=neo4j --delimiter='|' \
    --nodes=Forum="$current_dataset_dir/forum_0_0.csv" \
    --nodes=Person="$current_dataset_dir/person_0_0.csv" \
    --nodes=Post="$current_dataset_dir/post_0_0.csv" \
    --nodes=Place="$current_dataset_dir/place_0_0.csv" \
    --nodes=Organisation="$current_dataset_dir/organisation_0_0.csv" \
    --nodes=TagClass="$current_dataset_dir/tagclass_0_0.csv" \
    --nodes=Tag="$current_dataset_dir/tag_0_0.csv" \
    --relationships=CONTAINER_OF="$current_dataset_dir/forum_containerOf_post_0_0.csv" \
    --relationships=HAS_MEMBER="$current_dataset_dir/forum_hasMember_person_0_0.csv" \
    --relationships=HAS_MODERATOR="$current_dataset_dir/forum_hasModerator_person_0_0.csv" \
    --relationships=HAS_TAG="$current_dataset_dir/forum_hasTag_tag_0_0.csv" \
    --relationships=HAS_INTEREST="$current_dataset_dir/person_hasInterest_tag_0_0.csv" \
    --relationships=IS_LOCATED_IN="$current_dataset_dir/person_isLocatedIn_place_0_0.csv" \
    --relationships=KNOWS="$current_dataset_dir/person_knows_person_0_0.csv" \
    --relationships=LIKES="$current_dataset_dir/person_likes_post_0_0.csv" \
    --relationships=STUDIES_AT="$current_dataset_dir/person_studyAt_organisation_0_0.csv" \
    --relationships=WORKS_AT="$current_dataset_dir/person_workAt_organisation_0_0.csv" \
    --relationships=HAS_CREATOR="$current_dataset_dir/post_hasCreator_person_0_0.csv" \
    --relationships=HAS_TAG="$current_dataset_dir/post_hasTag_tag_0_0.csv" \
    --relationships=IS_LOCATED_IN="$current_dataset_dir/post_isLocatedIn_place_0_0.csv" \
    --relationships=IS_LOCATED_IN="$current_dataset_dir/organisation_isLocatedIn_place_0_0.csv" \
    --relationships=IS_PART_OF="$current_dataset_dir/place_isPartOf_place_0_0.csv" \
    --relationships=HAS_TYPE="$current_dataset_dir/tag_hasType_tagclass_0_0.csv" \
    --relationships=IS_SUBCLASS_OF="$current_dataset_dir/tagclass_isSubclassOf_tagclass_0_0.csv"
```

---

### **MB6 Dataset Import (using ',' delimiter)**

```
$NEO4J_DIR/bin/neo4j-admin import --database=neo4j --delimiter=',' \
    --nodes=Meta="$current_dataset_dir/Neuprint_Meta_mb6.csv" \
    --nodes=Neuron="$current_dataset_dir/Neuprint_Neurons_mb6.csv" \
    --relationships=CONNECTS_TO="$current_dataset_dir/Neuprint_Neuron_Connections_mb6.csv" \
    --nodes=SynapseSet="$current_dataset_dir/Neuprint_SynapseSet_mb6.csv" \
    --relationships=CONNECTS_TO="$current_dataset_dir/Neuprint_SynapseSet_to_SynapseSet_mb6.csv" \
    --relationships=CONTAINS="$current_dataset_dir/Neuprint_Neuron_to_SynapseSet_mb6.csv" \
    --nodes=Synapse="$current_dataset_dir/Neuprint_Synapses_mb6.csv" \
    --relationships=SYNAPSES_TO="$current_dataset_dir/Neuprint_Synapse_Connections_mb6.csv" \
    --relationships=CONTAINS="$current_dataset_dir/Neuprint_SynapseSet_to_Synapses_mb6.csv"
```

---

### **FIB25 Dataset Import (using ',' delimiter)**

```
$NEO4J_DIR/bin/neo4j-admin import --database=neo4j --delimiter=',' \
    --nodes=Meta="$current_dataset_dir/Neuprint_Meta_fib25.csv" \
    --nodes=Neuron="$current_dataset_dir/Neuprint_Neurons_fib25.csv" \
    --relationships=CONNECTS_TO="$current_dataset_dir/Neuprint_Neuron_Connections_fib25.csv" \
    --nodes=SynapseSet="$current_dataset_dir/Neuprint_SynapseSet_fib25.csv" \
    --relationships=CONNECTS_TO="$current_dataset_dir/Neuprint_SynapseSet_to_SynapseSet_fib25.csv" \
    --relationships=CONTAINS="$current_dataset_dir/Neuprint_Neuron_to_SynapseSet_fib25.csv" \
    --nodes=Synapse="$current_dataset_dir/Neuprint_Synapses_fib25.csv" \
    --relationships=SYNAPSES_TO="$current_dataset_dir/Neuprint_Synapse_Connections_fib25.csv" \
    --relationships=CONTAINS="$current_dataset_dir/Neuprint_SynapseSet_to_Synapses_fib25.csv"
```

---

## Post-Import Steps

1. **Verify the Import**:
   ```
   $NEO4J_DIR/bin/neo4j-admin check-consistency --database=neo4j
   ```

2. **Start the Neo4j Server**:
   ```
   cd $NEO4J_DIR
   bin/neo4j start
   ```
   
3. **Access the Neo4j Browser**:  
   Open [http://localhost:7474](http://localhost:7474) to visualize the graph and run queries.

---

## Notes

1. **Delimiters**:  
   - Use `|` for **LDBC** dataset.  
   - Use `,` for **MB6** and **FIB25** datasets.

2. **CSV Format Requirements**:  
   - Nodes must have a unique `id` field.
   - Relationships must have `:START_ID`, `:END_ID`, and `:TYPE` columns.
   
3. **Reloading Data**:  
   If you need to re-import, remove the database first:
   ```
   rm -rf $NEO4J_DIR/data/databases/neo4j
   ```

---

## Usage

Once the setup is complete and the datasets are loaded, you can run **PG-HIVE** to perform schema discovery.

**PG-HIVE** can be executed in multiple ways:

### Option 1: Using the Automated Script

Scripts (e.g., `run_pghive_ldbc.sh`, `run_pghive_fib.sh`, `run_pghive_mb6.sh`) are provided to automate the entire process:

1. Set environment variables and directories inside these scripts.
2. Make them executable:
   ```
   chmod +x run_pghive_ldbc.sh
   chmod +x run_pghive_fib.sh
   chmod +x run_pghive_mb6.sh
   ```
3. Run the script for the desired dataset:
   ```
   ./run_pghive_ldbc.sh
   ./run_pghive_fib.sh
   ./run_pghive_mb6.sh
   ```

The scripts handle:
- Removing and re-extracting Neo4j.
- Importing data.
- Create various test cases, with 0%-50%-100% usage of labels.
- Running schema discovery (LSH clustering).
- Stopping Neo4j and cleaning up.

The rest of the datasets where evaluated with the script run_dump_script.sh , given a dump, the datasets labels and properties it handles all the aforementioned.

### Option 2: Manual Execution

If you prefer more control, follow the manual steps:
1. Stop Neo4j, remove old database, re-extract Neo4j.
2. Import the desired dataset with `neo4j-admin import`.
3. In order for PG-HIVE to evaluate the data, you need to perform this cypher query:
```
CALL { MATCH (n) SET n.original_label = labels(n) } IN TRANSACTIONS OF 1000 ROWS
```

4. Start Neo4j.
5. Run:
   ```
   cd schemadiscovery
   sbt "run LSH"  # LSH clustering
   ```
6. or for incremental execution:
   ```
   cd schemadiscovery
   sbt "run LSH INCREMENTAL"  # LSH clustering
   ```

7. Stop Neo4j and clean up if needed.

#### ⚠️ Testing locally 
If you are testing PG-HIVE on a local machine or with a large dataset, make sure to add a LIMIT clause in the loadAllNodes and loadAllRelationships methods inside DataLoader.scala to avoid excessive memory consumption or long execution times.

### Incremental Script
Scripts (e.g., `run_pghive_ldbc_incremental.sh`, `run_pghive_fib_incremental.sh`, `run_pghive_mb6_incremental.sh`) are provided to automate the entire process:

1. Set environment variables and directories inside these scripts.
2. Make them executable:
   ```
   chmod +x run_pghive_ldbc_incremental.sh
   chmod +x run_pghive_fib_incremental.sh
   chmod +x run_pghive_mb6_incremental.sh
   ```
3. Run the script for the desired dataset:
   ```
   ./run_pghive_ldbc_incremental.sh
   ./run_pghive_fib_incremental.sh
   ./run_pghive_mb6_incremental.sh
   ```

## License

This project is licensed under the **MIT License**. See the [LICENSE](LICENSE) file for details.

---


## Contributions

Contributions to **PG-HIVE** are welcome! If you find bugs, have suggestions, or want to contribute features, feel free to open an issue or submit a pull request.

For questions, feedback, or support, please contact the repository maintainer.

---

## Citation

If you use this code or build upon **PG-HIVE**, please cite:

```bibtex
@misc{sideri2025pghivehybridincrementalschema,
      title={PG-HIVE: Hybrid Incremental Schema Discovery for Property Graphs}, 
      author={Sofia Sideri and Georgia Troullinou and Elisjana Ymeralli and Vasilis Efthymiou and Dimitris Plexousakis and Haridimos Kondylakis},
      year={2025},
      eprint={2512.01092},
      archivePrefix={arXiv},
      primaryClass={cs.DB},
      url={https://arxiv.org/abs/2512.01092}, 
}
```
