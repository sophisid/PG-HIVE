#!/bin/bash

ROOT_DIR="<your_root_directory>"
PROJECT_DIR="<your_project_directory>"
DATASETS_DIR="$PROJECT_DIR/noisy_datasets/MB6"
SCHEMA_DISCOVERY_DIR="$PROJECT_DIR/schemadiscovery"
NEO4J_TAR="neo4j-community.tar.gz"
NEO4J_VERSION="neo4j-community-4.4.0"
NEO4J_DIR="$NEO4J_VERSION"
OUTPUT_BASE_DIR="$PROJECT_DIR/output"
NEO4J_PORT=7687

mkdir -p "$OUTPUT_BASE_DIR"

datasets=($(ls "$DATASETS_DIR" | grep "corrupted"))

# Loop through each dataset
for dataset in "${datasets[@]}"
do
    echo "==============================="
    echo "Processing Dataset: $dataset"
    echo "==============================="

    current_dataset_dir="$DATASETS_DIR/$dataset"

    # Step 1: Remove the current Neo4j database directory
    echo "Removing $NEO4J_VERSION directory..."
    rm -rf "$NEO4J_DIR"

    # Step 2: Extract Neo4j from the tar.gz
    echo "Extracting Neo4j from $NEO4J_TAR..."
    tar -xzvf "$NEO4J_TAR"

    # Step 3: Import the CSV files into Neo4j
     echo "Importing data into Neo4j from $current_dataset_dir..."
    "$NEO4J_DIR/bin/neo4j-admin" import --database=neo4j --delimiter=',' \
        --nodes=Meta="$current_dataset_dir/Neuprint_Meta_mb6_corrupted.csv" \
        --nodes=Neuron="$current_dataset_dir/Neuprint_Neurons_mb6_corrupted.csv" \
        --relationships=CONNECTS_TO="$current_dataset_dir/Neuprint_Neuron_Connections_mb6_corrupted.csv" \
        --nodes=SynapseSet="$current_dataset_dir/Neuprint_SynapseSet_mb6_corrupted.csv" \
        --relationships=CONNECTS_TO="$current_dataset_dir/Neuprint_SynapseSet_to_SynapseSet_mb6_corrupted.csv" \
        --relationships=CONTAINS="$current_dataset_dir/Neuprint_Neuron_to_SynapseSet_mb6_corrupted.csv" \
        --nodes=Synapse="$current_dataset_dir/Neuprint_Synapses_mb6_corrupted.csv" \
        --relationships=SYNAPSES_TO="$current_dataset_dir/Neuprint_Synapse_Connections_mb6_corrupted.csv" \
        --relationships=CONTAINS="$current_dataset_dir/Neuprint_SynapseSet_to_Synapses_mb6_corrupted.csv"

    # Step 4: Start Neo4j
    echo "Starting Neo4j..."
    "$NEO4J_DIR/bin/neo4j" start

    # Wait for Neo4j to start
    echo "Waiting for Neo4j to start..."
    sleep 100  

    # Step 5: Save original labels
    echo "Saving original labels..."
    $NEO4J_DIR/bin/cypher-shell -u neo4j -p password "CALL { MATCH (n) SET n.original_label = labels(n) } IN TRANSACTIONS OF 1000 ROWS"

    # Step 6: Process with different label removal percentages
    for percentage in 0.0 0.5 1.0
    do
        echo "Removing labels for $percentage of nodes..."
        $NEO4J_DIR/bin/cypher-shell -u neo4j -p password "
            CALL {
                MATCH (n)
                WHERE rand() < $percentage
                REMOVE n:Meta
                REMOVE n:Neuron
                REMOVE n:SynapseSet
                REMOVE n:Synapse
            } IN TRANSACTIONS OF 1000 ROWS"


        echo "Running Schema Discovery with $percentage label removal (incremental)..."
        cd "$SCHEMA_DISCOVERY_DIR"
        sbt "run LSH INCREMENTAL" > "$OUTPUT_BASE_DIR/output_Hybrid_MB6_INC_${dataset#corrupted}_${percentage}.txt"
        cd "$ROOT_DIR"
    done

    # Step 7: Stop Neo4j
    echo "Stopping Neo4j..."
    "$NEO4J_DIR/bin/neo4j" stop
    sleep 100  

    # Step 8: Dynamically kill any process on port 7687
    echo "Checking and killing any process on port $NEO4J_PORT..."
    PID=$(lsof -t -i :$NEO4J_PORT)

    if [ -z "$PID" ]; then
        echo "No process found on port $NEO4J_PORT."
    else
        echo "Killing process with PID: $PID"
        kill -9 $PID
        if [ $? -eq 0 ]; then
            echo "Process $PID successfully killed."
        else
            echo "Failed to kill process $PID."
        fi
    fi

    echo "Finished processing dataset: $dataset"
    echo ""
done

echo "All datasets have been processed."
