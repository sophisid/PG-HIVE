#!/bin/bash

ROOT_DIR="/mnt/fast/sophisid"
PROJECT_DIR="/mnt/fast/sophisid/HybridLSHSchemaDiscovery"
SCHEMA_DISCOVERY_DIR="$PROJECT_DIR/schemadiscovery"
OUTPUT_BASE_DIR="$PROJECT_DIR/output"
NEO4J_VERSION="neo4j-community-4.4.0"
NEO4J_DIR="$ROOT_DIR/$NEO4J_VERSION"
NEO4J_PORT=7687
DUMP_URL="https://offshoreleaks-data.icij.org/offshoreleaks/neo4j/icij-offshoreleaks-4.4.26.dump"
DUMP_FILE="icij-offshoreleaks-4.4.26.dump"

# Node properties to remove (excluding original_label)
NODE_PROPERTIES=(
  "valid_until" "struck_off_date" "original_name" "jurisdiction" "ibcRUC"
  "_labels" "icij_id" "address" "comments" "service_provider" "note" "type"
  "lastEditTimestamp" "name" "jurisdiction_description" "former_name"
  "original_address" "company_type" "country" "incorporation_date" "addressID"
  "company_number" "closed_date" "dorm_date" "country_codes" "countries"
  "registration_date" "internal_id" "tax_stat_description" "inactivation_date"
  "sourceID" "status" "entity_number" "end_date" "country_code"
  "registered_office" "node_id"
)

# Edge properties to remove
EDGE_PROPERTIES=(
  "valid_until" "srcType" "relationshipType" "dstType" "lastEditTimestamp"
  "link" "sourceID" "status" "end_date" "start_date"
)

mkdir -p "$OUTPUT_BASE_DIR"

noise_levels=(0 10 20 30 40)
label_percents=(0.0 0.5 1.0)

# Trap for cleanup
trap 'rm -f "$DUMP_FILE"' EXIT

for noise in "${noise_levels[@]}"; do
  echo "==============================="
  echo "Processing Noise Level: $noise%"
  echo "==============================="

  # Stop Neo4j
  echo "Stopping Neo4j..."
  $NEO4J_DIR/bin/neo4j stop
  sleep 10
  if $NEO4J_DIR/bin/neo4j status >/dev/null; then
    echo "Error: Neo4j failed to stop"
    exit 1
  fi

  # Kill any leftover process on Bolt port
  PID=$(lsof -t -i :$NEO4J_PORT)
  if [ -n "$PID" ]; then
    echo "Killing process on port $NEO4J_PORT (PID: $PID)"
    kill -9 "$PID"
  fi

  # Redownload dump fresh
  echo "Downloading dump file..."
  rm -f "$DUMP_FILE"
  curl -fL --retry 5 --retry-connrefused --retry-delay 5 --progress-bar -o "$DUMP_FILE" "$DUMP_URL"

  # Reload dump
  echo "Loading Neo4j dump..."
  $NEO4J_DIR/bin/neo4j-admin load --from="$DUMP_FILE" --database=neo4j --force

  # Start Neo4j
  echo "Starting Neo4j..."
  $NEO4J_DIR/bin/neo4j start
  # Wait for Neo4j to be ready (poll up to 300s)
  timeout 300s bash -c "until $NEO4J_DIR/bin/cypher-shell -u neo4j -p password 'RETURN 1' >/dev/null 2>&1; do sleep 5; done"
  if [ $? -ne 0 ]; then
    echo "Error: Neo4j failed to start"
    exit 1
  fi

  # Save original labels
  echo "Saving original labels..."
  $NEO4J_DIR/bin/cypher-shell -u neo4j -p password \
    "MATCH (n) SET n.original_label = labels(n)" \
    --debug >> "$OUTPUT_BASE_DIR/log_noise${noise}_init.txt" 2>&1

  # -----------------------------
  # Add property noise (nodes)
  # -----------------------------
  frac=$(echo "$noise/100" | bc -l)
  echo "Removing node properties with fraction $frac"
  for prop in "${NODE_PROPERTIES[@]}"; do
    echo "Removing node property: $prop"
    # Write query to a temp file to avoid parameter escaping issues
    TEMP_CYPHER=$(mktemp /tmp/cypher_node.XXXXXX.cypher)
    echo "MATCH (n) WHERE rand() < $frac AND '$prop' IN keys(n) REMOVE n.\`$prop\`" > "$TEMP_CYPHER"
    $NEO4J_DIR/bin/cypher-shell -u neo4j -p password \
      -f "$TEMP_CYPHER" \
      --debug >> "$OUTPUT_BASE_DIR/log_noise${noise}_node_${prop}.txt" 2>&1
    rm -f "$TEMP_CYPHER"
  done

  # Debug: Check remaining node properties
  echo "Checking remaining node properties..."
  $NEO4J_DIR/bin/cypher-shell -u neo4j -p password \
    "MATCH (n) UNWIND keys(n) AS k RETURN k, count(*) AS cnt ORDER BY k LIMIT 10" \
    --debug > "$OUTPUT_BASE_DIR/node_props_noise${noise}.txt" 2>&1

  # -----------------------------
  # Add property noise (relationships)
  # -----------------------------
  echo "Removing relationship properties with fraction $frac"
  for prop in "${EDGE_PROPERTIES[@]}"; do
    echo "Removing relationship property: $prop"
    TEMP_CYPHER=$(mktemp /tmp/cypher_rel.XXXXXX.cypher)
    echo "MATCH ()-[r]-() WHERE rand() < $frac AND '$prop' IN keys(r) REMOVE r.\`$prop\`" > "$TEMP_CYPHER"
    $NEO4J_DIR/bin/cypher-shell -u neo4j -p password \
      -f "$TEMP_CYPHER" \
      --debug >> "$OUTPUT_BASE_DIR/log_noise${noise}_rel_${prop}.txt" 2>&1
    rm -f "$TEMP_CYPHER"
  done

  # Debug: Check remaining relationship properties
  echo "Checking remaining relationship properties..."
  $NEO4J_DIR/bin/cypher-shell -u neo4j -p password \
    "MATCH ()-[r]-() UNWIND keys(r) AS k RETURN k, count(*) AS cnt ORDER BY k LIMIT 10" \
    --debug > "$OUTPUT_BASE_DIR/rel_props_noise${noise}.txt" 2>&1

  for perc in "${label_percents[@]}"; do
    echo "Removing labels for fraction $perc"

    # Write label removal query to a temp file
    TEMP_CYPHER=$(mktemp /tmp/cypher_label.XXXXXX.cypher)
    echo "MATCH (n) WHERE rand() < $perc REMOVE n:Address:Entity:Intermediary:Node:Officer:Other" > "$TEMP_CYPHER"
    $NEO4J_DIR/bin/cypher-shell -u neo4j -p password \
      -f "$TEMP_CYPHER" \
      --debug >> "$OUTPUT_BASE_DIR/log_noise${noise}_labels${perc}.txt" 2>&1
    rm -f "$TEMP_CYPHER"
    sleep 10

    # Debug: Check remaining labels
    echo "Checking remaining labels..."
    $NEO4J_DIR/bin/cypher-shell -u neo4j -p password \
      "MATCH (n) UNWIND labels(n) AS l RETURN l, count(*) AS cnt ORDER BY l" \
      --debug > "$OUTPUT_BASE_DIR/labels_noise${noise}_perc${perc}.txt" 2>&1

    echo "Running Schema Discovery with LSH..."
    cd "$SCHEMA_DISCOVERY_DIR"
    sbt "run LSH" > "$OUTPUT_BASE_DIR/output_ICIJ_noise${noise}_labels${perc}_LSH.txt" 2>&1

    # Clean Spark tmp
    rm -rf /tmp/blockmgr-* /tmp/spark-* /tmp/**/temp_shuffle_* 2>/dev/null

    echo "Running Schema Discovery with KMEANS..."
    sbt "run KMEANS" > "$OUTPUT_BASE_DIR/output_ICIJ_noise${noise}_labels${perc}_KMEANS.txt" 2>&1

    cd "$ROOT_DIR"

    # Clean Spark tmp
    rm -rf /tmp/blockmgr-* /tmp/spark-* /tmp/**/temp_shuffle_* 2>/dev/null
  done

  # Stop Neo4j
  echo "Stopping Neo4j..."
  $NEO4J_DIR/bin/neo4j stop
  sleep 10
  if $NEO4J_DIR/bin/neo4j status >/dev/null; then
    echo "Error: Neo4j failed to stop"
    exit 1
  fi
done

echo "All noise levels processed."