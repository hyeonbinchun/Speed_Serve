#!/bin/bash

# Function to display usage
usage() {
    echo "Usage: $0 [-c] [-u] [-p] [-i] [-o] [-w workloadfile]"
    echo "  -c: Compile all code"
    echo "  -u: Start User service"
    echo "  -p: Start Product service"
    echo "  -i: Start ISCS"
    echo "  -o: Start Order service"
    echo "  -w workloadfile: Start workload parser"
    exit 1
}

# Get the directory where the script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
echo "$SCRIPT_DIR"


# Function to compile all code
compile_code() {
    echo "Compiling all services:"

    # Set the PostgreSQL JDBC driver path
    JDBC_DRIVER_PATH="$SCRIPT_DIR/lib/postgresql-42.7.5.jar"
    HIKARICP_DRIVER_PATH="$SCRIPT_DIR/lib/HikariCP-6.2.1.jar"
    SLF4J_API_PATH="$SCRIPT_DIR/lib/slf4j-api-2.0.17.jar"
    SLF4J_SIMPLE_PATH="$SCRIPT_DIR/lib/slf4j-simple-2.0.17.jar"
    JEDIS_DRIVER_PATH="$SCRIPT_DIR/lib/jedis-4.2.3.jar"
    COMMONS_POOL2_PATH="$SCRIPT_DIR/lib/commons-pool2-2.11.1.jar"
    GSON_PATH="$SCRIPT_DIR/lib/gson-2.8.6.jar"

    # Combined classpath for compilation
      CLASSPATH="$JDBC_DRIVER_PATH:$HIKARICP_DRIVER_PATH:$SLF4J_API_PATH:$SLF4J_SIMPLE_PATH:$JEDIS_DRIVER_PATH:$COMMONS_POOL2_PATH:$GSON_PATH"

    # Compile User Service
    echo "Compiling User Service..."
    # Compile Java files directly
    javac --release 11 -cp "$CLASSPATH" -d "$SCRIPT_DIR/compiled/UserService" "$SCRIPT_DIR/src/UserService"/*.java 
    # Copy the config.json file to the compiled/UserService directory
    cp "$SCRIPT_DIR/config.json" "$SCRIPT_DIR/compiled/UserService/"

    # Compile Product Service
    echo "Compiling Product Service..."
    javac --release 11 -cp "$CLASSPATH" -d "$SCRIPT_DIR/compiled/ProductService" "$SCRIPT_DIR/src/ProductService"/*.java 
    cp "$SCRIPT_DIR/config.json" "$SCRIPT_DIR/compiled/ProductService/"                         
    
    # Compile Order Service
    echo "Compiling Order Service..."
    javac --release 11 -cp "$CLASSPATH" -d "$SCRIPT_DIR/compiled/OrderService" "$SCRIPT_DIR/src/OrderService"/*.java
    cp "$SCRIPT_DIR/config.json" "$SCRIPT_DIR/compiled/OrderService/"
    
    # Copy ISCS 
    echo  "Compiling ISCS Service..."
    javac -d "$SCRIPT_DIR/compiled/ISCS" "$SCRIPT_DIR/src/ISCS"/*.java 
    cp "$SCRIPT_DIR/config.json" "$SCRIPT_DIR/compiled/ISCS/"
    
    echo "Compilation completed."
}

# Function to start a service
start_service() {
    local service=$1        # First argument: service name (e.g., "UserService")
    local class_name=$2     # Second argument: Java class name (e.g., "UserService")
    
    # Set the PostgreSQL JDBC driver path
    JDBC_DRIVER_PATH="$SCRIPT_DIR/lib/postgresql-42.7.5.jar"
    HIKARICP_DRIVER_PATH="$SCRIPT_DIR/lib/HikariCP-6.2.1.jar"
    SLF4J_API_PATH="$SCRIPT_DIR/lib/slf4j-api-2.0.17.jar"
    SLF4J_SIMPLE_PATH="$SCRIPT_DIR/lib/slf4j-simple-2.0.17.jar"
    JEDIS_DRIVER_PATH="$SCRIPT_DIR/lib/jedis-4.2.3.jar"
    COMMONS_POOL2_PATH="$SCRIPT_DIR/lib/commons-pool2-2.11.1.jar"
    GSON_PATH="$SCRIPT_DIR/lib/gson-2.8.6.jar"

    # Combined classpath for running
    CLASSPATH="$JDBC_DRIVER_PATH:$HIKARICP_DRIVER_PATH:$SLF4J_API_PATH:$SLF4J_SIMPLE_PATH:$JEDIS_DRIVER_PATH:$COMMONS_POOL2_PATH:$GSON_PATH:."

    cd "$SCRIPT_DIR/compiled/$service" || exit 1
    # Include the JDBC driver in the classpath
    java -cp "$CLASSPATH" "$class_name" "config.json"

    cd - > /dev/null
}

# Function to stop all services
stop_all_services() {
    echo "Shutting down all services..."
    
    # Find and kill Java processes for each service
    for service in "UserService" "ProductService" "ISCS" "OrderService"; do
        echo "Stopping $service..."
        pkill -f "$service" || echo "$service was not running"
    done
    
    echo "All services have been shut down."
}


# Flag Processing 
while getopts "cupiow:" opt; do
    case $opt in
        c) compile_code ;;                                    # -c runs compile
        u) start_service "UserService" "UserService" ;;       # -u starts User service
        p) start_service "ProductService" "ProductService" ;; # -p starts Product service
        i) start_service "ISCS" "ISCS" ;;                    # -i starts ISCS
        o) start_service "OrderService" "OrderService" ;;     # -o starts Order service
        w) # -w runs workload parser
            WORKLOAD_FILE=$OPTARG
            echo "Starting workload parser with file: $WORKLOAD_FILE"
            python3 "$SCRIPT_DIR/workload_parser.py" "config.json" "$WORKLOAD_FILE"
            
            # Check if the last line of the workload file contains "shutdown"
            if [ -f "$WORKLOAD_FILE" ]; then
                LAST_LINE=$(tail -n 1 "$WORKLOAD_FILE")
                if [[ "$LAST_LINE" == *"shutdown"* ]]; then
                    echo "Shutdown command detected in workload file."
                    stop_all_services
                fi
            fi
            ;;
        \?) usage ;;                                         # Invalid option shows usage
    esac
done

if [ $OPTIND -eq 1 ]; then
    usage
fi
