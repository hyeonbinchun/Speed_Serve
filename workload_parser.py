import requests
import sys
import json
import os

class WorkloadParser:
    def __init__(self, config_file: str):
        # Read config file to get service endpoints
        with open(config_file, 'r') as f:
           config = json.load(f)
      
        # Store base URLs for services
        self.order_service_url = f"http://{config['OrderService']['ip']}:{config['OrderService']['port']}"

        # Flag file to track if the server has been started before
        self.restart_flag_file = "restart_flag.txt"

    def parse_workload(self, workload_file: str) -> None:
        """Parse and process each line in the workload file"""
        with open(workload_file, 'r') as f:
            lines = [line.strip() for line in f.readlines()]

            # Check if the first non-comment, non-empty line is a restart command
            first_real_command = None
            for line in lines:
                if line and not line.startswith('#') and not line.startswith('//'):
                    first_real_command = line
                    break
            
            # Check if first command is exactly "restart"
            restart_command = (first_real_command == "restart")

            # Check if we should reset databases
            # Reset when: No restart flag exists AND the first command is not a restart
            if not os.path.exists(self.restart_flag_file) and not restart_command:
                print("No restart flag found and first command is not restart. Deleting all database files...")
                self.reset_databases()

            # Create a restart flag file if one doesn't exist
            # This marks that the services have been started at least once
            if not os.path.exists(self.restart_flag_file):
                with open(self.restart_flag_file, 'w') as flag_file:
                    flag_file.write("Flag indicating that services have been started")
                print("Created restart flag file")

            for line in lines:
                # Skip comments or empty lines
                if not line or line.startswith('#') or line.startswith('//'):
                    continue
                
                # Handle special commands
                if line == "shutdown":
                    print("Shutdown command detected")
                    # Remove restart flag file - this will make the next run check if first command is restart
                    if os.path.exists(self.restart_flag_file):
                        os.remove(self.restart_flag_file)
                        print("Removed restart flag file due to shutdown")
                    # The actual shutdown will be handled by runme.sh
                    continue
                elif line == "restart":
                    print("Restart command detected, keeping existing database")
                    # The main logic for restart is handled at the beginning when checking first command
                    continue
                
                # Process regular commands
                self.process_command(line)

    def reset_databases(self):
        """Reset all database files by deleting and recreating them"""

        # Define database file paths
        db_files = [
            "compiled/UserService/users.txt",
            "compiled/ProductService/products.txt",
            "compiled/OrderService/orders.txt"
        ]

        # Delete all database files
        for db_file in db_files:
            if os.path.exists(db_file):
                os.remove(db_file)
                print(f"Deleted {db_file}")
            else:
                print(f"{db_file} not found, skipping.")

        # Recreate fresh empty database files
        for db_file in db_files:
            os.makedirs(os.path.dirname(db_file), exist_ok=True)  # Ensure directories exist
            open(db_file, 'w').close()  # Create an empty file
            print(f"Recreated fresh database file: {db_file}")

    def process_command(self, line: str) -> None:
        """Process a single command line"""
        parts = line.split()
        if not parts:
            return
        service = parts[0].upper()
        command = parts[1].lower() if len(parts) > 1 else ""

        if service == "USER":
            self.handle_user_command(command, parts[2:])
        elif service == "PRODUCT":
            self.handle_product_command(command, parts[2:])
        elif service == "ORDER":
            self.handle_order_command(command, parts[2:])

    def handle_user_command(self, command: str, args: list) -> None:
        """Handle USER service commands by passing them to OrderService"""
        base_endpoint = f"{self.order_service_url}/user"

        if command == "create":
            try:
                headers = {'Content-Type' : 'application/json'}
                payload = {
                    "command": "create",  
                    "id": int(args[0]),
                    "username": args[1],
                    "email": args[2],
                    "password": args[3]
                }
                response = requests.post(base_endpoint, json=payload, headers=headers)
                if response.status_code == 200:
                    print("Successful: ", response.text)
                else:
                    print("Failed: ", response.text)
            except Exception as e:
                print("Failed:  {}")
          
        elif command == "get":
            try:
                user_id = args[0]
                endpoint = f"{base_endpoint}/{user_id}"
                response = requests.get(endpoint)
                if response.status_code == 200:
                    print("Successful: ", response.text)
                else:
                    print("Failed: ", response.text)
            except Exception as e:
                print("Failed:  {}")

        elif command == "update":
            try:
                headers = {'Content-Type' : 'application/json'}
                payload = {"command": "update", "id": int(args[0])}
                
                # Parse update fields
                for arg in args[1:]:
                    key, value = arg.split(':', 1)
                    payload[key] = value

                response = requests.post(base_endpoint, json=payload, headers=headers)
                if response.status_code == 200:
                    print("Successful: ", response.text)
                else:
                    print("Failed: ", response.text)
            except Exception as e:
                print("Failed:  {}")
        
        elif command == "delete":
            try:
                headers = {'Content-Type' : 'application/json'}
                payload = {
                    "command": "delete",  # Communicate intent to OrderService
                    "id": int(args[0]),
                    "username": args[1],
                    "email": args[2],
                    "password": args[3]
                }
                response = requests.post(base_endpoint, json=payload, headers=headers)
                if response.status_code == 200:
                    print("Successful: ", response.text)
                else:
                    print("Failed: ", response.text)
            except Exception as e:
                print("Failed:  {}")
      
    def handle_product_command(self, command: str, args: list) -> None:
        """Handle PRODUCT service commands by passing them to OrderService"""
        base_endpoint = f"{self.order_service_url}/product"

        if command == "create":
            try:
                headers = {'Content-Type' : 'application/json'}
                payload = {
                    "command": "create",
                    "id": int(args[0]),
                    "name": args[1],
                    "description": args[2],
                    "price": float(args[3]),
                    "quantity": int(args[4])
                }
                response = requests.post(base_endpoint, json=payload, headers=headers)
                if response.status_code == 200:
                    print("Successful: ", response.text)
                else:
                    print("Failed: ", response.text)
            except Exception as e:
                print("Failed:  {}")
            
        elif command == "info":
            try:
                product_id = args[0]
                endpoint = f"{base_endpoint}/{product_id}"
                response = requests.get(endpoint)
                if response.status_code == 200:
                    print("Successful: ", response.text)
                else:
                    print("Failed: ", response.text)
            except Exception as e:
                print("Failed:  {}")
            
        elif command == "update":
            try:
                headers = {'Content-Type' : 'application/json'}
                payload = {"command": "update", "id": int(args[0])}
                
                # Parse update fields
                for arg in args[1:]:
                    key, value = arg.split(':', 1)
                    payload[key] = value

                response = requests.post(base_endpoint, json=payload, headers=headers)
                if response.status_code == 200:
                    print("Successful: ", response.text)
                else:
                    print("Failed: ", response.text)
            except Exception as e:
                print("Failed:  {}")
        
        elif command == "delete":
            try:
                headers = {'Content-Type' : 'application/json'}
                payload = {
                    "command": "delete",
                    "id": int(args[0]),
                    "name": args[1],
                    "price": float(args[2]),
                    "quantity": int(args[3])
                }
                response = requests.post(base_endpoint, json=payload, headers=headers)
                if response.status_code == 200:
                    print("Successful: ", response.text)
                else:
                    print("Failed: ", response.text)
            except Exception as e:
                print("Failed:  {}")

    def handle_order_command(self, command: str, args: list) -> None:
        """Handle ORDER service commands directly via OrderService"""       
        base_endpoint = f"{self.order_service_url}/order"

        if command == "place":
            try:
                headers = {'Content-Type' : 'application/json'}
                payload = {
                    "command": "place order",
                    "product_id": int(args[0]),
                    "user_id": int(args[1]),
                    "quantity": int(args[2])
                }
                response = requests.post(base_endpoint, json=payload, headers=headers)
                if response.status_code == 200:
                    print("Successful: ", response.text)
                else:
                    print("Failed: ", response.text)
            except Exception as e:
                print("Failed: {\"status\":\"Invalid Request\"}")

def main():
    if len(sys.argv) != 3:
        print("Usage: python3 workload_parser.py <config_file> <workload_file>")
        sys.exit(1)

    config_file = sys.argv[1]     # config.json
    workload_file = sys.argv[2]   # workload...txt
    
    parser = WorkloadParser(config_file)
    parser.parse_workload(workload_file)

if __name__ == "__main__":
    main()