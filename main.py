import logging
import configparser
from db_manager import DatabaseManager
from sync_engine import SyncEngine

import os
import sys

# Get absolute path of the script directory
script_dir = os.path.dirname(os.path.abspath(__file__))
log_file = os.path.join(script_dir, 'sync_integra.log')

# Setup Logging
log_handlers = [logging.FileHandler(log_file, encoding='utf-8')]
if sys.stderr is not None:
    log_handlers.append(logging.StreamHandler())

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=log_handlers
)

def main():
    config = configparser.ConfigParser()
    config_path = os.path.join(script_dir, 'config.ini')
    
    try:
        # Load Configuration
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Configuration file config.ini not found in: {config_path}")
            
        config.read(config_path, encoding='utf-8')
        
        if not config.has_section('SETTINGS'):
            raise KeyError("Section [SETTINGS] is missing in config.ini")
        
        logging.info("==================================================")
        logging.info("Integra Python Sincronización Utility")
        logging.info("==================================================")
        
        db_manager = DatabaseManager(config)
        engine = SyncEngine(db_manager, config)
        
        # Execute Synchronization
        engine.execute_sync()
        
    except Exception as e:
        logging.exception("Critical error during execution of sync utility:")
        if sys.stdout:
            print(f"\n[ ERROR ] {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
