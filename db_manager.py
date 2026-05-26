import pyodbc
import mysql.connector
import platform
import logging

class DatabaseManager:
    def __init__(self, config):
        self.config = config
        self.local_conn = None
        self.remote_conn = None
        self.arch = platform.architecture()[0]

    def connect_local(self):
        """
        Connect to local MS Access Database using pyodbc.
        Searches through multiple known Access drivers to find a compatible one.
        """
        try:
            if self.arch == '32bit':
                logging.warning("⚠️ NOTE: You are running 32-bit Python. Ensure you have the corresponding 32-bit Access Database Engine installed.")
            else:
                logging.info("Running on 64-bit Python environment.")

            c = self.config['LOCAL_ACCESS']
            path = c['RutaBD']
            
            # Common driver names for MS Access
            drivers = [
                '{Microsoft Access Driver (*.mdb, *.accdb)}',
                '{Microsoft Access Driver (*.mdb)}',
                'Microsoft Access Driver (*.mdb, *.accdb)'
            ]
            
            connected = False
            for drv in drivers:
                try:
                    conn_str = f"DRIVER={drv};DBQ={path};"
                    logging.info(f"Attempting to connect to Access using driver: {drv}")
                    self.local_conn = pyodbc.connect(conn_str)
                    logging.info(f"Connected to Local Access DB using driver: {drv}")
                    connected = True
                    break
                except Exception as e:
                    logging.debug(f"Driver {drv} failed: {e}")
                    continue
            
            if not connected:
                # Let's list available pyodbc drivers for diagnostic visibility
                all_drivers = pyodbc.drivers()
                raise Exception(
                    f"Could not find a compatible Access driver for path: {path}. "
                    f"Available drivers in your system: {all_drivers}. "
                    f"Please install the Microsoft Access Database Engine."
                )

            return True
        except Exception:
            logging.exception("Error connecting to local Access database:")
            return False

    def connect_remote(self):
        """
        Connect to remote MySQL Database.
        """
        try:
            c = self.config['REMOTE_MYSQL']
            logging.info(f"Connecting to remote MySQL server: {c['Servidor']}...")
            self.remote_conn = mysql.connector.connect(
                host=c['Servidor'],
                user=c['Usuario'],
                password=c['Passwd'],
                database=c['BaseDatos'],
                autocommit=True
            )
            logging.info("Connected to Remote MySQL DB successfully.")
            return True
        except Exception:
            logging.exception("Error connecting to remote MySQL database:")
            return False

    def close_all(self):
        """
        Close all active database connections safely.
        """
        if self.local_conn:
            try:
                self.local_conn.close()
                logging.info("Closed local database connection.")
            except Exception:
                pass
        if self.remote_conn:
            try:
                self.remote_conn.close()
                logging.info("Closed remote database connection.")
            except Exception:
                pass
