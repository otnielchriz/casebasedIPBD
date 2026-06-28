# ==============================================================================
# Spark API daemon to receive commands from Airflow to start/stop the Consumer
# ==============================================================================

from http.server import BaseHTTPRequestHandler, HTTPServer
import subprocess
import os
import signal

process = None

class SparkAPIHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        global process
        
        if self.path == '/start':
            if process and process.poll() is None:
                self.send_response(200)
                self.send_header('Content-Type', 'text/plain')
                self.end_headers()
                self.wfile.write(b"Spark consumer already running.")
                return
            
            cmd = [
                "/opt/spark/bin/spark-submit",
                "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
                "/opt/airflow/scrapers/pyspark_consumer_weather.py"
            ]
            
            try:
                # Run the Spark job as a background process group
                process = subprocess.Popen(
                    cmd,
                    stdout=open("/opt/airflow/logs/spark_consumer.log", "a"),
                    stderr=open("/opt/airflow/logs/spark_consumer_err.log", "a"),
                    preexec_fn=os.setsid
                )
                self.send_response(200)
                self.send_header('Content-Type', 'text/plain')
                self.end_headers()
                self.wfile.write(b"Spark consumer started successfully in background.")
            except Exception as e:
                self.send_response(500)
                self.send_header('Content-Type', 'text/plain')
                self.end_headers()
                self.wfile.write(f"Failed to start Spark consumer: {e}".encode('utf-8'))
                
        elif self.path == '/stop':
            if process and process.poll() is None:
                try:
                    # Kill the process group to terminate spark-submit and JVM child processes
                    os.killpg(os.getpgid(process.pid), signal.SIGTERM)
                    process = None
                    self.send_response(200)
                    self.send_header('Content-Type', 'text/plain')
                    self.end_headers()
                    self.wfile.write(b"Spark consumer stopped successfully.")
                except Exception as e:
                    self.send_response(500)
                    self.send_header('Content-Type', 'text/plain')
                    self.end_headers()
                    self.wfile.write(f"Failed to stop Spark consumer: {e}".encode('utf-8'))
            else:
                self.send_response(200)
                self.send_header('Content-Type', 'text/plain')
                self.end_headers()
                self.wfile.write(b"Spark consumer is not running.")
        else:
            self.send_response(404)
            self.end_headers()

def run(server_class=HTTPServer, handler_class=SparkAPIHandler, port=5000):
    os.makedirs("/opt/airflow/logs", exist_ok=True)
    server_address = ('', port)
    httpd = server_class(server_address, handler_class)
    print(f"Starting Spark API server on port {port}...")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()

if __name__ == '__main__':
    run()
