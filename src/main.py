from batch_processing.batch_processing import BatchProcessor
from threading import Thread
from webserver.app import app

if __name__ == '__main__':
    batch_processor = BatchProcessor()
    bp_thread = Thread(target=batch_processor.start, daemon=True)
    bp_thread.start()
    app.run(host="localhost", port="5000")
