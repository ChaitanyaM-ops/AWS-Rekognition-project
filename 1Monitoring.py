#This script monitors a folder for the upload of the reference image using the Watchdog library. https://pypi.org/project/watchdog/

import time
import subprocess
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

#This class handles the monitoring aspect and target directory
class Watcher:
    
    #The target directory to monitor
    DIRECTORY_TO_WATCH = "/home/ubuntu/bead-project-2020/reference_image"
    
    def __init__(self):
        self.observer = Observer()

    def run(self):
        print("Listening for Lambda notification...")
        event_handler = Handler()
        self.observer.schedule(event_handler, self.DIRECTORY_TO_WATCH, recursive=True)
        self.observer.start()
        try:
            while True:
                time.sleep(5)
        except:
            self.observer.stop()
            print("Error")

        self.observer.join()


#This class handles the monitoring aspect and target directory
class Handler(FileSystemEventHandler):

    # Pointing the event monitor to the directory
    @staticmethod
    def on_any_event(event):
        if event.is_directory:
            return None

        # Take this action here when a file is created.
        elif event.event_type == 'created':
            # Take any action here when a file is created.
            print("Reference file upload detected.")
            print("Starting Spark Streaming script to download tweets.")
            subprocess.call(['python', '3sampled_stream.py'])
            subprocess.call(['python', '4receive_spark_stream.py'])

#Run everyting
if __name__ == '__main__':
    w = Watcher()
    w.run()
