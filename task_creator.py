import os.path

from celery import Celery
from config import *

app = Celery("tasks", broker=f"pyamqp://{CELERY_USERNAME}:{CELERY_PASSWORD}@{CELERY_SERVER}//")

start_date = "2025-02-01"
end_date = "2025-02-20"
email_address="ramaged@mailbox.sc.edu"
boundary_file_path = '/Users/danramage/Documents/workspace/WaterQuality/XMRGTask/XMRGTask/data/BadPoly1_Charlotte.zip'
#boundary_file_path = '/Users/danramage/Documents/workspace/WaterQuality/XMRGTask/XMRGTask/data/XMRGAreas.json'
#boundary_file_path = '/Users/danramage/Documents/workspace/WaterQuality/XMRGTask/XMRGTask/data/boundaries.csv'
# Read the file and convert to JSON string
with open(boundary_file_path
        , 'rb') as file:
    file_content = file.read()

boundary_directory, boundary_filename = os.path.split(boundary_file_path)
# Send the task along with file content to the queue
result = app.send_task('xmrg_celery_app.xmrg_task',
                       args=[start_date, end_date, boundary_filename, file_content, email_address] )

# Access the unique task ID
task_id = result.id
print(f'Task ID: {task_id}')

# Wait for the result and print it
print(f'Task Result: {result.get()}')
