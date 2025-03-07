import os.path

from celery import Celery
from dateutil.parser import parse as du_parse
from xmrg_processing.boundariesparse import Boundary
from xmrg_processing.xmrg_process import xmrg_process
from xmrg_processing.csvdatasaver import nexrad_csv_saver
import logging
from email_results import email_results
import zipfile
from config import *
import glob

if os.getenv("REMOTE_DEBUG", False):
    import sys
    sys.path.append("./debug/pydevd-pycharm.egg")
    import pydevd_pycharm
    pydevd_pycharm.settrace('127.0.0.1',
                            port=4200,
                            stdoutToServer=True,
                            stderrToServer=True)

app = Celery("tasks",
             broker=f"pyamqp://{CELERY_USERNAME}:{CELERY_PASSWORD}@{CELERY_SERVER}//",
             backend='rpc://')
app.conf.worker_force_execv = True
#app.worker_main(['worker', '--loglevel=info', '--concurrency=4'])
#app.conf.update(worker_max_tasks_per_child=100,  worker_max_memory_per_child=4000)

SCRIPT_DIRECTORY = os.path.abspath(os.path.dirname(__file__))

def pre_process_boundary_file(task_path, boundary_filename, boundary_file, task_id):
    logger = logging.getLogger()
    try:
        boundary_directory = os.path.join(task_path, BOUNDARY_DIRECTORY)
        os.makedirs(boundary_directory)
        boundary_filepath = os.path.join(boundary_directory, boundary_filename)
        logger.info(f"{task_id} Saving boundary file to {boundary_filepath}")
        with open(boundary_filepath, 'wb') as output_file:
            output_file.write(boundary_file)
        try:
            #Figure out if the file is a zip file.
            with zipfile.ZipFile(boundary_filepath, 'r') as zip_ref:
                logger.info(f"{task_id} boundary file: {boundary_filepath} is zipped, unzipping to: {boundary_directory}")
                zip_ref.extractall(boundary_directory)
        except zipfile.BadZipFile:
            logger.info(f"{task_id} boundary file: {boundary_filepath} not zipped.")
        return boundary_directory
    except Exception as e:
        logger.exception(e)
    return None

def build_task_directories(task_path):
    #Make sure we don't have a re-used id.
    if os.path.exists(task_path):
        os.remove(task_path)
    #Create the processing directory based on the task_id.
    os.makedirs(task_path)
    #Create the data processing directory.
    data_directory = os.path.join(task_path, DATA_DIRECTORY)
    os.makedirs(data_directory)
    #Create the results directory
    result_directory = os.path.join(task_path, RESULTS_DIRECTORY)
    os.makedirs(result_directory)
    #Create the logfiles  directory.
    log_directory = os.path.join(task_path, LOGFILE_DIRECTORY)
    os.makedirs(log_directory)


@app.task(bind=True)
def xmrg_task(self,
              start_date: str,
              end_date: str,
              boundary_filename: str,
              boundary_file: bytes,
              email_address: str):
    task_id = self.request.id

    task_path = os.path.join(SCRIPT_DIRECTORY, REQUEST_DIRECTORY, task_id)
    build_task_directories(task_path)
    local_data_directory = os.path.join(task_path, DATA_DIRECTORY)
    result_directory = os.path.join(task_path, RESULTS_DIRECTORY)
    log_directory = os.path.join(task_path, LOGFILE_DIRECTORY)
    # Create a logger
    logger = logging.getLogger(f"xmrg_task_{task_id}")
    logger.setLevel(logging.DEBUG)

    # Create a file handler and set level to info
    file_handler = logging.FileHandler(os.path.join(log_directory, 'xmrg_task.log'))
    file_handler.setLevel(logging.DEBUG)

    # Create a logging format
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)

    # Add the handlers to the logger
    logger.addHandler(file_handler)

    process_start_date = du_parse(start_date)
    process_end_date = du_parse(end_date)
    logger.info(f"{task_id} Start Date: {process_start_date.strftime('%Y-%m-%d %H-%M-%S')}"
                f"End Date: {process_end_date.strftime('%Y-%m-%d %H-%M-%S')}")
    #Save the boundary file to the task specific path.
    try:
        valid_boundary_file = False
        boundary_filepath = pre_process_boundary_file(task_path, boundary_filename, boundary_file, task_id)
        if boundary_filepath is not None:
            boundaries = Boundary(unique_id=task_id)
            if boundaries.parse_boundaries_file(boundary_filepath):
                boundary_count = len(boundaries.boundaries)
                boundary_names = [bnd[0] for bnd in boundaries.boundaries]
                logger.info(f"{task_id} Boundaries parsed. Count: {boundary_count} Names: {boundary_names}")
                valid_boundary_file = True
            else:
                logger.error(f"{task_id} Boundary file: {boundary_filepath} not parsed")
    except Exception as e:
        logger.error(f"{task_id} Boundary file: {boundary_filepath} not parsed")
        logger.exception(e)
        email_files = []
        subject = "XMRG Results Error"
        message = (f"There appears to be an issue with your boundaries file.\n"
                   f"Please make sure your WKID is ESPG:4326 "
                   f"and you've provided a field in the file defined 'Name'. \n"
                   f"If you still have an issue, the task id for this run was: {task_id} provide this "
                   f"to: ramaged@mailbox.sc.edu")

    else:
        if valid_boundary_file:
            csv_saver = nexrad_csv_saver(result_directory)
            xmrg_proc = xmrg_process(
                data_saver=csv_saver,
                boundaries=boundaries.boundaries,
                worker_process_count=WORKER_COUNT,
                unique_id=task_id,
                source_file_working_directory=local_data_directory,
                output_directory=task_path,
                base_log_output_directory=log_directory,
                results_directory=result_directory,
                kml_output_directory=result_directory,
                save_all_precip_values=SAVE_ALL_PRECIP_VALUES,
                delete_source_file=DELETE_SOURCE_FILE,
                delete_compressed_source_file=DELETE_COMPRESSED_SOURCE_FILE)
            xmrg_proc.process(start_date=process_start_date, end_date=process_end_date,
                              base_xmrg_directory=XMRG_DATA_DIRECTORY)

            email_files = csv_saver.csv_filenames
            if ADD_DEBUG_FILES:
                debug_files_filter = os.path.join(result_directory, "*.json")
                files = glob.glob(debug_files_filter)
                email_files.extend(debug_files_filter)
            subject = "XMRG Results"
            message = f"Attached are your results for: {start_date} to {end_date}"

    send_email(task_id, email_address, subject, message, email_files)
    logger.info(f"{task_id} completed task.")
    return

def send_email(task_id, to_email: str, subject: str, message: str, attachments: []):
    '''
    :return:
    '''
    #Let's zip the files
    files_to_attach = []
    for result_file in attachments:
        directory, file_name = os.path.split(result_file)
        file_name, exten = os.path.splitext(file_name)
        zip_filepath = os.path.join(directory, f"{file_name}.zip")
        logger.info(f"{task_id} adding file: {zip_filepath} to the zip.")
        with zipfile.ZipFile(zip_filepath, 'w', compression=zipfile.ZIP_DEFLATED) as zip_ref:
            zip_ref.write(result_file, f"{file_name}{exten}")
            files_to_attach.append(zip_filepath)

    email_settings = {
        'host': MAILHOST,
        'username': USER,
        'password': PASSWORD,
        'use_tls': True,
        'port': PORT,
        'from_address': FROMADDR,
        'to_addresses': [to_email],
    }
    #(email_settings, subject, message, attachments):
    email_results(email_settings, subject, message, files_to_attach)