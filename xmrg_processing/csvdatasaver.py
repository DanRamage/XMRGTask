import os
import logging

from xmrgprocessing.xmrg_results import xmrg_results
from xmrgprocessing.xmrgdatasaver.nexrad_data_saver import precipitation_saver
from datetime import datetime
from pandas import read_csv

class nexrad_csv_saver(precipitation_saver):
    def __init__(self, output_directory: str):
        self._logger = logging.getLogger()
        self._ouput_directory = output_directory
        self._new_records_added = 0
        self._boundary_output_files = {}
        self._now_date_time = datetime.now()
        self._finalized_filenames = []



    @property
    def new_records_added(self):
        return self._new_records_added

    @property
    def csv_filenames(self):
        return self._finalized_filenames

    def save(self, xmrg_results_data: xmrg_results):
        '''
        Saves the xmrg_results_data to the CSV output file.
        :param xmrg_results_data:
        :return:
        '''
        for boundary_name, boundary_results in xmrg_results_data.get_boundary_data():
            #Check to see if we have opened the output file for the data for each boundary.
            if boundary_name not in self._boundary_output_files:
                output_filename = os.path.join(self._ouput_directory,
                                               f"{boundary_name.replace(' ', '')}_"
                                               f"{self._now_date_time.strftime('%Y-%m-%d_%H%M%S')}_unsorted.csv")
                try:
                    file_obj = open(output_filename, "w")
                    self._boundary_output_files[boundary_name] = file_obj

                    file_obj.write("Name,Date,Precipition\n")
                except Exception as e:
                    self._logger.exception(e)
            try:
                file_obj = self._boundary_output_files[boundary_name]
                avg = boundary_results['weighted_average']
                file_obj.write(f"{boundary_name},{xmrg_results_data.datetime},{avg}\n")
            except Exception as e:
                self._logger.exception(e)
        return

    def finalize(self):
        """
        This function is for us to clean up before the script exits.
        :return:
        """
        #We need to make sure the files are sorted by the data.
        for boundary_name in self._boundary_output_files:
            try:
                file_obj = self._boundary_output_files[boundary_name]
                self._logger.info(f"Closing file object for file: {file_obj.name}")
                file_obj.close()
            except Exception as e:
                self._logger.exception(e)
        for boundary_name in self._boundary_output_files:
            try:
                file_obj = self._boundary_output_files[boundary_name]
                directory, filename = os.path.split(file_obj.name)
                filename = filename.replace("_unsorted.csv", ".csv")
                self._logger.info(f"Sorting file file: {file_obj.name} into file: {filename}")
                pd_df = read_csv(file_obj.name)
                sorted_df = pd_df.sort_values(by='Date')
                final_filename = os.path.join(directory, filename)
                sorted_df.to_csv(final_filename, index=False)
                self._finalized_filenames.append(final_filename)
                self._logger.info(f"Deleting temp file: {file_obj.name}")
                os.remove(file_obj.name)
            except Exception as e:
                self._logger.exception(e)

