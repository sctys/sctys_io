import os
import sys
sys.path.append(os.environ['SCTYS_PROJECT'] + '/sctys_global_parameters')
from global_parameters import Path
sys.path.append(Path.NOTIFIER_PROJECT)
sys.path.append(Path.UTILITIES_PROJECT)
from notifiers import get_notifier
from utilities_functions import convert_datetime_to_timestamp
import pandas as pd
import pyarrow
import pickle
import json
import time
import joblib
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor


class FileIO(object):

    NOTIFIER = 'slack'
    PREFIX_FAIL_SAVE_LIST = 'fileio_fail_save_list_'
    PREFIX_FAIL_LOAD_LIST = 'fileio_fail_load_list_'

    def __init__(self, project, logger):
        self.project = project
        self.logger = logger
        self.notifier = None
        self.fail_save_list = []
        self.fail_load_list = []

    @ staticmethod
    def list_files_in_folder(path):
        return os.listdir(path)

    @ staticmethod
    def check_if_folder_exist(path):
        return os.path.exists(path)

    @ staticmethod
    def create_directory_if_not_exist(path):
        if not os.path.exists(path):
            os.makedirs(path)

    @staticmethod
    def check_if_file_exists(path, file_name):
        full_file_name = os.path.join(path, file_name)
        file_exist = os.path.isfile(full_file_name)
        return file_exist

    def check_modified_time(self, path, file_name=None):
        if file_name is not None:
            path_exist = self.check_if_file_exists(path, file_name)
        else:
            path_exist = self.check_if_folder_exist(path)
        if path_exist:
            if file_name is not None:
                path = os.path.join(path, file_name)
            return os.path.getmtime(path)
        else:
            self.logger.info('{} not exist. Unable to get the modified time'.format(path))
            return None

    def list_modified_files_after_time(self, path, cutoff_date_time):
        if not self.check_if_folder_exist(path):
            return []
        time_stamp = convert_datetime_to_timestamp(cutoff_date_time)
        file_list = [file for file in os.listdir(path) if self.check_modified_time(path, file) > time_stamp]
        return file_list

    def list_modified_files_between_time(self, path, cutoff_start_time, cutoff_end_time):
        if not self.check_if_folder_exist(path):
            return []
        start_time_stamp = convert_datetime_to_timestamp(cutoff_start_time)
        end_time_stamp = convert_datetime_to_timestamp(cutoff_end_time)
        file_list = [file for file in os.listdir(path)
                     if start_time_stamp <= self.check_modified_time(path, file) < end_time_stamp]
        return file_list

    def save_file(self, data, file_path, file_name, file_type, **kwargs):
        full_path = os.path.join(file_path, file_name)
        try:
            getattr(self, '_save_{}_file'.format(file_type))(data, full_path, **kwargs)
            self.logger.debug('{} saved.'.format(full_path))
        except Exception as e:
            self.logger.error('Error in saving file {}. {}'.format(full_path, e))
            self.fail_save_list.append({'data': data, 'file_path': file_path, 'file_name': file_name,
                                        'file_type': file_type, 'kwargs': kwargs})

    def load_file(self, file_path, file_name, file_type, **kwargs):
        full_path = os.path.join(file_path, file_name)
        try:
            data = getattr(self, '_load_{}_file'.format(file_type))(full_path, **kwargs)
            self.logger.debug('{} loaded'.format(full_path))
        except Exception as e:
            data = None
            self.logger.error('Error in loading file {}. {}'.format(full_path, e))
            self.fail_load_list.append({'file_path': file_path, 'file_name': file_name,
                                        'file_type': file_type, 'kwargs': kwargs})
        return data

    def clear_fail_save_list(self):
        self.fail_save_list = []

    def clear_fail_load_list(self):
        self.fail_load_list = []

    def get_notifier(self):
        self.notifier = get_notifier(self.NOTIFIER, self.project, self.logger)

    '''
    def save_multiple_files(self, mode, data_list, file_path, file_name_list, file_type, verbose=False, **kwargs):
        self.clear_fail_save_list()
        if mode == 'normal':
            if isinstance(file_path, list):
                list(map(lambda data, path, file_name: self.save_file(
                    data, path, file_name, file_type, verbose, **kwargs), data_list, file_path, file_name_list))
            else:
                list(map(lambda data, file_name: self.save_file(
                    data, file_path, file_name, file_type, verbose, **kwargs), data_list, file_name_list))
        elif mode == 'thread':
            if isinstance(file_path, list):
                with ThreadPoolExecutor(max_workers=self.FILEIO_NO_WORKERS) as executor:
                    executor.map(lambda data, path, file_name: self.save_file(
                        data, path, file_name, file_type, verbose, **kwargs), data_list, file_path, file_name_list)
            else:
                with ThreadPoolExecutor(max_workers=self.FILEIO_NO_WORKERS) as executor:
                    executor.map(lambda data, file_name: self.save_file(
                        data, file_path, file_name, file_type, verbose, **kwargs), data_list, file_name_list)
        elif mode == 'process':
            if isinstance(file_path, list):
                with ProcessPoolExecutor(max_workers=self.FILEIO_NO_WORKERS) as executor:
                    executor.map(lambda data, path, file_name: self.save_file(
                        data, path, file_name, file_type, verbose, **kwargs), data_list, file_path, file_name_list)
            else:
                with ProcessPoolExecutor(max_workers=self.FILEIO_NO_WORKERS) as executor:
                    executor.map(lambda data, file_name: self.save_file(
                        data, file_path, file_name, file_type, verbose, **kwargs), data_list, file_name_list)
        self.notify_fail_file(True)
        self.save_fail_save_list()

    def load_multiple_files(self, mode, file_path, file_name_list, file_type, verbose=False, **kwargs):
        data = []
        self.clear_fail_load_list()
        if mode == 'normal':
            if isinstance(file_path, list):
                data = list(map(lambda path, file_name: self.load_file(
                            path, file_name, file_type, verbose, **kwargs), file_path, file_name_list))
            else:
                data = list(map(lambda file_name: self.load_file(
                            file_path, file_name, file_type, verbose, **kwargs), file_name_list))
        elif mode == 'thread':
            if isinstance(file_path, list):
                with ThreadPoolExecutor(max_workers=self.FILEIO_NO_WORKERS) as executor:
                    data = list(executor.map(lambda path, file_name: self.load_file(
                                        path, file_name, file_type, verbose, **kwargs), file_path, file_name_list))
            else:
                with ThreadPoolExecutor(max_workers=self.FILEIO_NO_WORKERS) as executor:
                    data = list(executor.map(lambda file_name: self.load_file(
                                        file_path, file_name, file_type, verbose, **kwargs), file_name_list))
        elif mode == 'process':
            if isinstance(file_path, list):
                with ProcessPoolExecutor(max_workers=self.FILEIO_NO_WORKERS) as executor:
                    data = list(executor.map(lambda path, file_name: self.load_file(
                                        path, file_name, file_type, verbose, **kwargs), file_path, file_name_list))
            else:
                with ProcessPoolExecutor(max_workers=self.FILEIO_NO_WORKERS) as executor:
                    data = list(executor.map(lambda file_name: self.load_file(
                                        file_path, file_name, file_type, verbose, **kwargs), file_name_list))
        self.notify_fail_file(False)
        self.save_fail_load_list()
        return data
    '''

    def notify_fail_file(self, save):
        if self.notifier is None:
            self.get_notifier()
        if save:
            operation = 'saved'
            fail_list_files = [os.path.join(fail['file_path'], fail['file_name']) for fail in self.fail_save_list]
        else:
            operation = 'loaded'
            fail_list_files = [os.path.join(fail['file_path'], fail['file_name']) for fail in self.fail_load_list]
        fail_list_str = '\n'.join(fail_list_files)
        if len(fail_list_str) > 0:
            message = 'The following files were not {} successfully:\n\n'.format(operation) + fail_list_str
            self.notifier.retry_send_message(message)

    def save_fail_save_list(self):
        if len(self.fail_save_list) > 0:
            file_name = self.PREFIX_FAIL_SAVE_LIST + '{}.txt'.format(int(time.time()))
            self.save_file(self.fail_save_list, Path.TEMP_FOLDER, file_name, 'txt')

    def save_fail_load_list(self):
        if len(self.fail_load_list):
            file_name = self.PREFIX_FAIL_LOAD_LIST + '{}.txt'.format(int(time.time()))
            self.save_file(self.fail_load_list, Path.TEMP_FOLDER, file_name, 'txt')

    '''
    def load_fail_save_list(self, file_name):
        self.fail_save_list = self.load_file(TEMP_PATH, file_name, 'joblib')

    def load_fail_load_list(self, file_name):
        self.fail_load_list = self.load_file(TEMP_PATH, file_name, 'joblib')

    def retry_save_multiple_files(self, mode, file_name=None):
        if file_name is not None:
            self.load_fail_save_list(file_name)
        fail_save_list = [(fail['data'], fail['file_path'], fail['file_name'], fail['file_type'], fail['verbose'],
                           fail['kwargs']) for fail in self.fail_save_list]
        data_list, file_path, file_name_list, file_type, verbose, kwargs = \
            ([fail[index] for fail in fail_save_list] for index in range(6))
        unique_file_path = set(file_path)
        file_path = list(unique_file_path)[0] if len(unique_file_path) == 1 else file_path
        file_type = file_type[0]
        verbose = verbose[0]
        kwargs = kwargs[0]
        self.save_multiple_files(mode, data_list, file_path, file_name_list, file_type, verbose, **kwargs)

    def retry_load_multiple_files(self, mode, file_name=None):
        if file_name is not None:
            self.load_fail_load_list(file_name)
        fail_load_list = [(fail['file_path'], fail['file_name'], fail['file_type'], fail['verbose'], fail['kwargs'])
                          for fail in self.fail_load_list]
        file_path, file_name_list, file_type, verbose, kwargs = \
            ([fail[index] for fail in fail_load_list] for index in range(5))
        unique_file_path = set(file_path)
        file_path = list(unique_file_path)[0] if len(unique_file_path) == 1 else file_path
        file_type = file_type[0]
        verbose = verbose[0]
        kwargs = kwargs[0]
        data = self.load_multiple_files(mode, file_path, file_name_list, file_type, verbose, **kwargs)
        return data
    '''

    def clear_temp_fail_file(self, save):
        if save:
            file_key = self.PREFIX_FAIL_SAVE_LIST
        else:
            file_key = self.PREFIX_FAIL_LOAD_LIST
        fail_file_list = os.listdir(Path.TEMP_FOLDER)
        fail_file_list = [os.path.join(Path.TEMP_FOLDER, fail_file) for fail_file in fail_file_list
                          if file_key in fail_file]
        [os.remove(fail_file) for fail_file in fail_file_list]

    @ staticmethod
    def _save_binary_file(data, full_path, encoding='utf-8'):
        with open(full_path, 'wb', encoding=encoding) as file:
            file.write(data)

    def _save_html_file(self, data, full_path, encoding='utf-8'):
        self._save_txt_file(data, full_path, encoding)

    def _save_json_file(self, data, full_path, encoding='utf-8'):
        if not isinstance(data, str):
            data = json.dumps(data)
        self._save_txt_file(data, full_path, encoding)

    @ staticmethod
    def _save_txt_file(data, full_path, encoding='utf-8'):
        with open(full_path, 'w', encoding=encoding) as file:
            file.write(data)

    @ staticmethod
    def _save_joblib_file(data, full_path, **kwargs):
        joblib.dump(data, full_path, **kwargs)

    @ staticmethod
    def _save_pickle_file(data, full_path, pandas=True, **kwargs):
        if pandas:
            data.to_pickle(full_path, **kwargs)
        else:
            with open(full_path, 'wb') as file:
                pickle.dump(data, file, **kwargs)

    @ staticmethod
    def _save_csv_file(data, full_path, **kwargs):
        data.to_csv(full_path, **kwargs)

    @ staticmethod
    def _save_parquet_file(data, full_path, **kwargs):
        data.to_parquet(full_path, **kwargs)

    @ staticmethod
    def _load_binary_file(full_path, encoding='utf-8'):
        with open(full_path, 'rb', encoding=encoding) as file:
            data = file.read()
        return data

    def _load_html_file(self, full_path, encoding='utf-8'):
        data = self._load_txt_file(full_path, encoding)
        return data

    def _load_json_file(self, full_path, encoding='utf-8'):
        data = self._load_txt_file(full_path, encoding)
        data = json.loads(data)
        return data

    @ staticmethod
    def _load_txt_file(full_path, encoding='utf-8'):
        with open(full_path, 'r', encoding=encoding) as file:
            data = file.read()
        return data

    @ staticmethod
    def _load_joblib_file(full_path, **kwargs):
        data = joblib.load(full_path, **kwargs)
        return data

    @ staticmethod
    def _load_pickle_file(full_path, pandas=True, **kwargs):
        if pandas:
            data = pd.read_pickle(full_path, **kwargs)
        else:
            with open(full_path, 'rb') as file:
                data = pickle.load(file, **kwargs)
        return data

    @ staticmethod
    def _load_csv_file(full_path, **kwargs):
        data = pd.read_csv(full_path, **kwargs)
        return data

    @ staticmethod
    def _load_parquet_file(full_path, **kwargs):
        try:
            data = pyarrow.parquet.ParquetDataset(full_path).read().to_pandas()
        except Exception as e:
            data = pd.read_parquet(full_path)
        return data

    @ staticmethod
    def _load_excel_file(full_path, **kwargs):
        data = pd.read_excel(full_path, **kwargs)
        return data
