import os
from io_setting import NOTIFIER_PATH, FileIOSetting
from io_utilities import set_logger
import sys
import pandas as pd
import pickle
import json
import joblib
import pickle
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
sys.path.append(NOTIFIER_PATH)
from notifiers import send_message


class FileIO(FileIOSetting):

    def __init__(self):
        self.logger = None
        self.set_logger()
        self.fail_save_list = []
        self.fail_load_list = []

    def set_logger(self):
        self.logger = set_logger(self.FILEIO_LOGGER_PATH, self.FILEIO_LOGGER_FILE, self.FILEIO_LOGGER_LEVEL, __name__)

    def save_file(self, data, file_path, file_name, file_type, verbose=False, **kwargs):
        full_path = os.path.join(file_path, file_name)
        try:
            getattr(self, '_save_{}_file'.format(file_type))(data, full_path, **kwargs)
            if verbose:
                self.logger.debug('{} saved.'.format(full_path))
        except Exception as e:
            self.logger.error('Error in saving file {}. {}'.format(full_path, e))
            self.fail_save_list.append({'data': data, 'file_path': file_path, 'file_name': file_name,
                                        'file_type': file_type, 'verbose': verbose, 'kwargs': kwargs})

    def load_file(self, file_path, file_name, file_type, verbose=False, **kwargs):
        full_path = os.path.join(file_path, file_name)
        try:
            data = getattr(self, '_load_{}_file'.format(file_type))(full_path, **kwargs)
            if verbose:
                self.logger.debug('{} loaded'.format(full_path))
        except Exception as e:
            data = None
            self.logger.error('Error in loading file {}. {}'.format(full_path, e))
            self.fail_load_list.append({'file_path': file_path, 'file_name': file_name,
                                        'file_type': file_type, 'verbose': verbose, 'kwargs': kwargs})
        return data

    def clear_fail_save_list(self):
        self.fail_save_list = []

    def clear_fail_load_list(self):
        self.fail_load_list = []

    def save_multiple_files(self, mode, data_list, file_path, file_name_list, file_type, verbose=False, **kwargs):
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

    def load_multiple_files(self, mode, file_path, file_name_list, file_type, verbose=False, **kwargs):
        if mode == 'normal':
            if isinstance(file_path, list):
                list(map(lambda path, file_name: self.load_file(
                    path, file_name, file_type, verbose, **kwargs), file_path, file_name_list))
            else:
                list(map(lambda file_name: self.load_file(
                    file_name, file_type, verbose, **kwargs), file_name_list))
        elif mode == 'thread':
            if isinstance(file_path, list):
                with ThreadPoolExecutor(max_workers=self.FILEIO_NO_WORKERS) as executor:
                    executor.map(lambda path, file_name: self.load_file(
                        path, file_name, file_type, verbose, **kwargs), file_path, file_name_list)
            else:
                with ThreadPoolExecutor(max_workers=self.FILEIO_NO_WORKERS) as executor:
                    executor.map(lambda file_name: self.load_file(
                        file_path, file_name, file_type, verbose, **kwargs), file_name_list)
        elif mode == 'process':
            if isinstance(file_path, list):
                with ProcessPoolExecutor(max_workers=self.FILEIO_NO_WORKERS) as executor:
                    executor.map(lambda path, file_name: self.load_file(
                        path, file_name, file_type, verbose, **kwargs), file_path, file_name_list)
            else:
                with ProcessPoolExecutor(max_workers=self.FILEIO_NO_WORKERS) as executor:
                    executor.map(lambda file_name: self.load_file(
                        file_name, file_type, verbose, **kwargs), file_name_list)

    def notify_fail_file(self, save):
        if save:
            operation = 'saved'
            fail_list_paths = [fail['full_path'] for fail in self.fail_save_list]
        else:
            operation = 'loaded'
            fail_list_paths = [fail['full_path'] for fail in self.fail_load_list]
        fail_list_str = '\n'.join(fail_list_paths)
        message = 'The following files were not {} successfully:\n\n'.format(operation) + fail_list_str
        send_message(message, self.FILEIO_NOTIFIER)


    @ staticmethod
    def _save_binary_file(data, full_path, encoding='utf-8'):
        with open(full_path, 'wb', encoding=encoding) as file:
            file.write(data)

    @ staticmethod
    def _save_txt_file(data, full_path, encoding='utf-8'):
        with open(full_path, 'w', encoding=encoding) as file:
            file.write(data)

    @ staticmethod
    def _save_joblib_file(data, full_path, **kwargs):
        joblib.dump(data, full_path, **kwargs)

    @ staticmethod
    def _save_pickle_file(data, full_path, **kwargs):
        with open(full_path, 'wb') as file:
            pickle.dump(data, file, **kwargs)

    @ staticmethod
    def _save_csv_file(data, full_path, **kwargs):
        data.to_csv(full_path, **kwargs)

    @ staticmethod
    def _save_hdf_file(data, full_path, **kwargs):
        data.to_hdf(full_path, **kwargs)

    @ staticmethod
    def _load_binary_file(full_path, encoding='utf-8'):
        with open(full_path, 'rb', encoding=encoding) as file:
            data = file.read()
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
    def _load_pickle_file(full_path, **kwargs):
        with open(full_path, 'rb') as file:
            data = pickle.load(file, **kwargs)
        return data

    @ staticmethod
    def _load_csv_file(full_path, **kwargs):
        data = pd.read_csv(full_path, **kwargs)
        return data

    @ staticmethod
    def _load_hdf_file(full_path, **kwargs):
        data = pd.read_hdf(full_path, **kwargs)
        return data


