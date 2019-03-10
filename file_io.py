import os
from io_setting import NOTIFIER_PATH, TEMP_PATH, FileIOSetting
from io_utilities import set_logger
import sys
import numpy as np
import pandas as pd
import pickle
import json
import time
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
        data = []
        if mode == 'normal':
            if isinstance(file_path, list):
                data = list(map(lambda path, file_name: self.load_file(
                            path, file_name, file_type, verbose, **kwargs), file_path, file_name_list))
            else:
                data = list(map(lambda file_name: self.load_file(
                            file_name, file_type, verbose, **kwargs), file_name_list))
        elif mode == 'thread':
            if isinstance(file_path, list):
                with ThreadPoolExecutor(max_workers=self.FILEIO_NO_WORKERS) as executor:
                    data = executor.map(lambda path, file_name: self.load_file(
                                        path, file_name, file_type, verbose, **kwargs), file_path, file_name_list)
            else:
                with ThreadPoolExecutor(max_workers=self.FILEIO_NO_WORKERS) as executor:
                    data = executor.map(lambda file_name: self.load_file(
                                        file_path, file_name, file_type, verbose, **kwargs), file_name_list)
        elif mode == 'process':
            if isinstance(file_path, list):
                with ProcessPoolExecutor(max_workers=self.FILEIO_NO_WORKERS) as executor:
                    data = executor.map(lambda path, file_name: self.load_file(
                                        path, file_name, file_type, verbose, **kwargs), file_path, file_name_list)
            else:
                with ProcessPoolExecutor(max_workers=self.FILEIO_NO_WORKERS) as executor:
                    data = executor.map(lambda file_name: self.load_file(
                                        file_name, file_type, verbose, **kwargs), file_name_list)
        return data

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

    def save_fail_save_list(self):
        file_name = 'fileio_fail_save_list_{}.pkl'.format(int(time.time()))
        self.save_file(self.fail_save_list, TEMP_PATH, file_name, 'joblib')

    def load_fail_save_list(self, file_name):
        self.fail_save_list = self.load_file(TEMP_PATH, file_name, 'joblib')

    def save_fail_load_list(self):
        file_name = 'fileio_fail_load_list_{}.pkl'.format(int(time.time()))
        self.save_file(self.fail_load_list, TEMP_PATH, file_name, 'joblib')

    def load_fail_load_list(self, file_name):
        self.fail_load_list = self.load_file(TEMP_PATH, file_name, 'joblib')

    def retry_save_multiple_files(self, mode, file_name=None):
        if file_name is not None:
            self.load_fail_save_list(file_name)
        fail_save_list = [(fail['data'], fail['file_path'], fail['file_name'], fail['file_type'], fail['verbose'],
                           fail['kwargs']) for fail in self.fail_save_list]
        data_list, file_path, file_name_list, file_type, verbose, kwargs = \
            ([fail[index] for fail in fail_save_list] for index in range(5))
        unique_file_path = list(set(file_path))
        file_path = unique_file_path[0] if len(unique_file_path) == 1 else file_path
        file_type = list(set(file_type))[0]
        verbose = list(set(verbose))[0]
        kwargs = list(set(kwargs))[0]
        self.save_multiple_files(mode, data_list, file_path, file_name_list, file_type, verbose, **kwargs)

    def retry_load_multiple_files(self, mode, file_name=None):
        if file_name is not None:
            self.load_fail_load_list(file_name)
        fail_load_list = [(fail['file_path'], fail['file_name'], fail['file_type'], fail['verbose'], fail['kwargs'])
                          for fail in self.fail_load_list]
        file_path, file_name_list, file_type, verbose, kwargs = \
            ([fail[index] for fail in fail_load_list] for index in range(4))
        unique_file_path = list(set(file_path))
        file_path = unique_file_path[0] if len(unique_file_path) == 1 else file_path
        file_type = list(set(file_type))[0]
        verbose = list(set(verbose))[0]
        kwargs = list(set(kwargs))[0]
        data = self.load_multiple_files(mode, file_path, file_type, verbose, **kwargs)
        return data

    def clear_temp_fail_file(self, save):
        if save:
            file_key = 'fileio_fail_save_list_'
        else:
            file_key = 'fileio_fail_load_list_'
        fail_file_list = os.listdir(TEMP_PATH)
        fail_file_list = [os.path.join(TEMP_PATH, fail_file) for fail_file in fail_file_list
                               if file_key in fail_file]
        [os.remove(fail_file) for fail_file in fail_file_list]

    @ staticmethod
    def _save_binary_file(data, full_path, encoding='utf-8'):
        with open(full_path, 'wb', encoding=encoding) as file:
            file.write(data)

    def _save_html_file(self, data, full_path, encoding='utf-8'):
        self._save_binary_file(data, full_path, encoding)

    def _save_json_file(self, data, full_path, encoding='utf-8'):
        data = json.dumps(data)
        self._save_binary_file(data, full_path, encoding)

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
        data.to_hdf(full_path, 'df', **kwargs)

    @ staticmethod
    def _load_binary_file(full_path, encoding='utf-8'):
        with open(full_path, 'rb', encoding=encoding) as file:
            data = file.read()
        return data

    def _load_html_file(self, full_path, encoding='utf-8'):
        data = self._load_html_file(full_path, encoding)
        return data

    def _load_json_file(self, full_path, encoding='utf-8'):
        data = self._load_binary_file(full_path, encoding)
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
        data = pd.read_hdf(full_path, 'df', **kwargs)
        return data


def test_load_save_file(file_name, file_type):
    file_path = os.getcwd()
    io = FileIO()
    print('{} load: {}\n'.format(file_type, file_name))
    data = io.load_file(file_path, file_name, file_type)
    print(data)
    file_name = file_name.replace('1', '2')
    print('{} save: {}\n'.format(file_type, file_name))
    io.save_file(data, file_path, file_name, file_type)


def test_load_save_multiple_files(file_name_list, file_type):
    file_path = os.getcwd()
    io = FileIO()
    print('{} load: {}\n'.format(file_type, file_name_list))
    data = io.load_multiple_files('thread', file_path, file_name_list, file_type)
    print(data)
    print('fail_load_list: {}'.format(io.fail_load_list))
    file_name_list = [file_name.replace('1', '3') for file_name in file_name_list]
    file_name_list = [file_name.replace('2', '4') for file_name in file_name_list]
    print('{} save: {}\n'.format(file_type, file_name_list))
    io.save_multiple_files('thread', data, file_path, file_name_list, file_type)
    print('fail_save_list: {}'.format(io.fail_save_list))


def test_fail_list(fail_file_name_list, file_type):
    file_path = os.getcwd()
    io = FileIO()
    io.fail_load_list = [{'file_path': file_path, 'file_name': fail_file, 'file_type': file_type, 'verbose': False,
                          'kwargs': {}} for fail_file in fail_file_name_list]
    print('fail_load_list: {}'.format(io.fail_load_list))
    io.save_fail_load_list()
    print('Clear fail_load_list:')
    io.clear_fail_load_list()
    print('fail_load_list: {}'.format(io.fail_load_list))
    fail_file_name = os.listdir(TEMP_PATH)
    fail_file_name = [file_name for file_name in fail_file_name if 'fileio_fail_load_list_' in file_name][-1]
    print('fail_load_file_name: {}'.format(fail_file_name))
    io.load_fail_load_list(fail_file_name)
    print('fail_load_list: {}'.format(io.fail_load_list))
    io.notify_fail_file(False)
    data_list = io.retry_load_multiple_files('thread')
    io.clear_temp_fail_file(False)
    io.fail_save_list = [{'data': data, 'file_path': file_path, 'file_name': fail_file, 'file_type': file_type,
                          'verbose': False, 'kwargs': {}} for data, fail_file in zip(data_list, fail_file_name_list)]
    print('fail_save_list: {}'.format(io.fail_save_list))
    io.save_fail_save_list()
    print('Clear fail_save_list:')
    io.clear_fail_save_list()
    print('fail_save_list: {}'.format(io.fail_save_list))
    fail_file_name = os.listdir(TEMP_PATH)
    fail_file_name = [file_name for file_name in fail_file_name if 'fileio_fail_save_list_' in file_name][-1]
    print('fail_save_file_name: {}'.format(fail_file_name))
    io.load_fail_save_list(fail_file_name)
    print('fail_save_list: {}'.format(io.fail_save_list))
    io.notify_fail_file(True)
    io.retry_save_multiple_files('thread')
    io.clear_temp_fail_file(True)


if __name__ == '__main__':
    run_test_load_save_file = True
    run_test_load_save_multiple_file = True
    run_test_file_list = True
    if run_test_load_save_file:
        test_load_save_file('test_html_file_1.html', 'html')
        test_load_save_file('test_json_file_1.json', 'json')
        test_load_save_file('test_txt_file_1.txt', 'txt')
        test_load_save_file('test_pickle_file_1.pkl', 'pickle')
        test_load_save_file('test_joblib_file_1.pkl', 'joblib')
        test_load_save_file('test_csv_file_1.csv', 'csv')
        test_load_save_file('test_hdf_file_1.hdf', 'hdf')
    elif run_test_load_save_multiple_file:
        test_load_save_multiple_files(['test_html_file_1.html', 'test_html_file_2.html'], 'html')
        test_load_save_multiple_files(['test_json_file_1.json', 'test_json_file_2.json'], 'json')
        test_load_save_multiple_files(['test_txt_file_1.txt', 'test_txt_file_2.txt'], 'txt')
        test_load_save_multiple_files(['test_pickle_file_1.pkl', 'test_pickle_file_2.pkl'], 'pickle')
        test_load_save_multiple_files(['test_joblib_file_1.pkl', 'test_joblib_file_2.pkl'], 'joblib')
        test_load_save_multiple_files(['test_csv_file_1.csv', 'test_csv_file_2.csv'], 'csv')
        test_load_save_multiple_files(['test_hdf_file_1.hdf', 'test_hdf_file_2.hdf'], 'hdf')
    elif run_test_file_list:
        test_fail_list(['test_html_file_1.html', 'test_html_file_2.html', 'test_html_file_3.html',
                        'test_html_file_4.html'], 'html')
        test_fail_list(['test_json_file_1.json', 'test_json_file_2.json', 'test_json_file_3.json',
                        'test_json_file_4.json'], 'json')
        test_fail_list(['test_txt_file_1.txt', 'test_txt_file_2.txt', 'test_txt_file_3.txt',
                        'test_txt_file_4.txt'], 'txt')
        test_fail_list(['test_pickle_file_1.pkl', 'test_pickle_file_2.pkl', 'test_pickle_file_3.pkl',
                        'test_pickle_file_4.pkl'], 'pickle')
        test_fail_list(['test_joblib_file_1.pkl', 'test_joblib_file_2.pkl', 'test_joblib_file_3.pkl',
                        'test_joblib_file_4.pkl'], 'joblib')
        test_fail_list(['test_csv_file_1.csv', 'test_csv_file_2.csv', 'test_csv_file_3.csv',
                        'test_csv_file_4.csv'], 'csv')
        test_fail_list(['test_hdf_file_1.hdf', 'test_hdf_file_2.hdf', 'test_hdf_file_3.hdf',
                        'test_hdf_file_4.hdf'], 'hdf')
