import os
import sys
sys.path.append(os.environ['SCTYS_PROJECT'] + '/sctys_global_parameters')
from global_parameters import Path
sys.path.append(Path.NOTIFIER_PROJECT)
sys.path.append(Path.UTILITIES_PROJECT)
from notifiers import get_notifier
from utilities_functions import convert_datetime_to_timestamp
from file_io import FileIO
import pandas as pd
import pickle
import json
import joblib
import s3fs
from aws_api_s3 import aws_api


s3 = s3fs.S3FileSystem(key=aws_api['id'], secret=aws_api['secret'])


class S3FileIO(FileIO):

    EMPTY_FILE_TAG = 'is_empty'

    def __init__(self, project, logger):
        super().__init__(project, logger)

    @ staticmethod
    def list_files_in_folder(path):
        file_list = s3.ls(path)
        file_list = [file.split('/')[-1] for file in file_list]
        return file_list

    @ classmethod
    def filter_non_empty_files(cls, path, file_list):
        file_list = [file for file in file_list
                     if not bool(s3.get_tags(os.path.join(path, file)).get(cls.EMPTY_FILE_TAG))]
        return file_list

    @ staticmethod
    def check_if_folder_exist(path):
        return s3.exists(path)

    @ staticmethod
    def create_directory_if_not_exist(path):
        if not s3.exists(path):
            s3.makedirs(path)

    @ staticmethod
    def check_if_file_exists(path, file_name):
        full_file_name = os.path.join(path, file_name)
        file_exist = s3.exists(full_file_name)
        return file_exist

    def check_modified_time(self, path, file_name=None):
        if self.check_if_folder_exist(path):
            file_list = s3.listdir(path)
            if file_name is not None:
                file_info = [file for file in file_list if file['Key'].split('/')[-1] == file_name]
                if len(file_info) == 0:
                    self.logger.info('{} not exist. Unable to get the modified time'.format(file_name))
                    return None
                else:
                    return file_info[0]['LastModified']
            else:
                return max([file['LastModified'] for file in file_list])
        else:
            self.logger.info('{} not exist. Unable to get the modified time'.format(path))
            return None

    def list_modified_files_after_time(self, path, cutoff_date_time):
        if not self.check_if_folder_exist(path):
            return []
        time_stamp = convert_datetime_to_timestamp(cutoff_date_time)
        file_list = s3.listdir(path)
        file_list = [file['Key'].split('/')[-1] for file in file_list if file['LastModified'] > time_stamp]
        return file_list

    def list_modified_files_between_time(self, path, cutoff_start_time, cutoff_end_time):
        if not self.check_if_folder_exist(path):
            return []
        start_time_stamp = convert_datetime_to_timestamp(cutoff_start_time)
        end_time_stamp = convert_datetime_to_timestamp(cutoff_end_time)
        file_list = s3.listdir(path)
        file_list = [file['Key'].split('/')[-1] for file in file_list
                     if start_time_stamp <= file['LastModified'] < end_time_stamp]
        return file_list

    def clear_temp_fail_file(self, save):
        if save:
            file_key = self.PREFIX_FAIL_SAVE_LIST
        else:
            file_key = self.PREFIX_FAIL_LOAD_LIST
        fail_file_list = self.list_files_in_folder(Path.TEMP_FOLDER)
        fail_file_list = [os.path.join(Path.TEMP_FOLDER, fail_file) for fail_file in fail_file_list
                          if file_key in fail_file]
        [s3.rm(fail_file) for fail_file in fail_file_list]

    def save_empty_file(self, path, file_name):
        full_path = os.path.join(path, file_name)
        self._save_txt_file('', full_path)
        s3.put_tags(full_path, {self.EMPTY_FILE_TAG, 'true'})

    @staticmethod
    def _save_binary_file(data, full_path, encoding='utf-8'):
        with s3.open(full_path, 'wb', encoding=encoding) as file:
            file.write(data)

    @staticmethod
    def _save_txt_file(data, full_path, encoding='utf-8'):
        with s3.open(full_path, 'w', encoding=encoding) as file:
            file.write(data)

    def _save_html_file(self, data, full_path, encoding='utf-8'):
        self._save_txt_file(data, full_path, encoding)

    def _save_json_file(self, data, full_path, encoding='utf-8'):
        if not isinstance(data, str):
            data = json.dumps(data)
        self._save_txt_file(data, full_path, encoding)

    @staticmethod
    def _save_joblib_file(data, full_path, **kwargs):
        with s3.open(full_path, 'wb') as file:
            joblib.dump(data, file, **kwargs)

    @staticmethod
    def _save_pickle_file(data, full_path, pandas=True, **kwargs):
        with s3.open(full_path, 'wb') as file:
            if pandas:
                data.to_pickle(file, **kwargs)
            else:
                pickle.dump(data, file, **kwargs)

    @staticmethod
    def _save_csv_file(data, full_path, **kwargs):
        with s3.open(full_path, 'w') as file:
            pd.to_csv(file, **kwargs)

    @staticmethod
    def _save_parquet_file(data, full_path, **kwargs):
        with s3.open(full_path, 'wb') as file:
            data.to_parquet(file, **kwargs)

    @staticmethod
    def _load_binary_file(full_path, encoding='utf-8'):
        with s3.open(full_path, 'rb', encoding=encoding) as file:
            data = file.read()
        return data

    @staticmethod
    def _load_txt_file(full_path, encoding='utf-8'):
        with s3.open(full_path, 'r', encoding=encoding) as file:
            data = file.read()
        return data

    def _load_html_file(self, full_path, encoding='utf-8'):
        data = self._load_txt_file(full_path, encoding)
        return data

    def _load_json_file(self, full_path, encoding='utf-8'):
        data = self._load_txt_file(full_path, encoding)
        data = json.loads(data)
        return data

    @staticmethod
    def _load_joblib_file(full_path, **kwargs):
        with s3.open(full_path, 'rb') as file:
            data = joblib.load(file, **kwargs)
        return data

    @staticmethod
    def _load_pickle_file(full_path, pandas=True, **kwargs):
        with s3.open(full_path, 'rb') as file:
            if pandas:
                data = pd.read_pickle(file, **kwargs)
            else:
                data = pickle.load(file, **kwargs)
        return data

    @staticmethod
    def _load_csv_file(full_path, **kwargs):
        with s3.open(full_path, 'w') as file:
            data = pd.read_csv(file, **kwargs)
        return data

    @staticmethod
    def _load_parquet_file(full_path, **kwargs):
        with s3.open(full_path, 'wb') as file:
            data = pd.read_parquet(file, **kwargs)
        return data

    @staticmethod
    def _load_excel_file(full_path, **kwargs):
        with s3.open(full_path, 'rb') as file:
            data = pd.read_excel(file, **kwargs)
        return data

    @staticmethod
    def download_file_from_s3(remote_path, local_path, remote_file_name, local_file_name=None):
        if local_file_name is None:
            local_file_name = remote_file_name
        remote_full_path = os.path.join(remote_path, remote_file_name)
        local_full_path = os.path.join(local_path, local_file_name)
        if not os.path.exists(local_path):
            os.makedirs(local_path)
        s3.get_file(remote_full_path, local_full_path)

    def download_list_of_files_from_s3_folder(self, remote_path, local_path, remote_file_list):
        remote_file_list = self.filter_non_empty_files(remote_path, remote_file_list)
        if not os.path.exists(local_path):
            os.makedirs(local_path)
        for remote_file in remote_file_list:
            self.download_file_from_s3(remote_path, local_path, remote_file)

    @staticmethod
    def upload_file_to_s3(local_path, remote_path, local_file_name, remote_file_name=None):
        if remote_file_name is None:
            remote_file_name = local_file_name
        local_full_path = os.path.join(local_path, local_file_name)
        remote_full_path = os.path.join(remote_path, remote_file_name)
        s3.put_file(local_full_path, remote_full_path)

    def upload_list_of_files_to_s3_folder(self, local_path, remote_path, local_file_list):
        for local_file in local_file_list:
            self.upload_file_to_s3(local_path, remote_path, local_file)

    @ staticmethod
    def verify_single_file_downloaded(local_path, local_file_name):
        full_file_name = os.path.join(local_path, local_file_name)
        file_exist = os.path.isfile(full_file_name)
        if file_exist:
            return {'ok': True}
        else:
            return {'ok': False, 'missed_files': full_file_name}

    @ staticmethod
    def verify_list_of_files_downloaded(local_path, remote_file_list):
        local_file_list = [os.path.join(local_path, local_file) for local_file in remote_file_list]
        missed_file_list = []
        for local_file in local_file_list:
            if not os.path.isfile(local_file):
                missed_file_list.append(local_file)
        if len(missed_file_list) == 0:
            return {'ok': True}
        else:
            return {'ok': False, 'missed_files': missed_file_list}

    @ staticmethod
    def verify_single_file_uploaded(remote_path, remote_file_name):
        full_remote_file_name = os.path.join(remote_path, remote_file_name)
        file_exist = s3.exists(full_remote_file_name)
        if file_exist:
            return {'ok': True}
        else:
            return {'ok': False, 'missed_files': os.path.join(remote_path, remote_file_name)}

    @ staticmethod
    def verify_list_of_files_uploaded(remote_path, local_file_list):
        remote_file_list = [os.path.join(remote_path, remote_file) for remote_file in local_file_list]
        missed_file_list = []
        for remote_file in remote_file_list:
            if not s3.exists(remote_file):
                missed_file_list.append(remote_file)
        if len(missed_file_list) == 0:
            return {'ok': True}
        else:
            return {'ok': False, 'missed_files': missed_file_list}

    @staticmethod
    def remove_file_from_s3(remote_path, remote_file_name):
        remote_full_path = os.path.join(remote_path, remote_file_name)
        s3.rm_file(remote_full_path)

    @staticmethod
    def remove_all_files_in_s3_folder(remote_path):
        s3.rm(remote_path, recursive=True)

    def clone_list_of_empty_files_to_s3(self, remote_path, local_file_list):
        for local_file in local_file_list:
            self.save_empty_file(remote_path, local_file)


