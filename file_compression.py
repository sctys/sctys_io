import os
import sys
sys.path.append(os.environ['SCTYS_PROJECT'] + '/sctys_global_parameters')
from global_parameters import Path
sys.path.append(Path.NOTIFIER_PROJECT)
sys.path.append(Path.UTILITIES_PROJECT)
from notifiers import get_notifier
from utilities_functions import convert_datetime_to_timestamp
import tarfile
import tqdm


class FileCompress(object):

    def __init__(self, project, logger):
        self.project = project
        self.logger = logger

    @ staticmethod
    def set_compression_format(compression_format):
        return ':{}'.format(compression_format)

    @ staticmethod
    def get_compression_format_from_file_name(compressed_file_name):
        return compressed_file_name.split('.')[-1]

    @ staticmethod
    def dummy_folder_file_filter(file_path, file_name=None):
        return True

    def get_filtered_file_folder_list(self, file_path, is_child_file=True, folder_file_filter_func=None):
        if folder_file_filter_func is None:
            folder_file_filter_func = self.dummy_folder_file_filter
        file_folder_list = os.listdir(file_path)
        if is_child_file:
            file_folder_list = [
                file_name
                for file_name in file_folder_list if folder_file_filter_func(file_path, file_name)
            ]
        else:
            file_folder_list = [
                folder
                for folder in file_folder_list if folder_file_filter_func(os.path.join(file_path, folder))
            ]
        return file_folder_list

    def compress_single_file(self, file_path, file_name, compressed_file_name):
        if not os.path.isfile(os.path.join(file_path, file_name)):
            self.logger.error('{} not exist. Unable to compress the file.'.format(os.path.join(file_path, file_name)))
        compression_format = self.get_compression_format_from_file_name(compressed_file_name)
        format_text = self.set_compression_format(compression_format)
        full_file_path = os.path.join(file_path, file_name)
        tar_file_path = os.path.join(file_path, compressed_file_name)
        with tarfile.open(tar_file_path, 'w{}'.format(format_text)) as t:
            t.add(full_file_path, file_name)

    def compress_single_folder(self, file_path, compressed_file_name, file_filter_func=None):
        if not os.path.exists(file_path):
            self.logger.error('{} not exist. Unable to compress the folder.'.format(file_path))
        compression_format = self.get_compression_format_from_file_name(compressed_file_name)
        format_text = self.set_compression_format(compression_format)
        file_list = self.get_filtered_file_folder_list(file_path, True, file_filter_func)
        tar_file_path = os.path.join(file_path, compressed_file_name)
        root_folder = '/' + file_path.split('/')[-1]
        with tarfile.open(tar_file_path, 'w{}'.format(format_text)) as t:
            for file in file_list:
                t.add(os.path.join(file_path, file), os.path.join(root_folder, file))

    def compress_list_of_files(self, full_file_list, root_folder, compressed_file_name):
        if len(full_file_list) > 0:
            compression_format = self.get_compression_format_from_file_name(compressed_file_name)
            format_text = self.set_compression_format(compression_format)
            tar_file_path = os.path.join(root_folder, compressed_file_name)
            rename_file_list = [file_path.replace(root_folder, '') for file_path in full_file_list]
            with tarfile.open(tar_file_path, 'w{}'.format(format_text)) as t:
                for file, rename_file in zip(tqdm.tqdm(full_file_list), rename_file_list):
                    t.add(file, rename_file)
        else:
            self.logger.info('No files to be added to {}'.format(compressed_file_name))

    def extract_compressed_file(self, compressed_file_name, root_folder):
        compression_format = self.get_compression_format_from_file_name(compressed_file_name)
        format_text = self.set_compression_format(compression_format)
        tar_file_path = os.path.join(root_folder, compressed_file_name)
        with tarfile.open(tar_file_path, 'r{}'.format(format_text)) as t:
            t.extractall(root_folder)

    def list_files_in_compressed_file(self, compressed_file_name, root_folder):
        compression_format = self.get_compression_format_from_file_name(compressed_file_name)
        format_text = self.set_compression_format(compression_format)
        tar_file_path = os.path.join(root_folder, compressed_file_name)
        with tarfile.open(tar_file_path, 'r{}'.format(format_text)) as t:
            file_list = t.getnames()
        return file_list

    def verify_all_files_compressed(self, full_file_list, root_folder, compressed_file_name):
        compressed_file_list = self.list_files_in_compressed_file(compressed_file_name, root_folder)
        compressed_file_list = [os.path.join(root_folder, file) for file in compressed_file_list]
        missed_files = set(full_file_list) - set(compressed_file_list)
        if len(missed_files) == 0:
            return {'ok': True}
        else:
            return {'ok': False, 'missed_files': list(missed_files)}
