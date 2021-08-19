# -*- coding: utf-8 -*-


import paramiko
import os
from stat import S_ISDIR as isdir
import zipfile
import fnmatch
import logging
import logging.config
import datetime
import pysftp


def download_from_remote(sftp_obj, remote_dir_name, local_dir_name):
    remote_file = sftp_obj.stat(remote_dir_name)
    if isdir(remote_file):
        #если remote - каталог - идем идем глубже, папки скачивать нельзя
        #создаем такую же папку на локальном компьютере, если ее нет
        check_local_dir(local_dir_name)
        #print('загрузка папки: ' + remote_dir_name)
        for remote_file_name in sftp.listdir(remote_dir_name):
            sub_remote = os.path.join(remote_dir_name, remote_file_name)
            sub_remote = sub_remote.replace('\\', '/')
            sub_local = os.path.join(local_dir_name, remote_file_name)
            sub_local = sub_local.replace('\\', '/')
            download_from_remote(sftp_obj, sub_remote, sub_local)
    else:
        #print ('загрузка файла: '+ remote_dir_name)
        sftp.get(remote_dir_name, local_dir_name)
        logger = logging.getLogger("exampleApp")
        logger.info('download file with name' + f'{remote_dir_name}')
        

def check_local_dir(local_dir_name): 
    #проверка локальной папки, если нет - создаем
    if not os.path.exists(local_dir_name):
        os.makedirs(local_dir_name)
        logger = logging.getLogger("exampleApp")
        logger.info(f'{local_dir_name} folder created')
        
def create_archive(local_dir_name):
    #архивирование ZipFile
    zf = zipfile.ZipFile(local_dir_name, "w")
    for dirname, subdirs, files in os.walk(local_dir_name):
        zf.write(dirname)
    zf.close()
    
def process_logging():
    """ логирование операций """
    logging.config.fileConfig('logging.conf')
    
def search_older_file(sftp_obj, remote_dir_name):
    """ функция для определения наличия на сервере storage файлов старше 
    90 дней """
    today = datetime.date.today()
    remote_file = sftp_obj.stat(remote_dir_name)
    if isdir(remote_file):
        for remote_file_name in sftp.listdir(remote_dir_name):
            sub_remote = os.path.join(remote_dir_name, remote_file_name)
            sub_remote = sub_remote.replace('\\', '-')
            search_older_file(sftp_obj, sub_remote)
    else:
        remote_dir_name = remote_dir_name.split('-')
        remote_files_date = datetime.date(int(remote_dir_name[0]),
                                    int(remote_dir_name[1]),
                                    int(remote_dir_name[2]))
        
        global oldest_files
        oldest_files = today - remote_files_date
    return oldest_files
    
def storage_dir_name(dirname):
    """ массив хранит названия всех папок с сервера """
    dir_names = []
    dir_names.append(dirname)     
        
if __name__ == "__main__":
    """"""
    process_logging()
    logger = logging.getLogger("exampleApp")
    logger.info('Starting program')
    
    #Информация о подключении к storage
    storage_name = 'storage'
    storage_user_name = 'USER'
    storage_password = 'PASS'
    storage_port = 22
    
    client = paramiko.SSHClient()
    client.connect(storage_name, storage_port, 
                   storage_user_name, storage_password)
    stdin, stdout, stderr = client.exec_command('df -h')

    out = stdout.read().decode().strip()
    current_volume = out.sys.agrv[4]
    current_volume = int(current_volume)
    logger = logging.getLogger("exampleApp")
    logger.info(f'{current_volume} of storage server is load')
    
    
    if (current_volume > 90) or (oldest_files > 90):
        #путь к каталогу на локальной машине
        local_dir = 'archive/'
        #путь ко всем каталогам на сервере storage
        #используем pysftp для рекурсивного возврата всех каталогов
        cnopts = pysftp.CnOpts()
        with pysftp.Connection(host=storage_name, username=storage_user_name, 
                                 private_key=storage_password, 
                                 cnopts=cnopts) as sftp:
            sftp.walktree(storage_name, storage_dir_name,recurse=True)
        #подключение к storage просмотра каталогов на наличие старых файлов
        search_older_file(sftp, storage_dir_name)
        #подключение к storage для загрузки
        t = paramiko.Transport((storage_name, storage_port))
        t.connect(username=storage_user_name, password=storage_password)
        sftp = paramiko.SFTPClient.from_transport(t)
        #вызываем загрузчик
        download_from_remote(sftp, storage_dir_name, local_dir)
        #Close connection
        t.close()
        logger = logging.getLogger("exampleApp")
        logger.info('Download complete')
        #архивируем
        create_archive(local_dir)
        logger = logging.getLogger("exampleApp")
        logger.info(f'{local_dir} starting creating Zip')
        #Информация о подключении к arhive
        arhive_name = 'arhive'
        arhive_user_name = 'USER'
        arhive_password = 'PASS'
        arhive_port = 25
        
        #Путь к архивному серверу
        archive_remote_dir = 'archive/'
        rootPath = r"C:\local_archive"
        pattern = '*.zip'
        for files in os.walk(rootPath):
            for filename in fnmatch.filter(files, pattern):
                #подключение к <archive>
                t = paramiko.Transport((arhive_name, arhive_port))
                t.connect(username=arhive_user_name, password=arhive_password)
                sftp = paramiko.SFTPClient.from_transport(t)
                sftp.put(filename, archive_remote_dir)
                #Close connection
                t.close()
                logger = logging.getLogger("exampleApp")
                logger.info(f'{filename} send to' + f'{archive_remote_dir}')
            

