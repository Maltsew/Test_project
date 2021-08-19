# -*- coding: utf-8 -*-

"""
загрузка данных с удаленного сервера с помощью библиотеки paramiko
допущения: на сервере - Linux. в storage папки с файлами расположены
в иерархии сначала год 21/ -> 02/ -> 25/, внутри которых уже и находятся 
искомые файлы. нет ограничения на время скачивания, нет ограничения
на время архивирования, и нет ограничения на объем локального диска.
в упрощенном виде я просто выгружу весь сервер на лок. машину, 
заархивирую каждую папку, потом каждый архив
отправлю на архивный сервер. можно скачивать только например по 1 году
с хранилища, архивировать такой каталог и после его отправлять,
можно поработать с потоками.
"""


import paramiko
import os
from stat import S_ISDIR as isdir
import zipfile
from pathlib import Path
import fnmatch
import logging
import logging.config


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
        """исходная папка not isdir - конечный каталог, 
        день месяца. загружаем файлы, путь на локальном такой же как до
        исходной папки на сервере"""
        
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
    logging.config.fileConfig('logging.conf')
        
if __name__ == "__main__":
    
    process_logging()
    logger = logging.getLogger("exampleApp")
    logger.info('Starting program')
    
    #Информация о подключении к storage
    storage_name = 'storage'
    storage_user_name = 'USER'
    storage_password = 'PASS'
    storage_port = 22
    
    
    """Резберемся теперь со storage. Загрузку данных с него необходимо выполнить
    только если диск занят на > 90% (примем что диск один) или если данные
    хранятся на нем больше 90 дней. Можем получить эту информацию по ssh
    сервера, и тут снова использую paramiko, т.к. с его помощью можно выполнить
    запрос напрямую"""
    
    
    client = paramiko.SSHClient()
    client.connect(storage_name, storage_port, 
                   storage_user_name, storage_password)
    stdin, stdout, stderr = client.exec_command('df -h')
    
    
    """в stdout получим колонки Filesystem, Size, Used, Avail, Use%, Mounted on
    получим только значение USE%, потом отбросим %, приведем к int и сравним
    со значением 90. Превысили - начинаем выгрузку со storage. На сервере может
    быть несколько систем, много дисков, как вариант в stdout считывать [4] для
    всех строк, потом посчитать среднюю заполненность сервера, думаю"""
    
    
    current_volume = stdout.sys.agrv[4]
    procent = '%'
    current_volume = current_volume - procent
    current_volume = int(current_volume)
    
    
    """ Для второго условия выгрузки могу только предположить. также через
    SSHClient заходить на сервер. сделать функцию, которая будет проходить по
    всем папкам,которые являются дерикториями, и записывать названия в 
    формат вроде гггг-мм-дд. собрать все названия в список, сортировать. 
    от сегодняшней даты отсчитывать 90 дней, вычислить эту дату. после попытаться
    найти эту дату в списке, и если она есть, взять срез от начала до это даты,
    это и будут папки с датами старше 90 дней. Скорее всего, все можно сделать
    намного проще в линуксе, надо подумать
    """
    
    
    if current_volume > 90:
        #путь к каталогу на локальной машине
        local_dir = 'archive/'
        
        
        """Путь к удаленному серверу. Требуется абсолютный путь к файлам, поскольку 
        папок много, нужно будет менять абсолютный путь для каждой даты
        решение - использовать pathlib, списковым включением верну все подкаталоги
        """
        
        
        p = Path('storage/')
        storage_remote_dir = [folder for folder in p.iterdir() if folder.is_dir()]
        #подключение к storage
        t = paramiko.Transport((storage_name, storage_port))
        t.connect(username=storage_user_name, password=storage_password)
        sftp = paramiko.SFTPClient.from_transport(t)
        #вызываем загрузчик
        download_from_remote(sftp, storage_remote_dir, local_dir)
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
        
        
        """путь к архиву.на локальной машине надо перебирать из списка всех архивов
        отпрвляем только архивы, чтобы не скинуть лог"""
        
        
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
            
    
    
    
