#!/usr/bin/python

import argparse
import json
import logging
import logging.handlers
from datetime import datetime, timedelta
import time
from subprocess import Popen, PIPE
from threading import Thread
import requests

import boto

has_error = False


def main():
    parser = argparse.ArgumentParser(description='read the config.json file and start the process.')
    parser.add_argument('-c', '--config', help='/path/to/config.json', action='store')
    parser.add_argument('-d', '--date', help='date of the files: yyyy-mm-dd hh:00', action='store')
    args = parser.parse_args()

    json_file = args.config
    d = args.date

    with open(json_file) as config_file:
        try:
            json_data = json.load(config_file)
        except ValueError:
            raise Exception("[WARN]: Invalid JSON File / Format")

    # Setup log system
    log_file = json_data['info']['log_folder'] + "/s3_filelist.log"
    logger = logging.getLogger()

    handler = logging.handlers.RotatingFileHandler(log_file, maxBytes=50 * 1024 * 1024, backupCount=10)
    handler.setFormatter(logging.Formatter(
        fmt='[%(asctime)s][%(levelname)s][%(threadName)s]%(module)s.%(funcName)s(line %(lineno)d):%(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'))
    logger.addHandler(handler)
    # logger.addHandler(logging.StreamHandler())
    logger.setLevel(logging.INFO)

    logging.info("Start process")

    if d is None:
        date = datetime.now() - timedelta(hours=1)
    else:
        try:
            date = datetime.strptime(d, "%Y-%m-%d %H:00")
        except BaseException:
            raise Exception("Invalid date")

    date_uploaded = date - timedelta(days=2)
    date_notify = date - timedelta(hours=1)

    threads = []
    for log_type in json_data['info']['types']:
        for server in json_data['servers']:
            thread = ListingWorker(json_data['info'], date, log_type, server, False, json_data['influx'])
            threads.append(thread)
            thread.start()

            thread = ListFromS3(json_data['s3_server']['bucket'], json_data['info']['version'],
                                json_data['s3_server']['path_template'],
                                json_data['info']['output_path'], log_type, date_uploaded, server,
                                json_data['info']['path'], json_data['influx'])
            threads.append(thread)
            thread.start()

            thread = ListingWorker(json_data['info'], date_notify, log_type, server, True, json_data['influx'])
            threads.append(thread)
            thread.start()

    for thread in threads:
        thread.join()

    if has_error:
        logging.warn("Restart log downloader")
        Popen(['PID=$(ps aux | grep python | grep log_downloader | grep -v grep | awk "{print \$2}") && kill $PID'],
              stdout=PIPE, shell=True)


class ListFromS3(Thread):
    def __init__(self, bucket, version, path_template, output_path, log_type, date, server, local_path, influx):
        Thread.__init__(self)
        self.version = str(version).zfill(2)
        self.log_type = log_type
        self.date = date.strftime("%Y-%m-%d")
        self.hour = date.strftime("%H")
        bits = self.date.split("-")
        year = bits[0]
        month = str(bits[1]).zfill(2)
        day = str(bits[2]).zfill(2)
        try:
            self.conn = boto.connect_s3()
        except BaseException as e:
            raise Exception("no authentication found in .aws directory")

        self.bucket_name = str(bucket)
        self.path = path_template.format(log_type['foldersuffix'], year, month, day, self.hour)
        self.output_path = output_path
        self.server = server
        self.local_path = local_path
        self.influx = influx

    def run(self):

        s3_list = {}
        upload_list = {}
        count = 0

        try:
            b = self.conn.get_bucket(self.bucket_name.lower())
            file_pattern = name_generator(self.log_type['fileprefix'], self.version, self.server['hostname'],
                                          self.date, self.hour)
            prefix = self.path + file_pattern
            file_list = b.get_all_keys(prefix=prefix)
            for f in file_list:
                fn = str(f.key)
                md5 = f.etag.strip('"')
                s3_list[fn] = md5
                file_name = fn.split('/')
                upload_list[file_name[len(file_name) - 1]] = md5
                count = count + 1
        except Exception as e:
            logging.error(e)

        save_to_disk(self.output_path, s3_list, generate_file_name('s3', file_pattern, count))

        local_path = get_local_path(self.local_path, 'gzip', self.log_type['foldersuffix'],
                                    self.server['folder'])
        command = "cd {0}; for f in $(ls {1}*); do md5sum $f; done".format(local_path, file_pattern)
        gzip_list, line_count = get_file_list(command)
        not_upload = []
        for fn, md5 in gzip_list.items():
            if fn not in upload_list or md5 != upload_list[fn]:
                not_upload.append(fn)
        if len(not_upload) > 0:
            body = "Some files are not uploaded:\n"
            for line in not_upload:
                body = body + line + "\n"
            send_mail("Some log files on {0} are not uploaded".format(self.server["url"]), body)

        send_to_influx("s3_missing", self.server['url'], len(not_upload), self.influx, self.log_type['foldersuffix'])


class ListingWorker(Thread):
    def __init__(self, info, date, log_type, server, send_notify, influx):
        Thread.__init__(self)
        self.version = info['version']
        self.hour = date.strftime("%H")
        self.date = date.strftime("%Y-%m-%d")
        self.compression_types = ['raw', 'gzip']
        self.log_type = log_type
        self.server = server
        self.output_path = info['output_path']
        self.local_path = info['path']
        self.remote_folder = info['remote_folder']
        self.send_notify = send_notify
        self.influx = influx

    def run(self):
        file_list = {}
        file_pattern = name_generator(self.log_type['fileprefix'], self.version, self.server['hostname'], self.date,
                                      self.hour)
        for compression_type in self.compression_types:
            local_path = get_local_path(self.local_path, compression_type, self.log_type['foldersuffix'],
                                        self.server['folder'])

            command = "cd {0}; for f in $(ls {1}*); do md5sum $f; done".format(local_path, file_pattern)
            text, line_count = get_file_list(command)
            logging.info("Found {0} files with {1} pattern name".format(line_count, file_pattern))
            save_to_disk(self.output_path, text, generate_file_name(compression_type, file_pattern, line_count))
            file_list[compression_type] = text
        command = "ssh dk@{0}.trafficnetwork.net 'cd {1}; for f in $(ls {2}*); do md5sum $f; done'".format(
            self.server['url'],
            self.remote_folder,
            file_pattern)
        text, line_count = get_file_list(command)
        logging.info(
            "Found {0} files on server {1} with {2} pattern name".format(line_count, self.server['url'], file_pattern))
        save_to_disk(self.output_path, text, generate_file_name('remote', file_pattern, line_count))
        file_list['remote'] = text
        validate_download_list(file_list, self.send_notify, self.server["url"], self.influx, self.log_type)


def get_local_path(path, compression_type, log_type, server_folder):
    return "{0}{1}_{2}/{3}".format(path, compression_type, log_type, server_folder)


def validate_download_list(file_list, send_notify, server_name, influx, log_type):
    not_download = []
    for fn, md5 in file_list['remote'].items():
        if fn not in file_list['raw'] or md5 != file_list['raw'][fn]:
            not_download.append(fn)
    if len(not_download) > 0 and send_notify:
        body = "Some files are not downloaded:\n"
        for line in not_download:
            body = body + line + "\n"
        send_mail("Some log files on {0} are not downloaded".format(server_name), body)
    if send_notify:
        send_to_influx("download_missing", server_name, len(not_download), influx, log_type['foldersuffix'])

    not_gzip = []
    for fn, md5 in file_list['raw'].items():
        gzip_file = fn + ".gz"
        if gzip_file not in file_list['gzip']:
            not_gzip.append(fn)
    if len(not_gzip) > 0 and send_notify:
        body = "Some files are not compressed:\n"
        for line in not_gzip:
            body = body + line + "\n"
        send_mail("Some log files on {0} are not compressed".format(server_name), body)

    if send_notify:
        send_to_influx("gzip_missing", server_name, len(not_gzip), influx, log_type['foldersuffix'])

    if len(not_download) > 0 or len(not_gzip) > 0:
        global has_error
        has_error = True


def name_generator(file_type, version, hostname, date, hour):
    # sample file name format:
    # click_imp_log_V02_ded5128_2016-05-09_21
    return "{0}_log_V{1}_{2}_{3}_{4}".format(file_type, str(version).zfill(2), hostname, date, hour)


def get_file_list(command):
    text = {}
    count = 0
    try:
        lines = Popen([command], stdout=PIPE, shell=True)
        while True:
            line = lines.stdout.readline().rstrip()
            if line != "":
                f = line.split(' ')
                text[f[len(f) - 1]] = f[0]
                count = count + 1
            else:
                break
    except Exception as e:
        logging.error(e)
    return text, count


def generate_file_name(file_type, file_pattern, line_count):
    return "{0}_{1}_{2}.txt".format(file_type, file_pattern, line_count)


def save_to_disk(output_path, content, filename):
    with open("{0}/{1}".format(output_path, filename), "w") as f:
        for fn in content:
            f.write(content[fn] + '  ' + fn + "\n")
    logging.info("Saved file {0}".format(filename))


def send_mail(subject, body):
    # mail = Popen(["mail", "-s", str(subject), "tong.pang@mindgeek.com,gerry.gao@mindgeek.com"], stdin=PIPE)
    # mail.stdin.write(str(body))
    # mail.stdin.flush()
    # mail.stdin.close()
    # mail.wait()
    pass


def send_to_influx(event, server, count, influx_url, log_type):
    msg = "errMsg,event={0},server={1},log_type={2} count={3} {4}000000000".format(event, server, log_type,
                                                                                   count, int(time.time()))

    r = requests.post(url=influx_url, data=msg, headers={'Content-Type': 'application/octet-stream'})
    if r.status_code != 204:
        logging.info("Send message to influx returns {0}".format(r.status_code))


if __name__ == "__main__":
    main()
