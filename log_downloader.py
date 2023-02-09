#!/usr/bin/python

import json
import logging
import os
import signal
import subprocess
import sys
import time
from logging.handlers import RotatingFileHandler
from threading import Thread

stop = False

class DownloadWorker(Thread):
    def __init__(self, server_name, server_pattern, raw_path, gzip_path):
        Thread.__init__(self)
        self.server_name = server_name
        self.server_log_path = '/home/dk/log/impressions_V02/'
        self.server_pattern = server_pattern
        self.raw_path = raw_path
        self.gzip_path = gzip_path
        self.start_hour = 0
        self.max_hour = 48

    def run(self):
        global stop
        if not os.path.exists(self.raw_path):
            os.makedirs(self.raw_path)
        if not os.path.exists(self.gzip_path):
            os.makedirs(self.gzip_path)
        logging.info("Download worker for {0} start".format(self.server_name))
        self.compress()
        while not stop:
            try:
                logging.info("Start loop")
                file_num = self.download_loop()
                if 0 == file_num:
                    time.sleep(5)
            except BaseException as e:
                logging.error(e.message)

    def download_loop(self):
        total = 0
        oldest = 0
        for hour in xrange(self.start_hour, -1, -1):
            file_num = self.download(hour)
            total = total + file_num
            if file_num > 0:
                self.compress()
                if 0 == oldest and (hour > 0 or file_num > 20):
                    """ Longer period needs more time to download.
                    Multiply 2 to make sure that next start time is before current one.
                    """
                    oldest = min((hour + 1) * 2, self.max_hour)
        self.start_hour = oldest
        return total

    def download(self, start):
        global stop
        logging.info("Check {0}:{1}{2}".format(self.server_name, self.server_log_path, self.server_pattern))
        response = check_output(
            ["ssh", "dk@{0}.trafficnetwork.net".format(self.server_name),
             "find {0} -name {1} -mmin +{2} -mmin -{3}".format(self.server_log_path, self.server_pattern, start * 60 + 10, start * 60 + 120)])
        file_list = response.split("\n")
        file_num = 0
        for file_full_name in file_list:
            if stop:
                break
            if len(file_full_name) == 0:
                continue
            file_name = file_full_name.split("/")[-1]
            if not os.path.exists(self.gzip_path + file_name + ".gz"):
                file_num += 1
                logging.info("Start to download {0}".format(file_name))
                if 0 == subprocess.call(
                        ["scp", "-q", "dk@{0}.trafficnetwork.net:{1}".format(self.server_name, file_full_name),
                         self.raw_path + file_name + ".tmp"]):
                    logging.info("Validate {0}".format(file_name))
                    server_md5 = check_output(["ssh", "adbroker@{0}.trafficjunky.net".format(self.server_name),
                                               "md5sum {0}".format(file_full_name)])
                    local_md5 = check_output(["md5sum", self.raw_path + file_name + ".tmp"])
                    if server_md5[:32] == local_md5[:32]:
                        subprocess.call(["mv", "-f", self.raw_path + file_name + ".tmp", self.raw_path + file_name])
                        logging.info("Download {0} success".format(file_name))
                    else:
                        if os.path.exists(self.raw_path + file_name + ".tmp"):
                            os.remove(self.raw_path + file_name)
                        logging.warning("Validate {0} failed".format(file_name))
                else:
                    if os.path.exists(self.raw_path + file_name + ".tmp"):
                        os.remove(self.raw_path + file_name + ".tmp")
                    logging.warning("Download {0} failed".format(file_name))
        return file_num

    def compress(self):
        global stop
        raw_list = os.listdir(self.raw_path)
        for raw_file in raw_list:
            if stop:
                break
            if raw_file.endswith(".gz") or raw_file.endswith(".tmp"):
                os.remove(self.raw_path + raw_file)
                continue
            if not os.path.exists(self.gzip_path + raw_file + ".gz"):
                logging.info("Start to compress {0}".format(raw_file))
                success = False

                process = subprocess.Popen("gzip -c {0} > {1}".format(self.raw_path + raw_file,
                                                                      self.raw_path + raw_file + ".gz"), shell=True)
                process.wait()
                if 0 == process.returncode:
                    if 0 == subprocess.call(
                            ["mv", "-f", self.raw_path + raw_file + ".gz", self.gzip_path]):
                        success = True
                if success:
                    logging.info("Compress {0} success".format(raw_file))
                else:
                    logging.warning("Compress {0} failed".format(raw_file))


def check_output(cmd):
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    result = process.communicate()
    if process.returncode == 0:
        output = result[0]
        return output
    else:
        logging.warning(result[1])
        return ""


def handler(signum, frame):
    global stop
    stop = True


def main(config):
    running = subprocess.Popen("ps aux | grep python | grep log_downloader | grep -v grep | wc -l",
                               stdout=subprocess.PIPE, shell=True).stdout.read()
    if int(running) > 1:
        return
    global stop
    signal.signal(signal.SIGQUIT, handler)
    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)

    servers = []
    with open(config) as json_file:
        try:
            json_data = json.load(json_file)
            for s in json_data["servers"]:
                servers.append(s["url"])
            log_folder = json_data['info']['log_folder']
        except ValueError:
            raise Exception("[WARN]: Invalid JSON File / Format")

    if log_folder is None or len(log_folder) == 0:
        log_folder = "/home/dk/log/s3_backup"

    log_file = log_folder + "/downloader/download.log"
    logger = logging.getLogger()
    logging_handler = RotatingFileHandler(log_file, maxBytes=50 * 1024 * 1024, backupCount=10)
    logging_handler.setFormatter(logging.Formatter(
        fmt='[%(asctime)s][%(threadName)s][%(levelname)s]:%(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'))
    logger.addHandler(logging_handler)
    logger.setLevel(logging.INFO)

    server_imp_pattern = "imp_log_V02*.log"
    server_click_pattern = "click_imp_log_V02*.log"
    raw_imp_path = "/home/dk/data/log/raw_impressions/"
    raw_click_path = "/home/dk/data/log/raw_clicks/"
    gzip_imp_path = "/home/dk/data/log/gzip_impressions/"
    gzip_click_path = "/home/dk/data/log/gzip_clicks/"

    logging.info("Start")
    for server in servers:
        thread = DownloadWorker(server, server_imp_pattern, "{0}{1}/".format(raw_imp_path, server),
                                "{0}{1}/".format(gzip_imp_path, server))
        thread.start()
        thread = DownloadWorker(server, server_click_pattern, "{0}{1}/".format(raw_click_path, server),
                                "{0}{1}/".format(gzip_click_path, server))
        thread.start()

    while not stop:
        time.sleep(5)


if __name__ == "__main__":
    main(sys.argv[1])
