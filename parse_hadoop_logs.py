import os
import sys

HADOOP_LOGS_DIR = '/usr/local/hadoop/logs/userlogs'
# HADOOP_LOGS_DIR = sys.argv[1]
#HADOOP_LOGS_DIR = 'userlogs'
SUCCESS_MESSAGE = 'Job Transitioned from COMMITTING to SUCCEEDED'
REPORT_FILE = '/opt/ansible/report.csv'


def list_log_files():
    syslog_files = []
    for root_dir, sub_dirs, files in os.walk(HADOOP_LOGS_DIR):
        for file in files:
            if file == 'syslog':
                syslog_files.append(os.path.join(root_dir, file))

    return syslog_files


def parse_logs(syslog_files):
    success_logs = []
    for file in syslog_files:
        with open(file, 'r') as f_logs:
            data = f_logs.readlines()

        for line in data:
            if '[AsyncDispatcher event handler]' in line and SUCCESS_MESSAGE in line:
                line = line.split()
                date = line[0]
                time = line[1].replace(',', ':')
                job_id = line[7][:-3]
                status = line[12]
                log_tuple = (date, time, job_id, status)
                success_logs.append(log_tuple)

    return success_logs


def write_output_file(success_logs):
    with open(REPORT_FILE, 'w') as f_report:
        f_report.write('Date, Time, Job_ID, Status\n')
        for log in success_logs:
            f_report.write('%s, %s, %s, %s\n' %(log[0], log[1], log[2], log[3]))
            print(log)


if __name__ == '__main__':
    syslog_files = list_log_files()

    success_logs = parse_logs(syslog_files)

    write_output_file(success_logs)
