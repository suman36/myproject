import os

# HADOOP_LOGS_DIR = '/usr/local/hadoop/logs/userlogs'
HADOOP_LOGS_DIR = 'userlogs'
SUCCESS_MESSAGE = 'Job Transitioned from COMMITTING to SUCCEEDED'
REPORT_FILE = 'report.csv'


def read_logs(log_file_path):
    with open(log_file_path, 'r') as f_logs:
        data = f_logs.readlines()

    for line in data:
        if '[AsyncDispatcher event handler]' in line and SUCCESS_MESSAGE in line:
            success_logs.append(line)

def parse_logs(success_logs):
    for log in success_logs:
        log = log.split()
        date = log[0]
        time = log[1].replace(',', ':')
        job_id = log[7][:-3]
        status = log[12]

        with open(REPORT_FILE, 'a') as f_report:
            f_report.write('%s, %s, %s, %s\n' %(date, time, job_id, status))

        print(date, time, job_id, status)


if __name__ == '__main__':

    # List files to read

    with open(REPORT_FILE, 'a') as f_report:
        f_report.write('Date, Time, Job_ID, Status\n')

    log_file = 'application_1580117353349_0001/container_1580117353349_0001_01_000001/syslog'
    log_file_path = os.path.join(HADOOP_LOGS_DIR, log_file)

    success_logs = []

    read_logs(log_file_path)

    parse_logs(success_logs)