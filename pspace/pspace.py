#!/usr/bin/env python3

"""pspace - Useful functions for running jobs on paperspace
"""


import copy
import datetime
import json
import pathlib
import subprocess
import time

import yaml
import paperspace


PSPACE_INFO_DIR = '.pspace'


def run_cmd_python(cmd_str, machine_type, container, ignore_files):
    # send job to paperspace, using paperspace-python command
    # for paperspace-python, we need to explicitly save workspace
    params = {
            'container':container,
            'machineType':machine_type,
            'command':cmd_str,
            'project':'mlexperiments',
            'tail':'false',
            'ignoreFiles':ignore_files,
            'workspace':'.',
            }
    job_info = paperspace.jobs.create(params, no_logging=True)
    return job_info


def run_cmd_cli(cmd_str, machine_type, container, ignore_files):
    # send job to paperspace, using paperspace-node binary
    command = [
            'paperspace', 'jobs', 'create',
            '--container', container,
            '--machineType', machine_type,
            '--command', cmd_str,
            '--project', 'mlexperiments',
            '--tail', 'false',
            '--ignoreFiles', ",".join(ignore_files),
            ]
    cmd_proc = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    job_info = json.loads(cmd_proc.stdout.decode('utf8'))
    return job_info

def run_cmd(*args, **kwargs):
    #return run_cmd_cli(*args, **kwargs)
    return run_cmd_python(*args, **kwargs)

def job_done(job_id):
    """Check if job is done (Successfully or unsuccessfully)
    Args:
        job_id (str): the Paperspace job ID string
    Returns:
        bool: True if job has finished (and will not be running in future)
    """
    command = [
            'paperspace', 'jobs', 'show',
            '--jobId', job_id
            ]
    cmd_proc = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    job_info = json.loads(cmd_proc.stdout.decode('utf8'))
    # https://paperspace.github.io/paperspace-node/jobs.html#.waitfor
    # job_info['state'] is one of:
    #   Pending - the job has not started setting up on a machine yet
    #   Provisioned
    #   Running - the job is setting up on a machine, running, or tearing down
    #   Stopped - the job finished with a job command exit code of 0
    #   Error - the job was unable to setup or run to normal completion
    #   Failed - the job finished but the job command exit code was non-zero
    #   Cancelled - the job was manual stopped before completion
    #
    # normal:
    #   Pending -> Provisioned -> Running -> Stopped
    #                                    \-> Cancelled
    return job_info['state'] in ['Stopped', 'Cancelled', 'Failed', 'Error']


def jobs_list():
    """Return all running jobs
    """
    command = [
            'paperspace', 'jobs', 'list',
            '--state', 'Running'
            ]
    cmd_proc = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    jobs_info = json.loads(cmd_proc.stdout.decode('utf8'))
    return jobs_info


def get_log_lines(job_id, line_start=0):
    command = [
            'paperspace', 'jobs', 'logs',
            '--jobId', job_id,
            '--line', str(line_start),
            '--json',
            ]
    cmd_proc = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    log_output = json.loads(cmd_proc.stdout.decode('utf8'))

    # log_output is list of dicts, each dict:
    #   {
    #       'line':<int, line number>
    #       'timestamp':<str, time in UTC>,
    #       'message':<str, line of log output>,
    #   }
    return [x['message'] for x in log_output]


def get_artifacts(job_id, local_data_dir):
    """Put artifact files/dirs in local_data_dir / job_id
    """
    local_data_path = pathlib.Path(local_data_dir)
    dest_path = local_data_path / job_id
    dest_path.mkdir(parents=True, exist_ok=True)
    command = [
            'paperspace', 'jobs', 'artifactsGet',
            '--jobId', job_id,
            '--dest', str(dest_path),
            ]
    cmd_proc = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)


def save_log(job_id, local_data_dir):
    """Write log file of job to directory local_data_dir / job_id / "log.txt"
    """
    local_data_path = pathlib.Path(local_data_dir)
    dest_path = local_data_path / job_id
    dest_path.mkdir(parents=True, exist_ok=True)
    log_path = dest_path / 'log.txt'
    log_lines = get_log_lines(job_id)
    with log_path.open('w') as log_fh:
        for log_line in log_lines:
            print(log_line, file=log_fh)


def follow_log(job_id):
    log_line_start = 0
    # in case we drop through this while immediately, have log_lines for next
    #   while loop
    log_lines = ['PSEOF']
    while not job_done(job_id):
        time.sleep(5)
        log_lines = get_log_lines(job_id, log_line_start)
        if log_lines:
            print('\n'.join(log_lines))
        log_line_start += len(log_lines)
    # try to get remaining log lines until timeout
    timeout = 20
    start_time = time.time()
    # under normal circumstances looks like log file can last about 3 seconds
    #   after job Stopped until PSEOF (got 2 times through 1 second loop)
    # NOTE: last log line should be PSEOF
    while not log_lines[-1]=='PSEOF':
        time.sleep(5)
        log_lines = get_log_lines(job_id, log_line_start)
        if log_lines:
            print('\n'.join(log_lines))
        if time.time() - start_time > timeout:
            break


def get_job_info(job_id):
    command = [
            'paperspace', 'jobs', 'show',
            '--jobId', job_id
            ]
    cmd_proc = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    job_info = json.loads(cmd_proc.stdout.decode('utf8'))
    # https://paperspace.github.io/paperspace-node/jobs.html#.waitfor
    # job_info['state'] is one of:
    #   Pending - the job has not started setting up on a machine yet
    #   Provisioned
    #   Running - the job is setting up on a machine, running, or tearing down
    #   Stopped - the job finished with a job command exit code of 0
    #   Error - the job was unable to setup or run to normal completion
    #   Failed - the job finished but the job command exit code was non-zero
    #   Cancelled - the job was manual stopped before completion
    #
    # normal:
    #   Pending -> Provisioned -> Running -> Stopped
    #                                    \-> Cancelled
    return job_info


def get_config(arg_config=None):
    if arg_config is None:
        arg_config = {}

    default_config = {
            'command': './cmd_paperspace.sh',
            'machineType': 'K80',
            'container': 'itsayellow/tensorflow-python:latest',
            'ignoreFiles': ['virt'],
            'local_data_dir': 'data',
            'project': None
            }
    with open('paperspace.yaml', 'r') as yaml_fh:
        yaml_config = yaml.safe_load(yaml_fh)
    job_config = copy.deepcopy(default_config)
    for key in yaml_config:
        job_config[key] = yaml_config[key]
    for key in arg_config:
        job_config[key] = arg_config[key]

    return job_config


def save_last_info(job_info):
    pspace_info_path = pathlib.Path('.') / PSPACE_INFO_DIR
    pspace_info_path.mkdir(exist_ok=True)
    pspace_job_info_path = pspace_info_path / 'info.json'

    job_info = {x:job_info[x] for x in job_info if job_info[x] is not None}
    pspace_info = {'info_updated': str(datetime.datetime.now())}

    info = {'job_info': job_info, 'pspace_info': pspace_info}

    with pspace_job_info_path.open('w') as pspace_job_info_fh:
        json.dump(info, pspace_job_info_fh, indent=4, sort_keys=True)


def get_last_info():
    job_info = {}

    pspace_info_path = pathlib.Path('.') / PSPACE_INFO_DIR
    pspace_job_info_path = pspace_info_path / 'info.json'
    try:
        with pspace_job_info_path.open('r') as pspace_job_info_fh:
            info = json.load(pspace_job_info_fh)
    except IOError:
        print("Can't find pspace info for last job.")

    return info
