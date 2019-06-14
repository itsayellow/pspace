#!/usr/bin/env python3

"""pspace - Useful functions for running jobs on paperspace
"""


import copy
import datetime
import pathlib
import time

import yaml
import paperspace


PSPACE_INFO_DIR = '.pspace'
YAML_CONFIG_SEARCH_PATHS_LOCAL = [
        pathlib.Path('.'),
        pathlib.Path('./.pspace'),
        ]
PSPACE_CONFIG_FILE = 'pspace.yaml'
PSPACE_LASTINFO_FILE = 'last_cmd_info.yaml'
# Any default not listed here will show up as None
#   value is [<actual command default>, <newyaml init default>
CMD_ARG_DEFAULTS = {
        'create':{
            'commands': [None, ['echo hello world']],
            'container': [
                'tensorflow/tensorflow-python:latest',
                'tensorflow/tensorflow-python:latest',
                ],
            'machineType': ['K80','K80'],
            'ignoreFiles': [None, ['ignore1', 'ignore2']],
            },
        'tail':{
            'follow': [False, False],
            'last': ['all', 20],
            },
        'getart':{
            'destdir': ['data', 'data'],
            },
        'jobs':{
            'last': [None, None],
            'utc': [False, False],
            },
        'status':{
            'utc': [False, False],
            }
        }


# all datetimes start with dt in job_info (e.g. dtCreated, dtFinished, etc.)
#
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
#                                    \-> Failed
#   other exits at any time: Error, Cancelled


# pspace helpers -------------------------------------------------------------

# TODO 20190422: can use naive dt_utc return value, may be simpler for other code
def parse_jobinfo_dt(dt_in_str, utc_str=False):
    """From dt string from job_info, return datetime in UTC
    """
    dt = datetime.datetime.strptime(
                dt_in_str[:-5], "%Y-%m-%dT%H:%M:%S"
                )
    dt_utc = dt.replace(tzinfo=datetime.timezone.utc)

    if utc_str:
        dt_out_str = dt_utc.strftime("%Y-%m-%d %I:%M:%S%p") + " UTC"
    else:
        dt_local = dt_utc.astimezone()
        dt_out_str = dt_local.strftime("%Y-%m-%d %I:%M:%S%p") + " " + dt_local.tzname()

    return (dt_utc, dt_out_str)


def wrap_command_str(in_str, max_width, indent):
    """Commands ending with semicolon are split with a carriage return after
        semicolon.  Each command is split as necessary to not overrun the end
        of the line by splitting before an option.
    """
    max_width = max_width - indent
    # split commands at ;
    command_list = in_str.split('; ')
    command_list = [x + ';' for x in command_list]
    new_command_list = []
    for command in command_list:
        # wrap command list in a good way for commands (split at start of switch)
        command_part_list = command.split(' -')
        # after splitting, join as many pieces together that will fit on line
        new_command_part_list = []
        in_start = 0
        while in_start < len(command_part_list):
            in_end = in_start + 1
            while in_end <= len(command_part_list):
                if len(' -'.join(command_part_list[in_start:in_end])) < max_width:
                    in_end += 1
                else:
                    in_end -= 1
                    in_end = max(in_start + 1, in_end)
                    break
            new_command_part_list.append(' -'.join(command_part_list[in_start:in_end]))
            in_start = in_end
        new_command_list.append(new_command_part_list)

    command_str = ""
    all_commands_start = True
    for command_parts in new_command_list:
        command_start = True
        for command_substr in command_parts:
            if all_commands_start:
                command_str += command_substr + "\n"
                all_commands_start = False
            elif command_start:
                command_str += " "*(indent-1) + command_substr + "\n"
                command_start = False
            else:
                command_str += " "*indent + "-" + command_substr + "\n"
    return command_str.rstrip()


def print_create_options(create_options):
    indent_str = " "*4
    indent = len(indent_str + 'commands:  ')

    command_str = ''
    first_time = True

    cmd = "; ".join(create_options['commands'])
    command_str = wrap_command_str(cmd, 79, indent)

    #for cmd in create_options['commands']:
    #    if not first_time:
    #        command_str += "\n" + " "*indent
    #    command_str += wrap_command_str(cmd, 79, indent + 4)
    #    first_time = False

    for opt in sorted(create_options):
        if opt == 'commands':
            opt_value = command_str
        else:
            opt_value = create_options[opt]

        if create_options[opt] is not None:
            print(indent_str + opt + ": " + str(opt_value))


def get_cmd_config(args, extra_keys=None):
    if extra_keys is None:
        extra_keys = []

    # Lowest Priority
    # ----------------
    # command defaults
    # pspace last info
    # yaml config
    # command arguments
    # ----------------
    # Highest Priority

    args_to_pspacelast = {
            'job_id': ('job_info', 'id'),
            'total_log_lines': ('pspace_info', 'total_log_lines'),
            'last_job_id': ('pspace_info', 'last_job_id'),
            }

    args_dict = vars(args)
    cmd = args_dict.pop('pspace_cmd')
    yaml_config = get_yaml_config(cmd)
    pspace_last = get_last_info()
    cmd_config = {}
    for argkey in list(args_dict.keys()) + extra_keys:
        # Default to CMD_ARG_DEFAULTS value or None
        # first item in dict value list is for newyaml
        cmd_config[argkey] = CMD_ARG_DEFAULTS.get(cmd, {}).get(argkey, [None])[0]

        # last command
        if argkey in args_to_pspacelast:
            (psection, pkey) = args_to_pspacelast[argkey]
            try:
                cmd_config[argkey] = pspace_last[psection][pkey]
            except KeyError:
                pass

        # YAML
        if argkey in yaml_config:
            cmd_config[argkey] = yaml_config[argkey]

        # command arguments
        if argkey in args_dict:
            if args_dict[argkey] is not None:
                cmd_config[argkey] = args_dict[argkey]

    return cmd_config


def update_job_info(job_info, later_indent, utc_str=False):
    """Format job_info values and add new derived ones.
    """
    if job_info.get('dtStarted', None) is not None:
        (started_utc, job_info['Started']) = parse_jobinfo_dt(job_info['dtStarted'], utc_str=utc_str)
    if job_info.get('dtFinished', None) is not None:
        (finished_utc, job_info['Finished']) = parse_jobinfo_dt(job_info['dtFinished'], utc_str=utc_str)
    if 'Started' in job_info and 'Finished' in job_info:
        job_info['Duration'] = str(finished_utc - started_utc)
    elif job_info['state'] in ['Running',]:
        job_info['Duration'] = str(datetime.datetime.now(tz=datetime.timezone.utc) - started_utc) + " (to now)"
    job_info['entrypoint'] = wrap_command_str(job_info['entrypoint'], 79, later_indent)

    return job_info


def print_error(job_info):
    job_err = job_info['error']
    print("ERROR {0}: {1}".format(job_err['status'], job_err['message']))


def get_job_info(job_id):
    job_info = paperspace.jobs.show({'jobId': job_id})
    return job_info


# job status helpers ----------------------------------------------------------

def job_not_started(job_id, job_info=None):
    """Check if has not started yet
    Args:
        job_id (str): the Paperspace job ID string
    Returns:
        bool: True if job has finished (and will not be running in future)
    """
    if job_info is None:
        job_info = paperspace.jobs.show({'jobId': job_id})
    return job_info['state'] in ['Pending', 'Provisioned']


def job_started(job_id, job_info=None):
    """Check if job has ever started
    """
    return not job_not_started(job_id=job_id, job_info=job_info)


def job_done(job_id, job_info=None):
    """Check if job is done (Successfully or unsuccessfully)
    Args:
        job_id (str): the Paperspace job ID string
    Returns:
        bool: True if job has finished (and will not be running in future)
    """
    if job_info is None:
        job_info = paperspace.jobs.show({'jobId': job_id})
    return job_info['state'] in ['Stopped', 'Cancelled', 'Failed', 'Error']


# pspace main api ------------------------------------------------------------

def print_job_status(job_info, print_keys, utc_str=False):
    max_key_len = max([len(x) for x in print_keys])
    job_info = update_job_info(job_info, later_indent=max_key_len+6, utc_str=utc_str)

    print(job_info['id'])
    for key in print_keys:
        post_key = " "*(max_key_len - len(key))
        print(" "*3 + key + post_key + ": " + str(job_info.get(key, '')))


def jobs_create(**kwargs):
    params = kwargs.copy()
    if 'project' not in params or params['project'] is None:
        params['project'] = pathlib.Path.cwd().name
    if 'workspace' not in params or params['workspace'] is None:
        params['workspace'] = '.'
    if params.get('commands', None) is not None:
        params['command'] = "; ".join(params['commands'])
        del params['commands']
    params.update({'tail':'false',})
    job_info = paperspace.jobs.create(params, no_logging=True)
    return job_info


def jobs_list(**kwargs):
    """Return all running jobs
    """
    jobs_list = paperspace.jobs.list(kwargs)

    return jobs_list


def get_artifacts(job_id, local_data_dir):
    """Put artifact files/dirs in local_data_dir / job_id
    """
    # TODO 20190422: maybe make this command quiet and make our own progress?
    local_data_path = pathlib.Path(local_data_dir)
    local_data_path.mkdir(parents=True, exist_ok=True)
    params = {
            'jobId': job_id,
            'dest': str(local_data_path),
            }
    paperspace.jobs.artifactsGet(params)
    # make 0-byte-size file with jobid
    (local_data_path / job_id).open('w').close()


def stop_job(job_id):
    return paperspace.jobs.stop({'jobId':job_id})

# job log stuff ----------------------------------------------------------------

def get_log_lines(job_id, line_start=0):
    # Keep asking for more log lines until we receive none, in case we hit
    #   max number of lines that paperspace.jobs.logs will return at once
    #   (default 2000).
    more_log_lines = True
    log_output = []
    while more_log_lines:
        params = {'jobId': job_id, 'line': line_start}
        new_log_output = paperspace.jobs.logs(params, no_logging=True)
        log_output.extend(new_log_output)
        more_log_lines = bool(new_log_output)
        line_start += len(new_log_output)
    # log_output is list of dicts, each dict:
    #   {
    #       'line':<int, line number>
    #       'timestamp':<str, time in UTC>,
    #       'message':<str, line of log output>,
    #   }
    # TODO 20190422: some of that info might be useful, return it?
    return [x['message'] for x in log_output]


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


def seconds_since_done(job_info):
    (finished_utc, _) = parse_jobinfo_dt(job_info['dtFinished'])
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    return (now_utc - finished_utc).total_seconds()


# TODO 20190422: one time this did not read or notice the PSEOF.  bug, but how?
def follow_log(job_id, line_start=0):
    last_log_line = ""
    while last_log_line != "PSEOF":
        job_info = get_job_info(job_id)
        # TODO 20190509: one time the next job_done had KeyError about 'state'
        #   need to redo job_info if error or empty?
        if 'error' in job_info:
            print("DEBUG: Error in job_info:")
            print(job_info)
        if job_done(job_id, job_info) and seconds_since_done(job_info) > 20:
            break

        time.sleep(5)

        log_lines = get_log_lines(job_id, line_start=line_start)
        line_start += len(log_lines)
        last_log_line = log_lines[-1] if log_lines else last_log_line
        for line in log_lines:
            print(line)

    return job_info


def follow_job_state(job_id):
    job_info = get_job_info(job_id)
    print("State: " + job_info['state'] + " "*10, end="", flush=True)
    if job_not_started(job_id, job_info):
        # waiting for job to start, updating state while we wait
        while job_not_started(job_id, job_info):
            time.sleep(5)
            last_state = job_info['state']
            job_info = get_job_info(job_id)
            if job_info['state'] != last_state:
                print("\r", end="", flush=True)
                print("State: " + job_info['state'] + " "*10, end="", flush=True)
    print("")
    return job_info


def print_last_log_lines(job_id, tail_lines=0, line_start=0, follow=False):
    """
    Args:
        job_id (str): job_id of paperspace job
        tail_lines (int): how many lines to print at the end of the log
            or 0 if all lines in log so far should be printed
        line_start (int): what line to start fetching from remote log
        follow (bool): whether or not to wait and print more log lines as they
            appear.  Returns when 'PSEOF' line is read.

    Returns:
        (job_info, total_log_lines): job_info for this job, and total_log_lines
            so far
    """
    total_log_lines = 0
    log_lines = []
    job_info = get_job_info(job_id)
    if 'error' in job_info:
        return (job_info, None)

    print("Job: " + job_id)
    if follow:
        # keep updating printed state while waiting for Running
        job_info = follow_job_state(job_id)
    else:
        print("State: " + job_info['state'] + " "*10)

    # check job_started with old job_info so log_lines matches reported status
    #   in case we're not follow'ing
    if job_started(job_id, job_info):
        log_lines = get_log_lines(job_id, line_start=line_start)
        total_log_lines = line_start + len(log_lines)
        for line in log_lines[-tail_lines:]:
            print(line)

    last_log_line = log_lines[-1] if log_lines else ''
    if follow and last_log_line != "PSEOF":
        line_start = len(log_lines) + line_start
        # TODO 20190422: try-except ctrl-c around this to enable rest of code to execute?
        job_info = follow_log(job_id, line_start)

    return (job_info, total_log_lines)


# yaml config -----------------------------------------------------------------

def get_yaml_cwd():
    yaml_config = None
    for dir_path in YAML_CONFIG_SEARCH_PATHS_LOCAL:
        yaml_config_file_path = dir_path / PSPACE_CONFIG_FILE
        try:
            with yaml_config_file_path.open('r') as yaml_fh:
                yaml_config = yaml.safe_load(yaml_fh)
        except IOError:
            pass
        else:
            break
    return yaml_config


def save_new_yaml_config():
    yaml_path = pathlib.Path('pspace.yaml')
    if yaml_path.exists():
        yaml_path.rename('pspace.yaml.bak')
    # second item in dict value list is for newyaml
    newyaml_defaults = copy.copy(CMD_ARG_DEFAULTS)
    for cmd_def in newyaml_defaults:
        for opt in newyaml_defaults[cmd_def]:
            newyaml_defaults[cmd_def][opt] = newyaml_defaults[cmd_def][opt][1]

    with yaml_path.open('w') as yaml_fh:
        yaml.dump(CMD_ARG_DEFAULTS, yaml_fh, width=70, indent=4, sort_keys=True)


def get_yaml_config(subcommand):
    yaml_config = get_yaml_cwd()

    job_config = {}
    if yaml_config is not None and subcommand in yaml_config:
        for key in yaml_config[subcommand]:
            job_config[key] = yaml_config[subcommand][key]

    return job_config


# pspace info ----------------------------------------------------------------

# TODO 20190422: We probably don't need to save all job info.
# really the only things we use are very few:
#   last job ID
#   last total log lines for a job ID

def save_last_info(job_info, extra_info=None):
    # only save .pspace/ if pspace.yaml in cwd,
    #   so we don't crap up every dir with a .pspace subdir
    if get_yaml_cwd() is None:
        return

    if extra_info is None:
        extra_info = {}

    pspace_info_path = pathlib.Path('.') / PSPACE_INFO_DIR
    pspace_info_path.mkdir(exist_ok=True)
    pspace_job_info_path = pspace_info_path / PSPACE_LASTINFO_FILE

    job_info = {x:job_info[x] for x in job_info if job_info[x] is not None}
    pspace_info = {'info_updated': str(datetime.datetime.now())}
    pspace_info.update(extra_info)

    info = {'job_info': job_info, 'pspace_info': pspace_info}

    with pspace_job_info_path.open('w') as pspace_job_info_fh:
        yaml.dump(info, pspace_job_info_fh, width=70, indent=4, sort_keys=True)


def get_last_info():
    info = {}

    pspace_info_path = pathlib.Path('.') / PSPACE_INFO_DIR
    pspace_job_info_path = pspace_info_path / PSPACE_LASTINFO_FILE
    try:
        with pspace_job_info_path.open('r') as pspace_job_info_fh:
            info = yaml.safe_load(pspace_job_info_fh)
    except IOError:
        #print("Can't find pspace info for last job.")
        info = {}

    return info
