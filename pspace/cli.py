#!/usr/bin/env python3
"""
command-line utility for starting/monitoring paperspace jobs
    notably can use a yaml file in cwd to specify defaults
"""

# TODO 20190422: see if switches make sense (should -s and -l be reserved for short
#       and long?)
# TODO 20190422: maybe a --quiet/-q switch for some of these to reduce chattiness?


import argparse
import pathlib
import sys

import pspace


VALID_JOB_STATES = ['Pending', 'Provisioned', 'Running', 'Stopped',
        'Error', 'Failed', 'Cancelled']

def process_command_line(argv):
    """Process command line invocation arguments and switches.

    Args:
        argv: list of arguments, or `None` from ``sys.argv[1:]``.

    Returns:
        argparse.Namespace: named attributes of arguments and switches
    """
    #script_name = argv[0]
    argv = argv[1:]

    # initialize the parser object:
    parser = argparse.ArgumentParser(
            description="Utilities for creating and monitoring paperspace jobs.")
    subparsers = parser.add_subparsers(dest='pspace_cmd', help='sub-command help')

    # create
    create_desc = 'create a new job'
    parser_create = subparsers.add_parser(
            'create', help=create_desc, description=create_desc
            )
    parser_create.add_argument(
            '--machineType', action='store',
            help='The type of remote machine to use.'
            )
    parser_create.add_argument(
            '--project', action='store',
            help='The Paperspace project for the job.'
            )
    parser_create.add_argument(
            '--ignoreFiles', action='store',
            help='The directories or files to ignore (comma-separated).'
            )
    parser_create.add_argument(
            '--container', action='store',
            help='The docker container to use.'
            )
    parser_create.add_argument(
            '--commands', action='store',
            help='A list of commands to execute on remote machine.'
            )

    # tail (of job log file)
    tail_desc = 'get the tail of a job\'s log'
    parser_tail = subparsers.add_parser(
            'tail', help=tail_desc, description=tail_desc
            )
    parser_tail.add_argument(
            'job_id', nargs='?',
            help='ID of job to be checked.'
            )
    parser_tail.add_argument(
            '-f', '--follow', action='store_true',
            help='Follow the log file until cancelled or PSEOF.'
            )
    parser_tail.add_argument(
            '-l', '--last', action='store',
            help='How many lines at the end of the log (or "all") (default: 20)'
            )

    # jobs
    jobs_desc = 'list jobs'
    parser_jobs = subparsers.add_parser(
            'jobs', help=jobs_desc, description=jobs_desc
            )
    parser_jobs.add_argument(
            '-p', '--project', action='store',
            help='Filter only jobs matching this project.'
            )
    parser_jobs.add_argument(
            '-s', '--state', action='store',
            help='Filter only jobs matching this run state.'
            )
    parser_jobs.add_argument(
            '-l', '--last', action='store', type=int,
            help='Only list the last this many jobs matching filter(s).'
            )
    parser_jobs.add_argument(
            '-u', '--utc', action='store_true',
            help='Display all times as UTC.'
            )

    # status
    status_desc = 'get a job\'s status'
    parser_status = subparsers.add_parser(
            'status', help=status_desc, description=status_desc
            )
    parser_status.add_argument(
            'job_id', nargs='?',
            help='ID of job to be checked.'
            )
    parser_status.add_argument(
            '-u', '--utc', action='store_true',
            help='Display all times as UTC.'
            )

    # getart
    getart_desc = 'download artifact files from a job'
    parser_getart = subparsers.add_parser(
            'getart', help=getart_desc, description=getart_desc
            )
    parser_getart.add_argument(
            'job_id', nargs='?',
            help='ID of job to fetch artifacts for.'
            )
    parser_getart.add_argument(
            '--destdir', action='store',
            help='Local data directory to put job data dir. (default: data)'
            )

    # stop
    stop_desc = 'stop or cancel a job'
    parser_stop = subparsers.add_parser(
            'stop', help=stop_desc, description=stop_desc
            )
    parser_stop.add_argument(
            'job_id', nargs='?',
            help='ID of job to stop.'
            )

    # newyaml
    newyaml_desc = 'create new yaml config file from defaults'
    parser_stop = subparsers.add_parser(
            'newyaml', help=newyaml_desc, description=newyaml_desc
            )

    args = parser.parse_args(argv)

    return args


# commands -------------------------------------------------------------------

def command_newyaml(args):
    pspace.save_new_yaml_config()


def command_status(args):
    cmd_config = pspace.get_cmd_config(args)
    job_id = cmd_config['job_id']
    if job_id is None:
        print("Cannot determine job id.")
        return

    job_info = pspace.get_job_info(job_id)
    if 'error' in job_info:
        pspace.print_error(job_info)
        return

    pspace.save_last_info(job_info)

    print_keys = ['state', 'Started', 'Finished', 'Duration', 'exitCode']
    pspace.print_job_status(job_info, print_keys, utc_str=cmd_config['utc'])


def command_jobs(args):
    cmd_config = pspace.get_cmd_config(args)

    if cmd_config['state'] is not None:
        # match any state argument case-insensitively, only first chars are
        #   needed
        state = cmd_config['state'][0].upper() + cmd_config['state'][1:]
        matching_state = [x for x in VALID_JOB_STATES if x.startswith(state)][0]
        cmd_config['state'] = matching_state

    list_kwargs = cmd_config.copy()
    list_kwargs.pop('last')
    job_list = pspace.jobs_list(**list_kwargs)
    # job_list appears to be in chronological order (first is earliest)
    if cmd_config['last'] is not None:
        job_list = job_list[-cmd_config['last']:]

    print_keys = ['name', 'state', 'entrypoint', 'project', 'Started',
            'Finished', 'exitCode', 'machineType',]
    first = True
    for job in job_list:
        if not first:
            print("")
        pspace.print_job_status(job, print_keys, utc_str=cmd_config['utc'])
        first = False


def command_tail(args):
    cmd_config = pspace.get_cmd_config(args, ['total_log_lines', 'last_job_id'])

    try:
        if cmd_config['last'].startswith('a') or cmd_config['last'].startswith('A'):
            # tail_lines = 0 is special value that indicates show all lines
            cmd_config['last'] = 0
    except AttributeError:
        pass
    tail_lines = int(cmd_config['last'])

    job_id = cmd_config['job_id']
    if job_id is None:
        print("Cannot determine job id.")
        return

    line_start = 0
    if tail_lines != 0:
        if (cmd_config['total_log_lines'] is not None) and (cmd_config['last_job_id'] == job_id):
            line_start = max(0, cmd_config['total_log_lines'] - tail_lines)

    (job_info, total_log_lines) = pspace.print_last_log_lines(
            job_id,
            tail_lines,
            line_start,
            follow=cmd_config['follow']
            )
    if 'error' in job_info:
        pspace.print_error(job_info)
        return

    extra_save_info = {
            'total_log_lines':total_log_lines,
            'last_job_id':job_id,
            }
    pspace.save_last_info(job_info, extra_save_info)


def command_create(args):
    cmd_config = pspace.get_cmd_config(args)

    if cmd_config['project'] is None:
        cmd_config['project'] = pathlib.Path.cwd().name

    print('Submitting with options:')
    pspace.print_create_options(cmd_config)

    job_info = pspace.jobs_create(**cmd_config)
    if 'error' in job_info:
        pspace.print_error(job_info)
        return

    print("Job " + job_info['id'] + " created.")

    pspace.save_last_info(job_info)


def command_getart(args):
    cmd_config = pspace.get_cmd_config(args)

    print("Retrieving artifacts for job " + cmd_config['job_id'] + " ...")
    pspace.get_artifacts(cmd_config['job_id'], cmd_config['destdir'])
    # TODO 20190422: error handling
    pspace.save_log(cmd_config['job_id'], cmd_config['destdir'])


def command_stop(args):
    cmd_config = pspace.get_cmd_config(args)

    print("Stopping job " + cmd_config['job_id'] + " ...")
    returnval = pspace.stop_job(cmd_config['job_id'])
    if not returnval:
        print("Success")
    else:
        pspace.print_error(returnval)


def main(argv=None):
    args = process_command_line(argv)

    if args.pspace_cmd == 'create':
        command_create(args)
    elif args.pspace_cmd == 'tail':
        command_tail(args)
    elif args.pspace_cmd == 'status':
        command_status(args)
    elif args.pspace_cmd == 'jobs':
        command_jobs(args)
    elif args.pspace_cmd == 'getart':
        command_getart(args)
    elif args.pspace_cmd == 'stop':
        command_stop(args)
    elif args.pspace_cmd == 'newyaml':
        command_newyaml(args)

    return 0


def cli():
    try:
        status = main(sys.argv)
    except KeyboardInterrupt:
        # Make a very clean exit (no debug info) if user breaks with Ctrl-C
        print("Stopped by Keyboard Interrupt", file=sys.stderr)
        # exit error code for Ctrl-C
        status = 130

    sys.exit(status)

if __name__ == "__main__":
    cli()
