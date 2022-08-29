#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
from argparse import ArgumentParser
from configparser import ConfigParser
import os
import shutil
from functools import lru_cache
from multiprocessing import Process
import subprocess
import docker
from docker import errors as docker_errors
import re
import math
from datetime import datetime, timedelta
import time
from crontab import CronTab
from docker.types import LogConfig, RestartPolicy, ServiceMode

from . import util

TRUTHS = {True, 1, 't', 'T', '1', "true", "TRUE", "True"}


def get_own_container(containers):
    own_id = util.get_own_docker_id()
    for c in containers:
        if c.id == own_id:
            return c

    raise LookupError("Cannot find a container running myself!")


class Job(object):
    label_key = None

    __slots__ = ("_ofelia", "_container", "_name", "_enabled", "_kv_args")

    def __init__(self, ofelia, container, name, enabled, kv_args):
        """
        :param Ofelia ofelia:
        :param JobContainer container:
        :param str name:
        :param bool enabled:
        :param dict kv_args:
        """
        self._ofelia = ofelia
        self._container = container  # type: JobContainer
        self._name = name
        self._enabled = enabled
        self._kv_args = kv_args

    @property
    def job_container(self):  # type: (Job) -> JobContainer
        return self._container

    def _execute(self):
        return NotImplementedError()

    def execute(self):
        ret_val, report_path = self._execute()
        if self._ofelia.do_save:
            save_on_error = self._ofelia.config("save-only-on-error", False) in TRUTHS
            if save_on_error and (ret_val == 0 or ret_val is None):
                pass  # skip save, no error
            else:
                save_folder = self._ofelia.config("save-folder", ".")
                save_folder = os.path.abspath(save_folder)
                if not os.path.isdir(save_folder):
                    print("Save dir does not exist: {}".format(save_folder))
                shutil.move(report_path, save_folder)


class ScheduledJob(Job):
    label_key = None
    # https://regex101.com/r/0gGnD2/2
    every_pattern = re.compile(r"^@every\s+(?:([\d]+)d)?\s*(?:([\d]+)h)?\s*(?:([\d]+)m)?\s*(?:([\d]+)s)?")
    __slots__ = ("_schedule", "_schedule_immediate", "_crontab", "_timedelta", "_lastran", "_post_delay")

    def __init__(self, ofelia, j_container, name, enabled, kv_args):
        """
        :param Ofelia ofelia:
        :param JobContainer j_container:
        :param str name:
        :param bool enabled:
        :param dict kv_args:
        """
        super(ScheduledJob, self).__init__(ofelia, j_container, name, enabled, kv_args)
        try:
            self._schedule = kv_args['schedule']
            del kv_args['schedule']
        except LookupError:
            raise RuntimeError("Job {} needs to have a .schedule property".format(name))
        self._schedule_immediate = kv_args.get('schedule-immediate', False) in TRUTHS
        self._post_delay = kv_args.get('post-delay', False) in TRUTHS
        if self._schedule.lstrip(" .\\/").startswith("@ev"):
            match = self.every_pattern.match(self._schedule)
            if not match:
                raise RuntimeError("Bad @every string in job {}.".format(name))
            days = int(match[1]) if match[1] is not None else 0
            hours = int(match[2]) if match[2] is not None else 0
            mins = int(match[3]) if match[3] is not None else 0
            secs = int(match[4]) if match[4] is not None else 0
            self._timedelta = timedelta(days=days, seconds=((hours*3600) + (mins*60) + secs), milliseconds=0)
            self._crontab = None
        else:
            try:
                self._crontab = CronTab(self._schedule)
                self._timedelta = None
            except AssertionError as ae:
                if len(ae.args) > 0:
                    message = str(ae.args)
                else:
                    message = "Unknown assertion error"
                raise RuntimeError("Bad schedule string in job: {}\n{}".format(name, message))
            except Exception as e:
                print(repr(e))
                raise RuntimeError("Bad schedule string in job: {}".format(name))
        self._lastran = 0

    def _execute_local(self, args):
        start_time = str(math.floor(time.time()))
        report_filename = "report_{}_{}.txt".format(self._name, str(math.floor(time.time())))
        report_path = os.path.join("/tmp", report_filename)
        with open(report_path, "wb") as report_file:
            report_file.write(b"Executing local job %s at %s\n" % (self._name.encode('latin-1'), start_time.encode('latin-1')))
            report_file.write(b"Command: %s\n" % b" ".join(a.encode('latin-1') for a in args))
            with subprocess.Popen(args, stderr=subprocess.PIPE, stdout=subprocess.PIPE, bufsize=8, universal_newlines=False, text=False) as process:
                stdout_iter_make = lambda: iter(process.stdout.readline, b"")
                stderr_iter_make = lambda: iter(process.stderr.readline, b"")
                stdout_iter = None
                stderr_iter = None
                stdout_stopped = False
                stderr_stopped = False
                while True:
                    if stdout_iter is None and not stdout_stopped:
                        stdout_iter = stdout_iter_make()
                    if stderr_iter is None and not stderr_stopped:
                        stderr_iter = stderr_iter_make()
                    if not stderr_stopped:
                        try:
                            error_chunk = next(stderr_iter)
                            sys.stderr.buffer.write(error_chunk)
                            report_file.write(b"E: %s" % error_chunk)
                        except StopIteration:
                            stderr_stopped = True
                            stderr_iter = None
                    if not stdout_stopped:
                        try:
                            out_chunk = next(stdout_iter)
                            sys.stdout.buffer.write(out_chunk)
                            report_file.write(b"O: %s" % out_chunk)
                        except StopIteration:
                            stdout_stopped = True
                            stdout_iter = None
                    if stderr_stopped and stdout_stopped:
                        p = process.poll()
                        if p is not None:
                            break
                process.stdout.close()
                process.stderr.close()
                ret_val = process.wait()
            report_file.write(b"Exit code: %s\n" % str(ret_val).encode('latin-1'))
            if ret_val:
                err = subprocess.CalledProcessError(ret_val, args)
                report_file.write(b"%s\n" % str(err).encode('latin-1'))
                print(err)
        return ret_val, report_path

    def _execute(self):
        return NotImplementedError()

    def _process(self):
        if self._schedule_immediate:
            print("Once-off executing, before scheduling.")
            self.execute()
            self._lastran = math.floor(time.time())
        if self._crontab:
            print("Scheduling job \"{}\" using given crontab".format(self._name))
            get_delay_seconds = lambda: math.floor(self._crontab.next(default_utc=True))
        elif self._timedelta:
            print("Scheduling job \"{}\" using offset: {}".format(self._name, self._timedelta))
            delta = self._timedelta
            get_delay_seconds = lambda: math.floor(delta.days * 86400 + delta.seconds)
        else:
            raise RuntimeError("Cannot determine how to process this scheduled job.")
        d = get_delay_seconds()
        while True:
            # get_delay_seconds will be zero seconds if we are _at_ the time for execution,
            # but we might've only just executed
            time_now = math.floor(time.time())
            while d < 1 and (time_now - self._lastran) < 2:
                time.sleep(1)
                d = get_delay_seconds()
                time_now = math.floor(time.time())
            print("Job {} sleeping {:d} seconds".format(self._name, d))
            time.sleep(d)
            if self._timedelta and not self._post_delay:
                d = get_delay_seconds()
            self.execute()
            self._lastran = math.floor(time.time())
            if self._crontab or self._post_delay:
                d = get_delay_seconds()

    def fork(self):
        p = Process(target=self.__class__._process, args=(self,))
        p.start()
        return p


class JobLocal(ScheduledJob):
    config_key = "job-local"
    label_key = "ofelia.job-local"
    __slots__ = ("_command",)

    def __init__(self, ofelia, j_container, name, enabled, kv_args):
        """
        :param Ofelia ofelia:
        :param JobContainer j_container:
        :param str name:
        :param bool enabled:
        :param dict kv_args:
        """
        super(JobLocal, self).__init__(ofelia, j_container, name, enabled, kv_args)
        try:
            self._command = kv_args['command']
            del kv_args['command']
        except LookupError:
            raise RuntimeError("Job {} needs to have a .command property".format(name))

    def _execute(self):
        args = ["/bin/bash", "-c", self._command]
        return self._execute_local(args)




class JobExec(ScheduledJob):
    config_key = "job-exec"
    label_key = "ofelia.job-exec"
    __slots__ = ("_command",'_user')

    def __init__(self, ofelia, j_container, name, enabled, kv_args):
        """
        :param Ofelia ofelia:
        :param JobContainer j_container:
        :param str name:
        :param bool enabled:
        :param dict kv_args:
        """
        super(JobExec, self).__init__(ofelia, j_container, name, enabled, kv_args)
        try:
            self._command = kv_args['command']
            del kv_args['command']
        except LookupError:
            raise RuntimeError("Job {} needs to have a .command property".format(name))
        self._user = kv_args.get('user', '')

    def _execute(self):
        start_time = str(math.floor(time.time()))
        report_filename = "report_{}_{}.txt".format(self._name, str(math.floor(time.time())))
        report_path = os.path.join("/tmp", report_filename)
        with open(report_path, "wb") as report_file:
            report_file.write(b"Executing job-exec %s at %s\n" % (self._name.encode('latin-1'), start_time.encode('latin-1')))
            report_file.write(b"Command: %s\n" % self._command.encode("latin-1"))
            on_container = self.job_container
            c = on_container._container

            ex = c.client.api.exec_create(c.id, self._command, stdout=True, stderr=True, stdin=False,
                                          user=self._user)
            exec_output = c.client.api.exec_start(ex['Id'], detach=False, stream=True, demux=True)
            try:
                it = iter(exec_output)
            except docker_errors.NotFound:
                report_file.write(b"No output found!\n")
                return 0, report_path
            while True:
                try:
                    (it_out, it_err) = next(it)
                    if it_out is not None:
                        sys.stdout.buffer.write(it_out)
                        report_file.write(b"O: %s" % it_out)
                    if it_err is not None:
                        sys.stderr.buffer.write(it_err)
                        report_file.write(b"E: %s" % it_err)
                except StopIteration:
                    break
                except docker_errors.NotFound:
                    break
            result = c.client.api.exec_inspect(ex['Id'])['ExitCode']
            report_file.write(b"Exit code: %s\n" % str(result).encode('latin-1'))
        return result, report_path

class JobRun(ScheduledJob):
    config_key = "job-run"
    label_key = "ofelia.job-run"
    __slots__ = ("_command", "_image", "_user", "_tty")

    def __init__(self, ofelia, j_container, name, enabled, kv_args):
        """
        :param Ofelia ofelia:
        :param JobContainer j_container:
        :param str name:
        :param bool enabled:
        :param dict kv_args:
        """
        super(JobRun, self).__init__(ofelia, j_container, name, enabled, kv_args)
        if self.job_container.container_is_ofelia and "image" in kv_args:
            self._container = None
        elif "command" in kv_args and "image" not in kv_args and self._container is not None:
            kv_args['image'] = self.job_container._container.image.id
            self._container = None
        if self._container is None:
            try:
                self._command = kv_args['command']
                del kv_args['command']
            except LookupError:
                raise RuntimeError("Job-Run \"{}\" needs to have a .command property".format(name))
            try:
                self._image = kv_args['image']
                del kv_args['image']
                image_collection = self._ofelia._container.image.collection
                try:
                    i = image_collection.get(self._image)
                except docker_errors.ImageNotFound:
                    raise RuntimeError("Job-Run \"{}\" image does not exist: {}".format(name, self._image))
                self._image = i
            except LookupError:
                raise RuntimeError("Job-Run \"{}\" needs to have .image property if no container is given".format(name))
        else:
            self._command = None
            self._image = None
        self._user = kv_args.get('user', '')
        self._tty = kv_args.get('tty', True)

    def _execute(self):
        if self.job_container is None and self._image is not None and self._command is not None:
            return self._execute_run()
        if self.job_container is not None:
            return self._execute_start(self.job_container._container)
        else:
            raise RuntimeError("Bad configuration on Job-Run job \"{}\"".format(self._name))

    def _execute_start(self, c):
        start_time = str(math.floor(time.time()))
        report_filename = "report_{}_{}.txt".format(self._name, str(math.floor(time.time())))
        report_path = os.path.join("/tmp", report_filename)
        with open(report_path, "wb") as report_file:
            report_file.write(b"Executing job-run %s at %s\n" % (self._name.encode('latin-1'), start_time.encode('latin-1')))
            try:
                process = c.start()
            except docker_errors.APIError as ai:
                print(ai.explanation)
                report_file.write(b"%s\n" % ai.explanation.encode('latin-1'))
                print(str(ai.args[0]))
                report_file.write(b"%s\n" % str(ai.args[0]).encode('latin-1'))

                return 1, report_path
            try:
                logging_driver = c.attrs['HostConfig']['LogConfig']['Type']
            except (docker_errors.NotFound, LookupError, AttributeError):
                # Will throw a NotFoundError if container died directly after startup
                logging_driver = None
            out = None
            if logging_driver == 'json-file' or logging_driver == 'journald':
                try:
                    out = c.logs(stdout=True, stderr=True, stream=True, follow=True)
                except docker_errors.NotFound:
                    # Will throw a NotFoundError if container died before we get here
                    out = None
            if out:
                it = iter(out)
                new_line = True
                while True:
                    try:
                        out_chunk = next(it)
                        sys.stdout.buffer.write(out_chunk)
                        if new_line:
                            report_file.write(b"O: %s" % out_chunk)
                        else:
                            report_file.write(out_chunk)
                        new_line = out_chunk.endswith(b'\n')
                    except StopIteration:
                        break
                    except docker_errors.NotFound:
                        # Will throw a NotFoundError if container died during this buffer iteration
                        break
            try:
                exit_status = c.wait()['StatusCode']
            except docker_errors.NotFound:
                # Will throw a NotFoundError if container died before we could await the StatusCode
                exit_status = 0
            try:
                c.remove()
            except (docker_errors.NotFound, docker_errors.APIError):
                # Will throw an API error because autoremove is set above.
                # Will throw a NotFoundError if its removed already
                pass
            report_file.write(b"Exit code: %s\n" % str(exit_status).encode('latin-1'))
        return exit_status, report_path

    def _execute_run(self):
        container_collection = self._ofelia._container.collection
        lc = LogConfig(type=LogConfig.types.JSON, config={})
        try:
            c = container_collection.create(self._image, self._command, user=self._user, tty=self._tty,
                                            detach=False, auto_remove=True, log_config=lc)
        except docker_errors.ImageNotFound:
            raise RuntimeError("Job-Run \"{}\" image does not exist: {}".format(self._name, str(self._image)))
        return self._execute_start(c)

class JobServiceRun(ScheduledJob):
    config_key = "job-service-run"
    label_key = "ofelia.job-service-run"
    __slots__ = ("_command", "_image", "_user", "_service", "_delete", "_tty", "_network")

    # two modes:
    #1)To run a command inside a new "run-once" service, for running inside a swarm.
    #Schedule, Command, Image, Network, Delete, User, Tty
    #2)To run a command inside an existing service, for running inside a swarm.
    #Schedule, Command, Service, User, Tty

    def __init__(self, ofelia, j_container, name, enabled, kv_args):
        """
        :param Ofelia ofelia:
        :param JobContainer j_container:
        :param str name:
        :param bool enabled:
        :param dict kv_args:
        """
        super(JobServiceRun, self).__init__(ofelia, j_container, name, enabled, kv_args)
        self._container = None  # Job-Service-Run doesn't use an existing container.
        try:
            self._command = kv_args['command']
            del kv_args['command']
        except LookupError:
            self._command = None
        try:
            service_name = kv_args['service']
            del kv_args['service']
            try:
                self._service = ofelia.swarm_service.collection.get(service_name)
            except:
                raise RuntimeError(
                    "Service specified by job-service-run {} not found: {}".format(name, service_name))
            self._image = None
            self._delete = None
            self._network = None
        except:
            self._service = None

        if self._service is None:
            try:
                image_name = kv_args['image']
                del kv_args['image']
                try:
                    self._image = ofelia._container.image.collection.get(image_name)
                except:
                    raise RuntimeError(
                        "Image specified by job-service-run {} not found: {}".format(name, image_name))
            except:
                self._image = None
            self._delete = kv_args.get('delete', True) in TRUTHS
            self._network = kv_args.get('network', None)
        self._user = kv_args.get('user', '')
        self._tty = kv_args.get('tty', True)


    def _execute(self):
        if self._service is not None:
            return self._run_existing_service(self._service)
        elif self._image is not None:
            return self._execute_new_service(self._image.id)
        else:
            raise RuntimeError("Bad configuration on Job-Service-Run job \"{}\"".format(self._name))

    def _execute_new_service(self, image_name):
        if self._network is not None:
            networks = [n.strip() for n in str(self._network).split(",")]
        else:
            networks = None
        do_delete = bool(self._delete)
        rp = RestartPolicy(condition='none', delay=0, max_attempts=0, window=0)
        mode = ServiceMode('replicated', 0)
        service = self._ofelia.swarm_service.collection.create(image_name, self._command, user=self._user,
                                                               log_driver="json-file", networks=networks,
                                                               tty=self._tty, restart_policy=rp, mode=mode)

        def _remove_service():
            nonlocal service
            sid = service.id
            collection = service.collection
            s = collection.get(sid)
            s.remove()

        try:
            exit_status, report = self._run_existing_service(service)
        except Exception:
            if do_delete:
                try:
                    _remove_service()
                except Exception:
                    pass
            raise
        if do_delete:
            try:
                _remove_service()
            except Exception as e:
                print(e)
        return exit_status, report


    def _run_existing_service(self, s):
        service_id = s.id
        services = s.collection
        start_time = str(math.floor(time.time()))
        report_filename = "report_{}_{}.txt".format(self._name, str(math.floor(time.time())))
        report_path = os.path.join("/tmp", report_filename)
        with open(report_path, "wb") as report_file:
            report_file.write(b"Executing job-service-run %s at %s\n" % (self._name.encode('latin-1'), start_time.encode('latin-1')))
            s = services.get(service_id)  # refresh service metadata
            tasks = s.tasks()
            num_tasks = len(tasks)
            do_scale_up = 1
            if num_tasks > 0:
                try:
                    report_file.write(b"Found %s tasks already. Scaling down before up.\n" % str(num_tasks).encode("latin-1"))
                    scale_time = math.floor(time.time())
                    r1 = s.scale(num_tasks-1)
                except docker_errors.InvalidArgument:
                    pass
                except docker_errors.APIError as ai:
                    print(ai.explanation)
                    report_file.write(b"%s\n" % ai.explanation.encode('latin-1'))
                    print(str(ai.args[0]))
                    report_file.write(b"%s\n" % str(ai.args[0]).encode('latin-1'))
                    return 1, report_path
                time.sleep(1)
                now_tasks = len(s.tasks())
                while True:
                    if num_tasks <= now_tasks:
                        now_time = math.floor(time.time())
                        if now_time - scale_time > 5:
                            report_file.write(b"Timeout scale down\n")
                            do_scale_up = num_tasks
                            break
                        time.sleep(1)
                        now_tasks = len(s.tasks())
                    else:
                        do_scale_up = now_tasks + 1
                        break
            s = services.get(service_id)  # refresh service metadata
            num_tasks = len(s.tasks())
            if num_tasks < do_scale_up:
                try:
                    report_file.write(b"Scaling up service.\n")
                    scale_time = math.floor(time.time())
                    r2 = s.scale(do_scale_up)
                except docker_errors.InvalidArgument:
                    report_file.write(b"Not scaling service, its global. %s\n" % self._name.encode("latin-1"))
                    if len(tasks) < 1:
                        report_file.write(b"Cannot start our stopped global service.\n")
                        return 1, report_path
                except docker_errors.APIError as ai:
                    print(ai.explanation)
                    report_file.write(b"%s\n" % ai.explanation.encode('latin-1'))
                    print(str(ai.args[0]))
                    report_file.write(b"%s\n" % str(ai.args[0]).encode('latin-1'))
                    return 1, report_path
                time.sleep(1) # wait a second for scaling up
                now_tasks = len(s.tasks())
                while True:
                    if num_tasks >= now_tasks:
                        now_time = math.floor(time.time())
                        if now_time - scale_time > 5:
                            report_file.write(b"Timeout scale up\n")
                            break
                        time.sleep(1)
                        now_tasks = len(s.tasks())
                    else:
                        break
            s = services.get(service_id)  # refresh service metadata
            tasks = s.tasks()
            if len(tasks) < 1:
                report_file.write(b"No service task to attach to. Job-Service-Run failed.\n")
                return 1, report_path
            newest_task_date = "1990-01-01T00:00:00.000Z"
            newest_task = None
            for t in tasks:
                if t['CreatedAt'] > newest_task_date:
                    newest_task_date = t['CreatedAt']
                    newest_task = t
            if newest_task is None:
                newest_task = tasks[-1]
            if newest_task['Status']['State'] == 'running':
                complete = False
            elif newest_task['Status']['State'] == 'complete':
                complete = True
            else:
                report_file.write(b"Task is not running or complete!\n")
                print("Task is not running or complete!")
                return 1, report_path
            container_id = newest_task['Status']['ContainerStatus']['ContainerID']
            try:
                c = self._ofelia._container.collection.get(container_id)
            except (docker_errors.NotFound, docker_errors.APIError) as e:
                print("Container not found on this node!")
                print(e)
                c = None
            if c:
                try:
                    logging_driver = c.attrs['HostConfig']['LogConfig']['Type']
                except (docker_errors.NotFound, LookupError, AttributeError):
                    # Will throw a NotFoundError if container died directly after startup
                    logging_driver = None
                out = None
                if logging_driver == 'json-file' or logging_driver == 'journald':
                    try:
                        out = c.logs(stdout=True, stderr=True, stream=True, follow=(not complete))
                    except docker_errors.NotFound:
                        # Will throw a NotFoundError if container died before we get here
                        out = None
                if out:
                    it = iter(out)
                    new_line = True
                    while True:
                        try:
                            out_chunk = next(it)
                            sys.stdout.buffer.write(out_chunk)
                            if new_line:
                                report_file.write(b"O: %s" % out_chunk)
                            else:
                                report_file.write(out_chunk)
                            new_line = out_chunk.endswith(b'\n')
                        except StopIteration:
                            break
                        except docker_errors.NotFound:
                            # Will throw a NotFoundError if container died during this buffer iteration
                            break
                try:
                    exit_status = c.wait()['StatusCode']
                except docker_errors.NotFound:
                    # Will throw a NotFoundError if container died before we could await the StatusCode
                    exit_status = 0
            else:
                try:
                    logging_driver = s.attrs['Spec']['TaskTemplate']['LogDriver']['Name']
                except (docker_errors.NotFound, LookupError, AttributeError):
                    # Will throw a NotFoundError if container died directly after startup
                    logging_driver = None
                out = None
                out_since = None
                if logging_driver == 'json-file' or logging_driver == 'journald':
                    if complete:
                         out = s.logs(stdout=True, stderr=True, follow=False) # bug in following a service logs
                    else:
                         out = s.logs(stdout=True, stderr=True, follow=None, tail=1)
                    out_since = math.floor(time.time())

                while out:
                    it = iter(out)
                    new_line = True
                    while True:
                        try:
                            out_chunk = next(it)
                            sys.stdout.buffer.write(out_chunk)
                            if new_line:
                                report_file.write(b"O: %s" % out_chunk)
                            else:
                                report_file.write(out_chunk)
                            new_line = out_chunk.endswith(b'\n')
                        except StopIteration:
                            break
                    if complete:
                        break
                    tasks = s.tasks(filters={"id": newest_task['ID']})
                    if len(tasks) < 1:
                        break
                    newest_task = tasks[0]
                    state = newest_task['Status']['State']
                    if state == "running":
                        time.sleep(1)
                        out = s.logs(stdout=True, stderr=True, follow=None, since=out_since)
                        out_since = math.floor(time.time())
                    elif state == "complete":
                        out = s.logs(stdout=True, stderr=True, follow=False, since=out_since)
                        complete = True
                    else:
                        out = None
                exit_status = None
                while exit_status is None:
                    tasks = s.tasks(filters={"id": newest_task['ID']})
                    if len(tasks) < 1:
                        exit_status = 1
                        break
                    newest_task = tasks[0]
                    state = newest_task['Status']['State']
                    if state == "running":
                        time.sleep(1)
                        continue
                    exit_status = newest_task['Status']['ContainerStatus']['ExitCode']

            if do_scale_up:
                try:
                    r3 = s.scale(do_scale_up-1)
                except Exception as e:
                    print(e)
                    pass
            report_file.write(b"Exit code: %s\n" % str(exit_status).encode('latin-1'))
        return exit_status, report_path

class JobContainer(object):
    __slots__ = ("_ofelia", "_container", "_enabled", "_jobs")

    def __init__(self, ofelia, container):
        """
        :param Ofelia ofelia:
        :param docker.models.containers.Container container:
        """
        self._ofelia = ofelia
        self._container = container
        self._enabled = True
        self._jobs = []

    @property
    def jobs(self):
        for j in self._jobs:
            yield j

    @property
    def is_enabled(self):
        return self._enabled

    @property
    def container_is_ofelia(self):
        return self._container.id == self._ofelia._container.id

    def find_jobs(self):
        raise NotImplementedError

    def add_job(self, job_type, job_name, enabled=True, kv_args=None):
        if kv_args is None:
            kv_args = {}
        if job_type is JobLocal:
            if not self.container_is_ofelia:
                print("Warning: job-local can only be specified on the local master ofelia container.")
                return
            new_job = JobLocal(self._ofelia, self, job_name, enabled, kv_args=kv_args)
        else:
            new_job = job_type(self._ofelia, self, job_name, enabled, kv_args=kv_args)
        self._jobs.append(new_job)


class LabeledContainer(JobContainer):
    __slots__ = ("_ofelia", "_container", "_enabled", "_jobs")

    def __init__(self, ofelia, container):
        super(LabeledContainer, self).__init__(ofelia, container)
        self._ofelia = ofelia
        self._container = container
        labels = self._container.labels
        self._enabled = labels.get("ofelia.enabled", '') in TRUTHS
        self.find_jobs()

    @property
    def jobs(self):
        for j in self._jobs:
            yield j

    @property
    def is_enabled(self):
        return self._enabled

    def find_jobs(self):
        labels_dict = self._container.labels
        used_labels = []
        label_keys_copy = sorted(list(labels_dict.keys()))
        for l in label_keys_copy:
            if l in used_labels:
                continue
            if l.startswith(JobLocal.label_key):
                if not self.container_is_ofelia:
                    print("Warning: Cannot only user job-local on a local master ofelia container.")
                    continue
                j_type = JobLocal
            elif l.startswith(JobExec.label_key):
                j_type = JobExec
            elif l.startswith(JobRun.label_key):
                j_type = JobRun
            else:
                continue

            label_parts = l.split('.', 4)
            if len(label_parts) < 4:
                print("Badly formatted label, skipping: {}".format(l))
                used_labels.append(l)
                continue
            job_name = label_parts[2]
            search_for = "{}.{}.".format(j_type.label_key, job_name)
            match_labels = [m for m in label_keys_copy if m.startswith(search_for)]
            used_labels.extend(match_labels)
            kv_args = {}
            for m in match_labels:
                label_parts = m.split('.', 4)
                if len(label_parts) < 4:
                    print("Badly formatted label, skipping: {}".format(m))
                    continue
                _, label_rest = label_parts[2:]
                kv_args[label_rest] = labels_dict[m]
            self.add_job(j_type, job_name, self._enabled, kv_args=kv_args)


class OfeliaConfig(dict):

    def __new__(cls, **kwargs):
        self = super(OfeliaConfig, cls).__new__(cls)
        for k,v in kwargs.items():
            try:
                self[k] = v
            except Exception:
                pass
        return self

    @classmethod
    def new_from_ini(cls, ini_file):
        a = os.path.abspath(ini_file)
        if not os.path.exists(a):
            raise RuntimeError("Config file doesn't exist {}".format(a))
        p = ConfigParser(cls.defaults(), default_section="global")
        p.read(a)
        c = OfeliaConfig()
        for k,v in p['global'].items():
            c[k] = v
        jobs = {}
        for s in p.sections():
            if not s.startswith("job"):
                continue
            parts = s.split(" ", 1)
            if len(parts) < 2:
                continue
            job_type = parts[0].replace(" ", "-")
            job_name = parts[1].strip("\"\'").replace(" ", "-")
            job_tuple = (job_type, job_name)
            jobs[job_tuple] = dict()
            for k, v in p[s].items():
                jobs[job_tuple][k] = v
        c['jobs'] = jobs
        return c

    @classmethod
    def defaults(cls):
        return {
            "use-docker-labels": False,
            "save": False,
            "mail": False,
            "slack": False,
            "smtp-host": "",
            "smtp-port": "",
            "smtp-user": "",
            "smtp-password": "",
            "email-to": "",
            "email-from": "",
            "mail-only-on-error": False,
            "save-folder": "",
            "save-only-on-error": False,
            "slack-webhook": "",
            "slack-only-on-error": False,
        }

    @classmethod
    def new_from_defaults(cls):
        return cls(**cls.defaults())


class Ofelia(object):
    __slots__ = ("_container", "_job_containers", "_config")

    def __init__(self, container, use_labels=False, config_file=None):
        self._container = container
        if config_file is not None:
            try:
                self._config = OfeliaConfig.new_from_ini(config_file)
            except Exception as e:
                self._config = OfeliaConfig.new_from_defaults()
        else:
            self._config = OfeliaConfig.new_from_defaults()
        self._config['use-docker-labels'] = self._config['use-docker-labels'] in TRUTHS or use_labels
        self._job_containers = []
        if not self._config['use-docker-labels'] and not config_file:
            raise RuntimeError("Given neither config file nor docker labels. Can't discover jobs to schedule.")
        if config_file:
            self._job_containers.extend(self.find_jobs_by_config())
        if self._config['use-docker-labels']:
            self._job_containers.extend(self.find_job_containers_by_labels())


    def fork_all(self):
        s = self.collect_schedulable_jobs()
        p_set = [j.fork() for j in s]
        time.sleep(1)
        done_set = [p.join() for p in p_set]
        return done_set

    def config(self, key, default=None):
        return self._config.get(key, default)

    @property
    @lru_cache(1)
    def in_docker_compose(self):
        return bool(self._container.labels.get('com.docker.compose.version', False))

    @property
    @lru_cache(1)
    def in_docker_swarm(self):
        return bool(self._container.labels.get('com.docker.swarm.service.id', False))

    @property
    @lru_cache(1)
    def in_docker_stack(self):
        return bool(self._container.labels.get('com.docker.stack.namespace', False))

    @property
    @lru_cache(1)
    def compose_project(self):
        return self._container.labels.get('com.docker.compose.project', None)

    @property
    @lru_cache(1)
    def stack_namespace(self):
        return self._container.labels.get('com.docker.stack.namespace', None)

    @property
    @lru_cache(1)
    def swarm_service_id(self):
        return self._container.labels.get('com.docker.swarm.service.id', None)

    @property
    def swarm_service(self):
        service_id = self.swarm_service_id
        if service_id is None:
            return None
        return self._container.client.services.get(service_id)

    # uncomment for testing fake swarm service
    # @property
    # def swarm_service(self):
    #     l = self._container.client.services.list()
    #     return l[0]

    @property
    @lru_cache(1)
    def do_save(self):
        return self.config('save', False) in TRUTHS

    @property
    @lru_cache(1)
    def do_mail(self):
        return self.config('mail', False) in TRUTHS

    @property
    @lru_cache(1)
    def do_slack(self):
        return self.config('slack', False) in TRUTHS

    def sibling_containers(self):
        s = self.stack_namespace
        p = self.compose_project
        if p is None and s is None:
            print("Warning: Not running within a swarm stack or compose project. Using global namespace.")
        containers = self._container.collection.list()
        for c in containers:
            if c.id == self._container.id:
                continue
            if s is not None and c.labels.get('com.docker.stack.namespace', None) != s:
                continue
            if p is not None and c.labels.get('com.docker.compose.project', None) != p:
                continue
            yield c

    def find_job_containers_by_labels(self):
        j = [LabeledContainer(self, self._container)]
        for s in self.sibling_containers():
            for l in s.labels.keys():
                if l.startswith("ofelia."):
                    j.append(LabeledContainer(self, s))
                    break
        return j

    def find_jobs_by_config(self):
        jobs = self._config['jobs']
        job_containers = {self._container.id: JobContainer(self, self._container)}
        for job_tuple, options in jobs.items():
            job_type, job_name = job_tuple
            if job_type == JobLocal.config_key:
                job_containers[self._container.id].add_job(JobLocal, job_name, kv_args=options.copy())
                continue
            container = None

            if job_type == JobExec.config_key:
                job_type = JobExec
                try:
                    container_name = options['container']
                except:
                    raise RuntimeError("Container name must be specified for job-exec")
                try:
                    container = self._container.collection.get(container_name)
                except:
                    raise RuntimeError("Container specified by job-exec {} not found: {}".format(job_name, container_name))
            elif job_type == JobRun.config_key:
                job_type = JobRun
                try:
                    container_name = options['container']
                    try:
                        container = self._container.collection.get(container_name)
                    except:
                        raise RuntimeError(
                            "Container specified by job-run {} not found: {}".format(job_name, container_name))
                except:
                    # Use the ofelia container in the case of a job-run with an image
                    container = self._container
            elif job_type == JobServiceRun.config_key:
                job_type = JobServiceRun
                # Use the ofelia container in the case of a job-service-run with an image _or_ a service
                container = self._container
            else:
                print("Job type not supported: {}".format(job_type))
                continue
            if container.id not in job_containers:
                job_containers[container.id] = JobContainer(self, container)
            job_containers[container.id].add_job(job_type, job_name, kv_args=options.copy())
        return job_containers.values()

    def collect_schedulable_jobs(self):
        schedulable_jobs = []
        for c in self._job_containers:
            if not c.is_enabled:
                continue
            for j in c.jobs:
                if isinstance(j, ScheduledJob):
                    schedulable_jobs.append(j)
        return schedulable_jobs


def entrypoint():
    util.do_load_dotenv()
    parser = ArgumentParser(prog="daemon", description="PyOfelia commandline options")
    parser.add_argument("-c", "--config", dest="config")
    parser.add_argument('-d', "--docker", dest="docker", action='store_true')
    args = parser.parse_args()
    config = args.config
    client = docker.from_env()
    containers = client.containers.list()
    c = get_own_container(containers)
    o = Ofelia(c, use_labels=args.docker, config_file=config)
    done = o.fork_all()
    sys.exit(0)

if __name__ == "__main__":
    entrypoint()
