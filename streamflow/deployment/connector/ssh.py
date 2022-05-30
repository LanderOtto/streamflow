from __future__ import annotations

import asyncio
import contextlib
import os
import posixpath
import stat
import tarfile
import tempfile
from asyncio import Semaphore, Lock
from asyncio.subprocess import STDOUT
from pathlib import PurePosixPath
from typing import MutableSequence, Optional, MutableMapping, Tuple, Any, Union

import asyncssh
from asyncssh import SSHClientConnection
from cachetools import Cache, LRUCache
from jinja2 import Template

from streamflow.core import utils
from streamflow.core.asyncache import cachedmethod
from streamflow.core.context import StreamFlowContext
from streamflow.core.exception import WorkflowExecutionException
from streamflow.core.scheduling import Location, Hardware
from streamflow.data import aiotarstream
from streamflow.data.aiotarstream import StreamReaderWrapper, StreamWriterWrapper
from streamflow.deployment.connector.base import BaseConnector
from streamflow.log_handler import logger


def _parse_hostname(hostname):
    if ':' in hostname:
        hostname, port = hostname.split(':')
        port = int(port)
    else:
        port = 22
    return hostname, port


class SSHContext(object):

    def __init__(self,
                 streamflow_config_dir: str,
                 config: SSHConfig,
                 max_concurrent_sessions: int):
        self._streamflow_config_dir: str = streamflow_config_dir
        self._config: SSHConfig = config
        self._ssh_connection: Optional[SSHClientConnection] = None
        self._connection_lock: Lock = Lock()
        self._sem: Semaphore = Semaphore(max_concurrent_sessions)

    async def __aenter__(self):
        async with self._connection_lock:
            if self._ssh_connection is None:
                self._ssh_connection = await self._get_connection(self._config)
        await self._sem.acquire()
        return self._ssh_connection

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self._sem.release()

    async def _get_connection(self,
                              config: SSHConfig):
        if config is None:
            return None
        (hostname, port) = _parse_hostname(config.hostname)
        passphrase = (self._get_param_from_file(config.ssh_key_passphrase_file)
                      if config.ssh_key_passphrase_file else None)
        password = self._get_param_from_file(config.password_file) if config.password_file else None
        return await asyncssh.connect(
            client_keys=config.client_keys,
            compression_algs=None,
            encryption_algs=[
                "aes128-gcm@openssh.com",
                "aes256-ctr",
                "aes192-ctr",
                "aes128-ctr"
            ],
            known_hosts=() if config.check_host_key else None,
            host=hostname,
            passphrase=passphrase,
            password=password,
            port=port,
            tunnel=await self._get_connection(config.tunnel),
            username=config.username)

    def _get_param_from_file(self,
                             file_path: str):
        if not os.path.isabs(file_path):
            file_path = os.path.join(self._streamflow_config_dir, file_path)
        with open(file_path) as f:
            return f.read().strip()


class SSHConfig(object):

    def __init__(self,
                 check_host_key: bool,
                 client_keys: MutableSequence[str],
                 hostname: str,
                 password_file: Optional[str],
                 ssh_key_passphrase_file: Optional[str],
                 tunnel: Optional[SSHConfig],
                 username: str):
        self.check_host_key: bool = check_host_key
        self.client_keys: MutableSequence[str] = client_keys
        self.hostname: str = hostname
        self.password_file: Optional[str] = password_file
        self.ssh_key_passphrase_file: Optional[str] = ssh_key_passphrase_file
        self.tunnel: Optional[SSHConfig] = tunnel
        self.username: str = username


class SSHConnector(BaseConnector):

    @staticmethod
    def _get_command(location: str,
                     command: MutableSequence[str],
                     environment: MutableMapping[str, str] = None,
                     workdir: Optional[str] = None,
                     stdin: Optional[Union[int, str]] = None,
                     stdout: Union[int, str] = asyncio.subprocess.STDOUT,
                     stderr: Union[int, str] = asyncio.subprocess.STDOUT,
                     job_name: Optional[str] = None,
                     encode: bool = True):
        command = utils.create_command(
            command=command,
            environment=environment,
            workdir=workdir,
            stdin=stdin,
            stdout=stdout,
            stderr=stderr)
        logger.debug("Executing command {command} on {location} {job}".format(
            command=command,
            location=location,
            job="for job {job}".format(job=job_name) if job_name else ""))
        return utils.encode_command(command) if encode else command

    def __init__(self,
                 deployment_name: str,
                 context: StreamFlowContext,
                 nodes: MutableSequence[Any],
                 username: str,
                 checkHostKey: bool = True,
                 dataTransferConnection: Optional[Union[str, MutableMapping[str, Any]]] = None,
                 file: Optional[str] = None,
                 maxConcurrentSessions: int = 10,
                 passwordFile: Optional[str] = None,
                 sharedPaths: Optional[MutableSequence[str]] = None,
                 sshKey: Optional[str] = None,
                 sshKeyPassphraseFile: Optional[str] = None,
                 tunnel: Optional[MutableMapping[str, Any]] = None,
                 transferBufferSize: int = 2 ** 16) -> None:
        super().__init__(
            deployment_name=deployment_name,
            context=context,
            transferBufferSize=transferBufferSize)
        if file is not None:
            with open(os.path.join(context.config_dir, file)) as f:
                self.template: Optional[Template] = Template(f.read())
        else:
            self.template: Optional[Template] = None
        self.checkHostKey: bool = checkHostKey
        self.passwordFile: Optional[str] = passwordFile
        self.maxConcurrentSessions: int = maxConcurrentSessions
        self.sharedPaths: MutableSequence[str] = sharedPaths or []
        self.sshKey: Optional[str] = sshKey
        self.sshKeyPassphraseFile: Optional[str] = sshKeyPassphraseFile
        self.ssh_contexts: MutableMapping[str, SSHContext] = {}
        self.data_transfer_contexts: MutableMapping[str, SSHContext] = {}
        self.username: str = username
        self.tunnel: Optional[SSHConfig] = self._get_config(tunnel)
        self.dataTransferConfig: Optional[SSHConfig] = self._get_config(
            dataTransferConnection)
        self.nodes: MutableMapping[str, SSHConfig] = {n.hostname: n for n in [self._get_config(n) for n in nodes]}
        self.hardwareCache: Cache = LRUCache(maxsize=len(self.nodes))

    async def _build_helper_file(self,
                                 command: str,
                                 location: str,
                                 environment: MutableMapping[str, str] = None,
                                 workdir: str = None) -> str:
        helper_file = tempfile.mktemp()
        with open(helper_file, mode='w') as f:
            f.write(self.template.render(
                streamflow_command="sh -c '{command}'".format(command=command),
                streamflow_environment=environment,
                streamflow_workdir=workdir))
        os.chmod(helper_file, os.stat(helper_file).st_mode | stat.S_IEXEC)
        remote_path = posixpath.join(workdir or '/tmp', os.path.basename(helper_file))
        await self._copy_local_to_remote(
            src=helper_file,
            dst=remote_path,
            locations=[location])
        return remote_path

    async def _copy_local_to_remote_single(self,
                                           src: str,
                                           dst: str,
                                           location: str,
                                           read_only: bool = False):
        async with self._get_ssh_client(location) as ssh_client:
            async with ssh_client.create_process(
                    "tar xf - -C /",
                    encoding=None) as proc:
                try:
                    async with aiotarstream.open(
                            stream=StreamWriterWrapper(proc.stdin),
                            format=tarfile.GNU_FORMAT,
                            mode='w',
                            dereference=True,
                            copybufsize=self.transferBufferSize) as tar:
                        await tar.add(src, arcname=dst)
                except tarfile.TarError as e:
                    raise WorkflowExecutionException("Error copying {} to {} on location {}: {}".format(
                        src, dst, location, str(e))) from e

    async def _copy_remote_to_local(self,
                                    src: str,
                                    dst: str,
                                    location: str,
                                    read_only: bool = False) -> None:
        async with self._get_ssh_client(location) as ssh_client:
            async with ssh_client.create_process(
                    "tar chf - -C / " + posixpath.relpath(src, '/'),
                    stdin=asyncio.subprocess.DEVNULL,
                    encoding=None) as proc:
                try:
                    async with aiotarstream.open(
                            stream=StreamReaderWrapper(proc.stdout),
                            mode='r',
                            copybufsize=self.transferBufferSize) as tar:
                        await utils.extract_tar_stream(tar, src, dst, self.transferBufferSize)
                except tarfile.TarError as e:
                    raise WorkflowExecutionException("Error copying {} from location {} to {}: {}".format(
                        src, location, dst, str(e))) from e

    async def _copy_remote_to_remote(self,
                                     src: str,
                                     dst: str,
                                     locations: MutableSequence[str],
                                     source_location: str,
                                     read_only: bool = False) -> None:
        if source_location in locations:
            if src != dst:
                command = ['/bin/cp', "-rf", src, dst]
                await self.run(source_location, command)
                locations.remove(source_location)
        if locations:
            with contextlib.AsyncExitStack() as exit_stack:
                reader_client = await exit_stack.enter_async_context(self._get_ssh_client(source_location))
                # Open source StreamReader
                dirname, basename = posixpath.split(src)
                async with reader_client.create_process(
                        "tar chf - -C {} {}".format(dirname, basename),
                        stdin=asyncio.subprocess.DEVNULL,
                        encoding=None) as reader:
                    # Open a target StreamWriter for each location
                    writer_clients = await asyncio.gather(*(asyncio.create_task(
                        exit_stack.enter_async_context(location)) for location in locations))
                    with contextlib.AsyncExitStack() as writers_stack:
                        writers = await asyncio.gather(*(asyncio.create_task(
                            writers_stack.enter_async_context(
                                client.create_process("tar xf - -C " + posixpath.dirname(dst), encoding=None))
                            for client in writer_clients)))
                        # Multiplex the reader output to all the writers
                        while content := await reader.stdout.read(self.transferBufferSize):
                            for writer in writers:
                                writer.stdin.write(content)
                            await asyncio.gather(*(asyncio.create_task(writer.stdin.drain()) for writer in writers))

    def _get_config(self,
                    node: Union[str, MutableMapping[str, Any]]):
        if node is None:
            return None
        elif isinstance(node, str):
            node = {'hostname': node}
        ssh_key = node['sshKey'] if 'sshKey' in node else self.sshKey
        return SSHConfig(
            hostname=node['hostname'],
            username=node['username'] if 'username' in node else self.username,
            check_host_key=node['checkHostKey'] if 'checkHostKey' in node else self.checkHostKey,
            client_keys=[ssh_key] if ssh_key is not None else [],
            password_file=node['passwordFile'] if 'passwordFile' in node else self.passwordFile,
            ssh_key_passphrase_file=(node['sshKeyPassphraseFile'] if 'sshKeyPassphraseFile' in node else
                                     self.sshKeyPassphraseFile),
            tunnel=(self._get_config(node['tunnel']) if 'tunnel' in node else
                    self.tunnel if hasattr(self, 'tunnel') else
                    None))

    def _get_data_transfer_client(self, location: str):
        if self.dataTransferConfig:
            if location not in self.data_transfer_contexts:
                self.data_transfer_contexts[location] = SSHContext(
                    streamflow_config_dir=self.context.config_dir,
                    config=self.dataTransferConfig,
                    max_concurrent_sessions=self.maxConcurrentSessions)
            return self.data_transfer_contexts[location]
        else:
            return self._get_ssh_client(location)

    async def _get_existing_parent(self, location: str, directory: str):
        command_template = "test -e \"{path}\""
        async with self._get_ssh_client(location) as ssh_client:
            while True:
                result = await ssh_client.run(command_template.format(path=directory), stderr=STDOUT)
                if result.returncode == 0:
                    return directory
                elif result.returncode == 1:
                    directory = PurePosixPath(directory).parent
                else:
                    raise WorkflowExecutionException(result.stdout.strip())

    def _get_run_command(self,
                         command: str,
                         location: str,
                         interactive: bool = False):
        return "".join([
            "ssh ",
            "{location} ",
            "{command}"
        ]).format(
            location=location,
            command=command)

    def _get_ssh_client(self, location: str):
        if location not in self.ssh_contexts:
            self.ssh_contexts[location] = SSHContext(
                streamflow_config_dir=self.context.config_dir,
                config=self.nodes[location],
                max_concurrent_sessions=self.maxConcurrentSessions)
        return self.ssh_contexts[location]

    async def _run(self,
                   location: str,
                   command: MutableSequence[str],
                   environment: MutableMapping[str, str] = None,
                   workdir: Optional[str] = None,
                   stdin: Optional[Union[int, str]] = None,
                   stdout: Union[int, str] = asyncio.subprocess.STDOUT,
                   stderr: Union[int, str] = asyncio.subprocess.STDOUT,
                   job_name: Optional[str] = None,
                   capture_output: bool = False,
                   encode: bool = True,
                   interactive: bool = False,
                   stream: bool = False) -> Union[Optional[Tuple[Optional[Any], int]], asyncio.subprocess.Process]:
        command = self._get_command(
            location=location,
            command=command,
            environment=environment,
            workdir=workdir,
            stdin=stdin,
            stdout=stdout,
            stderr=stderr,
            encode=encode,
            job_name=job_name)
        if job_name is not None and self.template is not None:
            helper_file = await self._build_helper_file(command, location, environment, workdir)
            async with self._get_ssh_client(location) as ssh_client:
                result = await ssh_client.run(helper_file, stderr=STDOUT)
        else:
            async with self._get_ssh_client(location) as ssh_client:
                result = await ssh_client.run("sh -c '{command}'".format(command=command), stderr=STDOUT)
        if capture_output:
            return result.stdout.strip(), result.returncode

    @cachedmethod(lambda self: self.hardwareCache)
    async def _get_location_hardware(self,
                                     location: str,
                                     input_directory: str,
                                     output_directory: str,
                                     tmp_directory: str) -> Hardware:
        async with self._get_ssh_client(location) as ssh_client:
            cores, memory, input_directory, output_directory, tmp_directory = await asyncio.gather(
                ssh_client.run("nproc", stderr=STDOUT),
                ssh_client.run("free | grep Mem | awk '{print $2}'", stderr=STDOUT),
                ssh_client.run("df {} | tail -n 1 | awk '{{print $2}}'".format(input_directory), stderr=STDOUT),
                ssh_client.run("df {} | tail -n 1 | awk '{{print $2}}'".format(output_directory), stderr=STDOUT),
                ssh_client.run("df {} | tail -n 1 | awk '{{print $2}}'".format(tmp_directory), stderr=STDOUT))
            if (cores.returncode == 0 and
                    memory.returncode == 0 and
                    input_directory.returncode == 0 and
                    output_directory.returncode == 0 and
                    tmp_directory.returncode == 0):
                return Hardware(
                    cores=float(cores.stdout.strip()),
                    memory=float(memory.stdout.strip()) / 2 ** 10,
                    input_directory=float(input_directory.stdout.strip()) / 2 ** 10,
                    output_directory=float(output_directory.stdout.strip()) / 2 ** 10,
                    tmp_directory=float(tmp_directory.stdout.strip()) / 2 ** 10)
            else:
                raise WorkflowExecutionException(
                    "Impossible to retrieve locations for {location}".format(location=location))

    async def deploy(self, external: bool) -> None:
        pass

    async def get_available_locations(self,
                                      service: str,
                                      input_directory: str,
                                      output_directory: str,
                                      tmp_directory: str) -> MutableMapping[str, Location]:
        locations = {}
        for location_obj in self.nodes.values():
            inpdir, outdir, tmpdir = await asyncio.gather(
                self._get_existing_parent(location_obj.hostname, input_directory),
                self._get_existing_parent(location_obj.hostname, output_directory),
                self._get_existing_parent(location_obj.hostname, tmp_directory))
            hardware = await self._get_location_hardware(
                location=location_obj.hostname,
                input_directory=inpdir,
                output_directory=outdir,
                tmp_directory=tmpdir)
            locations[location_obj.hostname] = Location(
                name=location_obj.hostname,
                hostname=location_obj.hostname,
                hardware=hardware)
        return locations

    async def undeploy(self, external: bool) -> None:
        for ssh_context in self.ssh_contexts.values():
            async with ssh_context as ssh_client:
                ssh_client.close()
        self.ssh_contexts = {}
        for ssh_context in self.data_transfer_contexts.values():
            async with ssh_context as ssh_client:
                ssh_client.close()
        self.data_transfer_contexts = {}
