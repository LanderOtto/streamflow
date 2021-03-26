from __future__ import annotations

import asyncio
import base64
import io
import os
import shlex
import shutil
import tarfile
import tempfile
from abc import ABC
from typing import TYPE_CHECKING, MutableSequence, Tuple, cast

from streamflow.core import utils
from streamflow.core.deployment import Connector, ConnectorCopyKind
from streamflow.log_handler import logger

if TYPE_CHECKING:
    from typing import Any, Optional, MutableMapping, Union
    from typing_extensions import Text


class BaseConnector(Connector, ABC):

    @staticmethod
    def get_option(name: Text,
                   value: Any,
                   ) -> Text:
        if len(name) > 1:
            name = "-{name} ".format(name=name)
        if isinstance(value, bool):
            return "-{name} ".format(name=name) if value else ""
        elif isinstance(value, str):
            return "-{name} \"{value}\" ".format(name=name, value=value)
        elif isinstance(value, MutableSequence):
            return "".join(["-{name} \"{value}\" ".format(name=name, value=item) for item in value])
        elif value is None:
            return ""
        else:
            raise TypeError("Unsupported value type")

    def __init__(self,
                 streamflow_config_dir: Text,
                 transferBufferSize: int):
        super().__init__(streamflow_config_dir)
        self.transferBufferSize: int = transferBufferSize

    async def _copy_local_to_remote(self,
                                    src: Text,
                                    dst: Text,
                                    resources: MutableSequence[Text]) -> None:
        with tempfile.TemporaryFile() as tar_buffer:
            with tarfile.open(fileobj=tar_buffer, mode='w') as tar:
                tar.add(src, arcname=dst)
            tar_buffer.seek(0)
            await asyncio.gather(*[asyncio.create_task(
                self._copy_local_to_remote_single(resource, cast(io.BufferedRandom, tar_buffer))
            ) for resource in resources])

    async def _copy_local_to_remote_single(self,
                                           resource: Text,
                                           tar_buffer: io.BufferedRandom) -> None:
        resource_buffer = io.BufferedReader(tar_buffer.raw)
        proc = await self._run(
            resource=resource,
            command=["tar", "xf", "-", "-C", "/"],
            encode=False,
            interactive=True,
            stream=True
        )
        while content := resource_buffer.read(self.transferBufferSize):
            proc.stdin.write(content)
            await proc.stdin.drain()
        proc.stdin.close()
        await proc.wait()

    async def _copy_remote_to_local(self,
                                    src: Text,
                                    dst: Text,
                                    resource: Text) -> None:
        proc = await self._run(
            resource=resource,
            command=["tar", "cPf", "-", src],
            capture_output=True,
            encode=False,
            stream=True)
        with io.BytesIO() as byte_buffer:
            while data := await proc.stdout.read(self.transferBufferSize):
                byte_buffer.write(data)
            await proc.wait()
            utils.create_tar_from_byte_stream(byte_buffer, src, dst)

    async def _copy_remote_to_remote(self,
                                     src: Text,
                                     dst: Text,
                                     resources: MutableSequence[Text],
                                     source_remote: Text) -> None:
        # Check for the need of a temporary copy
        temp_dir = None
        for resource in resources:
            if source_remote != resource:
                temp_dir = tempfile.mkdtemp()
                await self._copy_remote_to_local(src, temp_dir, source_remote)
                break
        # Perform the actual copies
        copy_tasks = []
        for resource in resources:
            copy_tasks.append(asyncio.create_task(
                self._copy_remote_to_remote_single(src, dst, resource, source_remote, temp_dir)))
        await asyncio.gather(*copy_tasks)
        # If a temporary location was created, delete it
        if temp_dir is not None:
            shutil.rmtree(temp_dir)

    async def _copy_remote_to_remote_single(self,
                                            src: Text,
                                            dst: Text,
                                            resource: Text,
                                            source_remote: Text,
                                            temp_dir: Optional[Text]) -> None:
        if source_remote == resource:
            if src != dst:
                command = ['/bin/cp', "-rf", src, dst]
                await self.run(resource, command)
        else:
            copy_tasks = []
            for element in os.listdir(temp_dir):
                copy_tasks.append(asyncio.create_task(
                    self._copy_local_to_remote(os.path.join(temp_dir, element), dst, [resource])))
            await asyncio.gather(*copy_tasks)

    def _get_run_command(self,
                         command: Text,
                         resource: Text,
                         interactive: bool = False):
        raise NotImplementedError

    async def _run(self,
                   resource: Text,
                   command: MutableSequence[Text],
                   environment: MutableMapping[Text, Text] = None,
                   workdir: Optional[Text] = None,
                   stdin: Optional[Union[int, Text]] = None,
                   stdout: Union[int, Text] = asyncio.subprocess.STDOUT,
                   stderr: Union[int, Text] = asyncio.subprocess.STDOUT,
                   capture_output: bool = False,
                   encode: bool = True,
                   interactive: bool = False,
                   stream: bool = False) -> Union[Optional[Tuple[Optional[Any], int]], asyncio.subprocess.Process]:
        command = utils.create_command(
            command, environment, workdir, stdin, stdout, stderr)
        logger.debug("Executing command {command} on {resource}".format(command=command, resource=resource))
        if encode:
            command = utils.encode_command(command)
        run_command = self._get_run_command(command, resource, interactive=interactive)
        proc = await asyncio.create_subprocess_exec(
            *shlex.split(run_command),
            stdin=asyncio.subprocess.PIPE if interactive else None,
            stdout=asyncio.subprocess.PIPE if capture_output else asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE if capture_output else asyncio.subprocess.DEVNULL)
        if stream:
            return proc
        elif capture_output:
            stdout, _ = await proc.communicate()
            return stdout.decode().strip(), proc.returncode
        else:
            await proc.wait()

    async def copy(self,
                   src: Text,
                   dst: Text,
                   resources: MutableSequence[Text],
                   kind: ConnectorCopyKind,
                   source_remote: Optional[Text] = None) -> None:
        if kind == ConnectorCopyKind.REMOTE_TO_REMOTE:
            if source_remote is None:
                raise Exception("Source resource is mandatory for remote to remote copy")
            if len(resources) > 1:
                logger.info("Copying {src} on resource {source_remote} to {dst} on resources:\n\t{resources}".format(
                    source_remote=source_remote,
                    src=src,
                    dst=dst,
                    resources='\n\t'.join(resources)
                ))
            else:
                logger.info("Copying {src} on resource {source_remote} to {dst} on resource {resource}".format(
                    source_remote=source_remote,
                    src=src,
                    dst=dst,
                    resource=resources[0]
                ))
            await self._copy_remote_to_remote(src, dst, resources, source_remote)
        elif kind == ConnectorCopyKind.LOCAL_TO_REMOTE:
            if len(resources) > 1:
                logger.info("Copying {src} on local file-system to {dst} on resources:\n\t{resources}".format(
                    source_remote=source_remote,
                    src=src,
                    dst=dst,
                    resources='\n\t'.join(resources)
                ))
            else:
                logger.info("Copying {src} on local file-system to {dst} on resource {resource}".format(
                    source_remote=source_remote,
                    src=src,
                    dst=dst,
                    resource=resources[0]
                ))
            await self._copy_local_to_remote(src, dst, resources)
        elif kind == ConnectorCopyKind.REMOTE_TO_LOCAL:
            if len(resources) > 1:
                raise Exception("Copy from multiple resources is not supported")
            logger.info("Copying {src} on resource {resource} to {dst} on local file-system".format(
                source_remote=source_remote,
                src=src,
                dst=dst,
                resource=resources[0]
            ))
            await self._copy_remote_to_local(src, dst, resources[0])
        else:
            raise NotImplementedError

    async def run(self,
                  resource: Text,
                  command: MutableSequence[Text],
                  environment: MutableMapping[Text, Text] = None,
                  workdir: Optional[Text] = None,
                  stdin: Optional[Union[int, Text]] = None,
                  stdout: Union[int, Text] = asyncio.subprocess.STDOUT,
                  stderr: Union[int, Text] = asyncio.subprocess.STDOUT,
                  capture_output: bool = False) -> Optional[Tuple[Optional[Any], int]]:
        return await self._run(
            resource=resource,
            command=command,
            environment=environment,
            workdir=workdir,
            stdin=stdin,
            stdout=stdout,
            stderr=stderr,
            capture_output=capture_output)