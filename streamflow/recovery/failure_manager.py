from __future__ import annotations

import os
import re
import asyncio
import logging
import itertools
from asyncio import Lock
from typing import MutableMapping, MutableSequence, cast

import pkg_resources

from streamflow.core.utils import random_name
from streamflow.core.deployment import Connector, Location
from streamflow.core.exception import (
    FailureHandlingException,
    UnrecoverableTokenException,
)
from streamflow.core.recovery import FailureManager, ReplayRequest, ReplayResponse
from streamflow.core.workflow import (
    CommandOutput,
    Job,
    Status,
    Step,
    Port,
    Token,
    TokenProcessor,
)
from streamflow.cwl.processor import CWLCommandOutput
from streamflow.cwl.step import CWLTransferStep
from streamflow.cwl.token import CWLFileToken
from streamflow.cwl.transformer import CWLTokenTransformer
from streamflow.data import remotepath
from streamflow.log_handler import logger
from streamflow.recovery.recovery import JobVersion
from streamflow.workflow.executor import StreamFlowExecutor
from streamflow.workflow.port import ConnectorPort, JobPort
from streamflow.workflow.step import ExecuteStep, DeployStep
from streamflow.persistence.loading_context import DefaultDatabaseLoadingContext
from streamflow.workflow.token import TerminationToken, JobToken
from streamflow.workflow.utils import get_token_value, contains_file, get_file

# from streamflow.main import build_context
from streamflow.core.context import StreamFlowContext
from streamflow.core.workflow import Workflow


async def _cleanup_dir(
    connector: Connector, location: Location, directory: str
) -> None:
    await remotepath.rm(
        connector, location, await remotepath.listdir(connector, location, directory)
    )





async def _are_data_available(port, files1, context):
    job_key = '__job__'

    missing_steps = []
    for step in port.get_input_steps():
        step_keys = step.input_ports.keys()
        if job_key in step_keys:
            job = step.get_input_port(job_key).token_list[0].value
            allocation = context.scheduler.get_allocation(job.name)
            connector = context.scheduler.get_connector(job.name)
            locations = context.scheduler.get_locations(job.name)
            missing_jobs = []
            for job_token in step.get_input_port(job_key).token_list:
                # print("job_token", job_token, "job", job_token.value)
                if isinstance(job_token, JobToken):
                    files = [ file for token in job_token.value.inputs.values() for file in get_file(token) ]
                #    print("job", job_token.value, "files", files)

                    file_available = []
                    for file in files:
                        file_available.append(
                            asyncio.create_task(
                                remotepath.exists(
                                    connector, locations[0], file['path']
                                )
                            )
                        )
                    ttt = await asyncio.gather(*file_available)
                    if not all(ttt):
                        missing_jobs.append(job_token.job)
            if missing_jobs:
                missing_steps.append((step, missing_jobs))
        else:
            missing_steps.append((step, []))
    return missing_steps


async def _search_lost_steps(original_workflow: Workflow, job_failed: Job, step_failed: Step):
    # ExecuteStep -> __job__ port -> JobPort -> Jobs -> rilancia solo i job con Token persi.
    # Come ancora non so
    # Prima idea:
    # - con _search_lost_steps cerchi gli step
    # - con _create_graph non solo carichi gli step ma verifichi i job. Così crei
    # le port (ListPort in questo caso) adatte al fallimento
    # da _search_lost_steps però mi devo portare dietro l'info dei dati persi altrimenti
    # devo ricontrollarli dentro _create_graph per individuare i Job
    steps_to_check = [(step_failed, [job_failed])]
    steps_to_rollback = {}
    while steps_to_check:
        step, jobs = steps_to_check.pop()
        if step not in steps_to_rollback.keys():
            steps_to_rollback[step] = set()

        for j in jobs:
            steps_to_rollback[step].add(j)
        steps_not_available = []
        for s_port_name, port in step.get_input_ports().items():
            if not isinstance(port, JobPort):
                # files = [
                #     inner_token
                #     for outer_token in port.token_list
                #     for inner_token in get_file(outer_token)
                # ]
#                if files:
                files = None
                missing_steps = await _are_data_available(port, files, original_workflow.context)
                if missing_steps:
                    steps_not_available.extend(missing_steps)
        steps_to_check.extend(steps_not_available)
    return steps_to_rollback





async def _create_graph(
    original_step: Step, context, loading_context, new_workflow: Workflow
):
    if isinstance(original_step, DeployStep):
        port = original_step.get_output_port()
        new_workflow.add_port(port)
        return []

    input_ports_loaded = []
    output_ports_loaded = []
    step_loaded = await loading_context.load_step(context, original_step.persistent_id)
    step_loaded.workflow = new_workflow
    step_loaded.status = Status.WAITING
    step_loaded.terminated = False
    for in_port in original_step.get_input_ports().values():
        if not isinstance(
            in_port, ConnectorPort
        ):  # it used the ConnectorPort of original workflow
            input_ports_loaded.append(
                await loading_context.load_port(context, in_port.persistent_id)
            )
            input_ports_loaded[-1].workflow = new_workflow
    for out_port in original_step.get_output_ports().values():
        output_ports_loaded.append(
            await loading_context.load_port(context, out_port.persistent_id)
        )
        output_ports_loaded[-1].workflow = new_workflow
    new_workflow.add_step(step_loaded)
    for port in input_ports_loaded + output_ports_loaded:
        if port.name not in new_workflow.ports.keys():
            new_workflow.add_port(port)
    return output_ports_loaded


class DefaultFailureManager(FailureManager):
    def __init__(
        self,
        context: StreamFlowContext,
        max_retries: int | None = None,
        retry_delay: int | None = None,
    ):
        super().__init__(context)
        self.max_retries: int = max_retries
        self.retry_delay: int | None = retry_delay
        self.wait_queues: MutableMapping[str, asyncio.Condition] = {}

        self.jobs: MutableMapping[str, JobVersion] = {}
        self.rollback_lock: Lock = Lock()
        self.workflow_running = []
        self.wait_queues: MutableMapping[str, asyncio.Condition] = {}

        # TODO: correggere attributi
        # self.workflows    # tutti i workflow che si stanno rieseguendo
        # self.wait         # step che sono falliti ma stanno aspettando che degli step particolari in esecuzione su altri workflow terminino
        # altri jobs falliti che però gli step da cui dipendono sono già in rollback, quindi aspettano che loro terminano per prendere l'output

    async def _do_handle_failure(self, job: Job, step: Step) -> CommandOutput:
        if job.name not in self.jobs:
            self.jobs[job.name] = JobVersion(
                job=Job(
                    name=job.name,
                    workflow_id=job.workflow_id,
                    inputs=dict(job.inputs),
                    input_directory=job.input_directory,
                    output_directory=job.output_directory,
                    tmp_directory=job.tmp_directory,
                ),
                outputs=None,
                step=step,
                version=1,
            )
        while True:
            # Delay rescheduling to manage temporary failures (e.g. connection lost)
            if self.retry_delay is not None:
                await asyncio.sleep(self.retry_delay)

            command_output = await self._replay_job(self.jobs[job.name])
            if command_output.status == Status.FAILED:
                logger.error(
                    f"FAILED Job {job.name} version {self.jobs[job.name].version} with error:\n\t{command_output.value}"
                )
            else:
                return command_output

    async def _are_available_job_data(
        self, job: Job, connector: Connector, location: Location
    ) -> bool:
        directories = [job.input_directory, job.output_directory, job.tmp_directory]

        # check if directories exist
        exists_tasks = []
        for directory in directories:
            exists_tasks.append(
                asyncio.create_task(remotepath.exists(connector, location, directory))
            )
        exists_results = await asyncio.gather(*exists_tasks)

        # if the input directory exists, check if all its data exists
        # if the output or tmp directory exists, clean it up
        cleanup_tasks = []
        inputs_path_exist = []
        for exists, directory in zip(exists_results, directories):
            if exists:
                if directory == job.input_directory:
                    for token in job.inputs.items():
                        if isinstance(token, CWLFileToken):
                            inputs_path_exist.append(
                                asyncio.create_task(
                                    remotepath.exists(
                                        connector, location, token.value.path
                                    )
                                )
                            )
                else:
                    cleanup_tasks.append(
                        asyncio.create_task(
                            _cleanup_dir(connector, location, directory)
                        )
                    )
        await asyncio.gather(*cleanup_tasks)

        # input directory does not exist or missing some input file
        if not exists_results[0] or not all(await asyncio.gather(*inputs_path_exist)):
            logger.debug(
                f"Handling failure job {job.name}: invalidating all data in the {location} location"
            )
            self.context.data_manager.invalidate_location(location, "/")
            return False
        return True

    async def _recover_data(self, job_version: JobVersion, job: Job):
        workflow = job_version.step.workflow
        step_failed = job_version.step
        context = (
            step_failed.workflow.context
        )  # build_context(workflow.context.config_dir, workflow.context
        # TODO: usare stesso context così i dati vengono salvati nel data_manager. Però da vedere se rilanciare step darà problemi in qualche modulo (data, scheduling, ...)

        new_workflow = Workflow(
            context=context, type="cwl", name=random_name(), config=workflow.config
        )
        loading_context = DefaultDatabaseLoadingContext()

        inner_output_ports = []
        async with self.rollback_lock:
            rollback_steps = await _search_lost_steps(workflow, job, step_failed)
            for curr_step in rollback_steps.keys():
                for output_port in await _create_graph(
                    curr_step, context, loading_context, new_workflow
                ):
                    if output_port not in inner_output_ports:
                        inner_output_ports.append(output_port)
            self.workflow_running.append(new_workflow)

        job_ports = {}
        tags = set()
        for step, jobs in rollback_steps.items():
            job_ports[step.get_input_port("__job__").name] = jobs
            for j in jobs:
                for token in j.inputs.values():
                    tags.add(token.tag)
        # fix ports queue and tokens
        for port in new_workflow.ports.values():
            # reset queue in the ConnectorPort of steps to rollback
            if isinstance(port, ConnectorPort):
                for input_step in port.queues.keys():
                    if input_step in new_workflow.steps.keys():
                        port.reset(input_step)

            elif isinstance(port, JobPort):
                for j in job_ports[port.name]:
                    port.put(JobToken(j))

            # if the port is not the output port of any step then its data are available
            # add the tokens taken from original workflow
            elif port not in inner_output_ports:
                for token in workflow.ports[port.name].token_list:
                    if token.tag in tags or isinstance(token, TerminationToken):
                        port.put(token)

        # the output port is the same port of the step failed
        # for key, port in (
        #     new_workflow.steps[step_failed.name].get_output_ports().items()
        # ):
        #     new_workflow.output_ports[key] = port.name

        # TODO: agganciare le porte di output del workflow con le porte di output dello step che si sta rieseguendo
        print("VIAAAAAAAAAAAAAA")
        executor = StreamFlowExecutor(new_workflow)
        output_tokens = await executor.run()
        async with self.rollback_lock:
            self.workflow_running.remove(new_workflow)
        print("Finito")
        return CWLCommandOutput(value="", status=Status.COMPLETED, exit_code=0)

    async def _replay_job(self, job_version: JobVersion) -> CommandOutput:
        job = job_version.job
        # Retry job execution until the max number of retries is reached
        if self.max_retries is None or self.jobs[job.name].version < self.max_retries:
            # Update version
            self.jobs[job.name].version += 1

            # ciclo step
            # transformer(input)
            # transfer(input)
            # schedule
            # execute

            # TODO:
            # [x] invalida dati
            # [] libera le risorse
            # [] recupera i dati di input
            # [] rilancia job

            # invalida dati
            # Se la risorsa è morta then
            #   - invalidare nel data_manager tutti i dati mappati lì
            # otherwise
            #   - eliminare i dati inutilizzabili (output e tmp dir)

            # risorsa morta -> libera le risorse
            # aggiungi stato Recovering
            # e notify_status notifica tutti (situazione diversa del completed e failed)

            # v1 risorsa morta -> recupera i dati di input
            # - crea un workflow
            # - controlla le port
            # - se i dati esistono ok
            # - altrimenti ricarica step precedente
            # v0 risorsa morta -> recupera i dati di input
            # - cerco i dati di input nella port che me li ha passati
            # - se mancano dati di input fai rollback
            # - aggiorna port (poi quando hai il nuovo deploy copi e aggiorna token value)

            # rilancia job
            # - se lo step possiede alrecupera i dati di inputtre deployment/location disponibili,
            #     * then usare una di quelle
            #     * otherwise, aspetta/rialza la risorsa
            # risorsa morta -> copio i dati nella nuova risorsa
            # rilancio

            try:
                # Notify job failure
                # await self.context.scheduler.notify_status(
                #   job.name, Status.FAILED
                # )

                # Manage job rescheduling
                connector = self.context.scheduler.get_connector(job.name).connector
                active_locations = self.context.scheduler.get_locations(
                    job.name, [Status.RUNNING]
                )

                # TODO: sistemare le location
                allocation = self.context.scheduler.get_allocation(job.name)
                locations = self.context.scheduler.get_locations(job.name)
                available_locations = await connector.get_available_locations(
                    service=allocation.target.service
                )

                return await self._recover_data(job_version, job)

                # if not await self._are_available_job_data(job, connector, active_locations[0]):
                #     return await self._recover_data(job_version, job)

                # update target (tipo se ci sono più target, mettere quella appena usata all'ultimo)
                # schedule the job
                # data are available, is possible execute directly the job
            #    return await cast(ExecuteStep, job_version.step).command.execute(job)
            # When receiving a FailureHandlingException, simply fail
            except FailureHandlingException as e:
                logger.exception(e)
                raise
            # When receiving a KeyboardInterrupt, propagate it (to allow debugging)
            except KeyboardInterrupt:
                raise
            except Exception as e:
                logger.exception(e)
                return await self.handle_exception(job, job_version.step, e)
        else:
            logger.error(
                f"FAILED Job {job.name} {self.jobs[job.name].version} times. Execution aborted"
            )
            raise FailureHandlingException()

    async def close(self):
        pass

    @classmethod
    def get_schema(cls) -> str:
        return pkg_resources.resource_filename(
            __name__, os.path.join("schemas", "default_failure_manager.json")
        )

    async def handle_exception(
        self, job: Job, step: Step, exception: BaseException
    ) -> CommandOutput:
        if logger.isEnabledFor(logging.INFO):
            logger.info(
                f"Handling {type(exception).__name__} failure for job {job.name}"
            )
        return await self._do_handle_failure(job, step)

    async def handle_failure(
        self, job: Job, step: Step, command_output: CommandOutput
    ) -> CommandOutput:
        if logger.isEnabledFor(logging.INFO):
            logger.info(f"Handling command failure for job {job.name}")
        return await self._do_handle_failure(job, step)


class DummyFailureManager(FailureManager):
    async def close(self):
        ...

    @classmethod
    def get_schema(cls) -> str:
        return pkg_resources.resource_filename(
            __name__, os.path.join("schemas", "dummy_failure_manager.json")
        )

    async def handle_exception(
        self, job: Job, step: Step, exception: BaseException
    ) -> CommandOutput:
        raise exception

    async def handle_failure(
        self, job: Job, step: Step, command_output: CommandOutput
    ) -> CommandOutput:
        return command_output
