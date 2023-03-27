from __future__ import annotations

import posixpath
import re
import asyncio
import logging
import os
import itertools
import tempfile
import token
from asyncio import Lock
from typing import MutableMapping, MutableSequence, cast, MutableSet

import pkg_resources

from streamflow.core.context import StreamFlowContext
from streamflow.core.data import DataLocation, DataType
from streamflow.core.utils import random_name, get_class_fullname
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
from streamflow.data.data_manager import RemotePathMapper
from streamflow.log_handler import logger
from streamflow.recovery.recovery import JobVersion
from streamflow.workflow.step import ExecuteStep, ScatterStep
from streamflow.workflow.executor import StreamFlowExecutor
from streamflow.workflow.port import ConnectorPort, JobPort
from streamflow.workflow.step import (
    ExecuteStep,
    DeployStep,
    ScheduleStep,
    CombinatorStep,
)
from streamflow.persistence.loading_context import DefaultDatabaseLoadingContext
from streamflow.workflow.token import TerminationToken, JobToken, ListToken, ObjectToken

# from streamflow.workflow.utils import get_token_value, get_files_from_token

# from streamflow.main import build_context
from streamflow.core.context import StreamFlowContext
from streamflow.core.workflow import Workflow

# import networkx as nx
# import matplotlib.pyplot as plt
# from dyngraphplot import DynGraphPlot
import graphviz



def add_step(step, steps):
    found = False
    for s in steps:
        found = found or s.name == step.name
    if not found:
        steps.append(step)


def add_pair(step_name, label, step_labels, tokens):
    for curr_step_name, curr_label in step_labels:
        if curr_step_name == step_name and curr_label == label:
            return
    step_labels.append((step_name, label))


def valid_step_name(step_name, sep="_"):
    return step_name
    # return step_name[1:].replace('/', sep).replace('-', sep)


def print_graph_figure(graph, title):
    dot = graphviz.Digraph(title)
    for vertex, neighbors in graph.items():
        dot.node(str(vertex))
        for n in neighbors:
            dot.edge(str(vertex), str(n))
    # print(dot.source)
    dot.view("dev/" + title + ".gv")  # tempfile.mktemp('.gv')


def print_graph_figure_label(graph, title):
    dot = graphviz.Digraph(title)
    for vertex, neighbors in graph.items():
        dot.node(str(vertex))
        for n, l in neighbors:
            dot.edge(str(vertex), str(n), label=str(l))
    # print(dot.source)
    dot.view("dev/" + title + ".gv")  # tempfile.mktemp('.gv')


async def print_graph(job_version, loading_context):
    """
    FUNCTION FOR DEBUGGING
    """
    rows = await job_version.step.workflow.context.database.get_all_provenance()
    tokens = {}
    graph = {}
    for row in rows:
        dependee = (
            await loading_context.load_token(
                job_version.step.workflow.context, row["dependee"]
            )
            if row["dependee"]
            else -1
        )
        depender = (
            await loading_context.load_token(
                job_version.step.workflow.context, row["depender"]
            )
            if row["depender"]
            else -1
        )
        curr_key = dependee.persistent_id if dependee != -1 else -1
        if curr_key not in graph.keys():
            graph[curr_key] = set()
        graph[curr_key].add(depender.persistent_id)
        tokens[depender.persistent_id] = depender
        tokens[curr_key] = dependee

    steps_token = {}
    graph_steps = {}
    for k, values in graph.items():
        if k != -1:
            k_step = (
                await _load_steps_from_token(
                    tokens[k],
                    job_version.step.workflow.context,
                    loading_context,
                    job_version.step.workflow,
                )
            ).pop()
        step_name = k_step.name if k != -1 else INIT_DAG_FLAG
        if step_name not in graph_steps.keys():
            graph_steps[step_name] = set()
        if step_name not in steps_token.keys():
            steps_token[step_name] = set()
        steps_token[step_name].add(k)

        for v in values:
            s = (
                await _load_steps_from_token(
                    tokens[v],
                    job_version.step.workflow.context,
                    loading_context,
                    job_version.step.workflow,
                )
            ).pop()
            graph_steps[step_name].add(s.name)
            if s.name not in steps_token.keys():
                steps_token[s.name] = set()
            steps_token[s.name].add(v)

    valid_steps_graph = {}
    for step_name_1, steps_name in graph_steps.items():
        valid_steps_graph[step_name_1] = []
        for step_name_2 in steps_name:
            for label in steps_token[step_name_1]:
                add_pair(
                    step_name_2,
                    str_token_value(tokens[label]) + f"({label})",
                    valid_steps_graph[step_name_1],
                    tokens,
                )

    print_graph_figure_label(valid_steps_graph, "get_all_provenance_steps")
    wf_steps = sorted(job_version.step.workflow.steps.keys())
    pass


def str_token_value(token):
    if isinstance(token, CWLFileToken):
        return token.value["class"]  # token.value['path']
    if isinstance(token, ListToken):
        return str([str_token_value(t) for t in token.value])
    if isinstance(token, JobToken):
        return token.value.name
    if isinstance(token, TerminationToken):
        return "T"
    if isinstance(token, Token):
        return str(token.value)
    return "None"


def search_step_name_into_graph(graph_tokens):
    for tokens in graph_tokens.values():
        for s in tokens:
            if isinstance(s, str) and s != INIT_DAG_FLAG:
                return s
    raise Exception("Step name non trovato")

async def printa_token(token_visited, workflow, graph_tokens, loading_context):
    token_values = {}
    for token_id, (token, _) in token_visited.items():
        token_values[token_id] = str_token_value(token)
    token_values[INIT_DAG_FLAG] = INIT_DAG_FLAG
    step_name = search_step_name_into_graph(graph_tokens)
    token_values[step_name] = step_name

    graph_steps = {}
    for token_id, tokens_id in graph_tokens.items():
        step_1 = (
            (
                await _load_steps_from_token(
                    token_visited[token_id][0],
                    workflow.context,
                    loading_context,
                    workflow,
                )
            )
            .pop()
            .name
            if isinstance(token_id, int)
            else token_values[token_id]
        )
        steps_2 = set()
        label = (
            str_token_value(token_visited[token_id][0]) + f"({token_id})"
            if isinstance(token_id, int)
            else token_values[token_id]
        )
        for token_id_2 in tokens_id:
            step_2 = (
                (
                    await _load_steps_from_token(
                        token_visited[token_id_2][0],
                        workflow.context,
                        loading_context,
                        workflow,
                    )
                )
                .pop()
                .name
                if isinstance(token_id_2, int)
                else token_values[token_id_2]
            )
            steps_2.add(step_2)
        if step_1 != INIT_DAG_FLAG:
            graph_steps[step_1] = [(s, label) for s in steps_2]
    print_graph_figure_label(graph_steps, "graph_steps recovery")

async def _cleanup_dir(
    connector: Connector, location: Location, directory: str
) -> None:
    await remotepath.rm(
        connector, location, await remotepath.listdir(connector, location, directory)
    )

async def _load_prev_tokens(token_id, loading_context, context):
    rows = await context.database.get_dependee(token_id)

    return await asyncio.gather(
        *(
            asyncio.create_task(loading_context.load_token(context, row["dependee"]))
            for row in rows
        )
    )


async def _load_steps_from_token(token, context, loading_context, new_workflow):
    # TODO: quando interrogo sulla tabella dependency (tra step e port) meglio recuperare anche il nome della dipendenza
    # così posso associare subito token -> step e a quale porta appartiene
    # edit. Impossibile. Recuperiamo lo step dalla porta di output. A noi serve la port di input
    row_token = await context.database.get_token(token.persistent_id)
    steps = []
    if row_token:
        row_steps = await context.database.get_step_from_output_port(row_token["port"])
        for r in row_steps:
            st = await Step.load(
                context,
                r["step"],
                loading_context,
                new_workflow,
            )
            steps.append(
                st,
            )
            # due modi alternativi per ottenre il nome della output_port che genera il token in questione
            #    [ op.name for op in workflow.steps[st.name].output_ports if op.persistent_id == int(row_token['port'])][0]
            # (await context.database.get_port(row_token['port']))['name']

    return steps


async def _load_ports_from_token(token, context, loading_context):
    ports_id = await context.database.get_token_ports(token.persistent_id)
    # TODO: un token ha sempre una port? Da verificare
    if ports_id:
        return await asyncio.gather(
            *(
                asyncio.create_task(loading_context.load_port(context, port_id))
                for port_id in ports_id
            )
        )
    return None


async def data_location_exists(data_locations, context, token):
    for data_loc in data_locations:
        if data_loc.path == token.value["path"]:
            connector = context.deployment_manager.get_connector(data_loc.deployment)
            # location_allocation = job_version.step.workflow.context.scheduler.location_allocations[data_loc.deployment][data_loc.name]
            # available_locations = job_version.step.workflow.context.scheduler.get_locations(location_allocation.jobs[0])
            if exists := await remotepath.exists(
                connector, data_loc, token.value["path"]
            ):
                return True
            print(
                f"t {token.persistent_id} ({get_class_fullname(type(token))}) in loc {data_loc} -> exists {exists} "
            )
    return False




def _get_data_location(path, context):
    data_locs = context.data_manager.get_data_locations(path)
    for data_loc in data_locs:
        if data_loc.path == path:
            return data_loc
    return None


async def is_token_available(token, context):
    if isinstance(token, CWLFileToken):
        # data_locs = context.data_manager.get_data_locations(token.value["path"])
        # TODO: è giusto cercare una loc dal suo path? se va bene aggiustare data_location_exists method
        data_loc = _get_data_location(token.value["path"], context)
        if not data_loc:
            return False
        if not await data_location_exists([data_loc], context, token):
            context.data_manager.invalidate_location(data_loc, token.value["path"])
            return False
        return True
    if isinstance(token, ListToken):
        # if at least one file doesn't exist, returns false
        return all(
            await asyncio.gather(
                *(
                    asyncio.create_task(is_token_available(inner_token, context))
                    for inner_token in token.value
                )
            )
        )
    if isinstance(token, ObjectToken):
        # TODO: sistemare
        return True
    if isinstance(token, JobToken):
        return False
    return True


async def _load_and_add_port(token, context, loading_context, curr_dict):
    ports = await _load_ports_from_token(token, context, loading_context)
    # TODO: ports, in teoria, è sempre solo una. Sarebbe la port di output dello step che genera il token
    for p in ports:
        if p.name not in curr_dict.keys():
            curr_dict[p.name] = set()
        curr_dict[p.name].add(token)


def find_step_by_id(step_id, workflow):
    for step in workflow.steps.values():
        if step.persistent_id == step_id:
            return step
    return None



def clean_lists(steps_token, ports_token, token_visited):
    for port_name, p_token in ports_token.items():
        if not token_visited[p_token.persistent_id]:
            ports_token[port_name] = None
    for step_name, s_tokens in steps_token.items():
        to_remove = []
        for s_token in s_tokens:
            if not token_visited[s_token.persistent_id]:
                to_remove.append(s_token)
        for rm_tok in to_remove:
            steps_token[step_name].remove(rm_tok)


async def _put_tokens(
    new_workflow: Workflow,
    workflow: Workflow,
    dag_tokens: MutableMapping[int, MutableSet[int]],
    token_visited: MutableMapping[int, bool],
    loading_context,
):
    token_port = {}
    port_tokens = {}
    # recupero le port associate ai token
    # TODO: doppio lavoro. trovare un modo per ricavarlo quando popolo il workflow.
    for token_id in token_visited.keys():
        rows = await workflow.context.database.get_port_from_token(token_id)
        if rows:
            token_port[token_id] = await loading_context.load_port(
                workflow.context, rows["id"]
            )
        else:
            raise Exception("Token senza porta che lo genera")

        if token_port[token_id].name not in port_tokens.keys():
            port_tokens[token_port[token_id].name] = set()
        port_tokens[token_port[token_id].name].add(token_id)


    # add tokens into the ports
    for token_id in dag_tokens[INIT_DAG_FLAG]:
        port = new_workflow.ports[token_port[token_id].name]
        port.put(token_visited[token_id][0])
        if len(port.token_list) == len(port_tokens[port.name]):
            port.put(TerminationToken())

async def _populate_workflow(
    token_visited,
    workflow,
    new_workflow,
    loading_context,
):
    for token, is_available in token_visited.values():
        if not is_available:
            step = (
                await _load_steps_from_token(
                    token,
                    workflow.context,
                    loading_context,
                    new_workflow,
                )
            ).pop()
            if step and step.name not in new_workflow.steps.keys():
                new_workflow.add_step(step)

    # todo: gestire le port che viene aggiunta mentre si caricano gli step (una JobPort che al momento sovrascriviamo). L'ideale sarebbe che non venga aggiunta
    for step in new_workflow.steps.values():
        for port_name in list(step.input_ports.values()) + list(
            step.output_ports.values()
        ):
            new_workflow.add_port(
                await Port.load(
                    workflow.context,
                    workflow.ports[port_name].persistent_id,
                    loading_context,
                    new_workflow,
                )
            )

INIT_DAG_FLAG = "init"


class DefaultFailureManager(FailureManager):
    def __init__(
        self,
        context: StreamFlowContext,
        max_retries: int | None = None,
        retry_delay: int | None = None,
    ):
        super().__init__(context)
        self.jobs: MutableMapping[str, JobVersion] = {}
        self.max_retries: int = max_retries
        self.replay_cache: MutableMapping[str, ReplayResponse] = {}
        self.retry_delay: int | None = retry_delay
        self.wait_queues: MutableMapping[str, asyncio.Condition] = {}
        self.retags = {}

    async def _do_handle_failure(self, job: Job, step: Step) -> CommandOutput:
        # Delay rescheduling to manage temporary failures (e.g. connection lost)
        if self.retry_delay is not None:
            await asyncio.sleep(self.retry_delay)
        if job.name not in self.jobs:
            self.jobs[job.name] = JobVersion(
                job=Job(
                    name=job.name,
                    workflow_id=step.workflow.persistent_id,
                    inputs=dict(job.inputs),
                    input_directory=job.input_directory,
                    output_directory=job.output_directory,
                    tmp_directory=job.tmp_directory,
                ),
                outputs=None,
                step=step,
                version=1,
            )
        command_output = await self._replay_job(self.jobs[job.name])
        return command_output

    # TODO: aggiungere nella classe dummy.
    def get_retag(self, workflow_name, token, output_port):
        if workflow_name not in self.retags.keys():
            return True
        if output_port.name not in self.retags[workflow_name].keys():
            return True

        tokens = self.retags[workflow_name][output_port.name]
        for t in tokens:
            if type(token) == type(t):
                if isinstance(token, CWLFileToken):
                    # un confronto un po' semplicistico.
                    if token.value["basename"] == t.value["basename"]:
                        return False
                elif isinstance(token, ListToken):
                    # TODO: WIP
                    raise Exception(f"Not implemented (ListToken)")
                elif isinstance(token, ObjectToken):
                    # TODO: WIP
                    raise Exception(f"Not implemented (ObjectToken)")
                elif isinstance(token, JobToken):
                    # scatter sui jobtoken non dovrebbe essere un caso possibile
                    raise Exception(f"In port {output_port} can have JobToken")
                else:
                    # ok su workflow "normali", in token troviamo oggetti gestibili come strings e numbers.
                    # Nel caso di workflow particolari possiamo avere oggetti strani e questo confronto non va assolutamente bene
                    return token.value == t.value
            else:
                raise Exception(
                    f"For port {output_port}: token {token} and tokens saved for fault tolerance have different types"
                )
        return True

    def _save_for_retag(self, workflow, new_workflow, dag_token):
        if new_workflow.name not in self.retags.keys():
            self.retags[new_workflow.name] = {}
        for s in new_workflow.steps.values():
            if isinstance(s, ScatterStep):
                port = s.get_output_port()
                for t in workflow.ports[port.name].token_list:
                    if not isinstance(t, TerminationToken):
                        if t.persistent_id in dag_token:
                            pass
                        else:
                            if port.name not in self.retags[new_workflow.name].keys():
                                self.retags[new_workflow.name][port.name] = set()
                            self.retags[new_workflow.name][port.name].add(t)

    async def _recover_jobs(self, job_version, loading_context):
        await print_graph(job_version, loading_context)

        workflow = job_version.step.workflow
        new_workflow = Workflow(
            context=workflow.context,
            type="cwl",
            name=random_name(),
            config=workflow.config,
        )

        tokens = set(job_version.job.inputs.values())  # tokens to check
        tokens.add(job_version.step.get_job_token(job_version.job))

        dag_token = {}  # token.id -> set of next tokens id
        for t in job_version.job.inputs.values():
            dag_token[t.persistent_id] = set((job_version.step.name,))

        token_visited = {}  # { token_id: (token, is_available)}

        new_workflow.add_step(
            await Step.load(
                workflow.context,
                job_version.step.persistent_id,
                loading_context,
                new_workflow,
            )
        )
        while tokens:
            token = tokens.pop()
            res = await is_token_available(token, workflow.context)
            # controllo inutile. Aggiungo in tokens solo token che non sono stati visitati
            if token.persistent_id not in token_visited.keys():
                token_visited[token.persistent_id] = (token, res)
            else:
                raise Exception("DOPPIONEEEE")

            if not res:
                prev_tokens = await _load_prev_tokens(
                    token.persistent_id,
                    loading_context,
                    workflow.context,
                )
                if not prev_tokens:
                    if INIT_DAG_FLAG not in dag_token.keys():
                        dag_token[INIT_DAG_FLAG] = set()
                    dag_token[INIT_DAG_FLAG].add(token.persistent_id)
                for pt in prev_tokens:
                    if pt.persistent_id not in dag_token.keys():
                        dag_token[pt.persistent_id] = set()
                    dag_token[pt.persistent_id].add(token.persistent_id)
                    if pt.persistent_id not in token_visited.keys():
                        tokens.add(pt)
            else:
                if INIT_DAG_FLAG not in dag_token.keys():
                    dag_token[INIT_DAG_FLAG] = set()
                dag_token[INIT_DAG_FLAG].add(token.persistent_id)

        token_visited = dict(sorted(token_visited.items()))
        await printa_token(token_visited, workflow, dag_token, loading_context)
        pass

        await _populate_workflow(
            token_visited,
            workflow,
            new_workflow,
            loading_context,
        )

        pass
        await _put_tokens(
            new_workflow,
            workflow,
            dag_token,
            token_visited,
            loading_context,
        )
        pass

        self._save_for_retag(workflow, new_workflow, dag_token)

        # sblocca scheduler
        for token, _ in token_visited.values():
            if isinstance(token, JobToken):
                await workflow.context.scheduler.notify_status(
                    token.value.name, Status.WAITING
                )

        print("VIAAAAAAAAAAAAAA")
        await new_workflow.save(workflow.context)
        executor = StreamFlowExecutor(new_workflow)
        try:
            output_tokens = await executor.run()
        except Exception as err:
            print("ERROR", err)
            raise Exception("EXCEPTION ERR")
        print("output_tokens", output_tokens)
        print("Finito")
        return CWLCommandOutput(value="", status=Status.COMPLETED, exit_code=0)

    async def _replay_job(self, job_version: JobVersion) -> CommandOutput:
        job = job_version.job
        if self.max_retries is None or self.jobs[job.name].version < self.max_retries:
            # Update version
            self.jobs[job.name].version += 1
            try:
                loading_context = DefaultDatabaseLoadingContext()

                rollback_steps = await self._recover_jobs(job_version, loading_context)

                return None
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
