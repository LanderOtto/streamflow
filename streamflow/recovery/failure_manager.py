from __future__ import annotations

import posixpath
import re
import asyncio
import logging
import os
import itertools
import tempfile
import time
import token
from asyncio import Lock
import datetime
from typing import MutableMapping, MutableSequence, cast, MutableSet, Tuple

import pkg_resources

from streamflow.core.context import StreamFlowContext
from streamflow.core.data import DataLocation, DataType
from streamflow.core.utils import random_name, get_class_fullname, get_class_from_name
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
from streamflow.workflow.step import ExecuteStep, ScatterStep, TransferStep
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

from streamflow.workflow.utils import get_job_token


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


def print_graph_figure(graph, title):
    dot = graphviz.Digraph(title)
    for vertex, neighbors in graph.items():
        dot.node(str(vertex))
        for n in neighbors:
            dot.edge(str(vertex), str(n))
    # print(dot.source)
    #    dot.view("dev/" + title + ".gv")  # tempfile.mktemp('.gv')
    dot.render("dev/" + title + ".gv")


def print_graph_figure_label(graph, title):
    dot = graphviz.Digraph(title)
    for vertex, neighbors in graph.items():
        dot.node(str(vertex))
        for n, l in neighbors:
            dot.edge(str(vertex), str(n), label=str(l))
    # print(dot.source)
    # dot.view("dev/" + title + ".gv")  # tempfile.mktemp('.gv')
    dot.render("dev/" + title + ".gv")


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


def add_graph_tuple(graph_steps, step_1, tuple):
    for s, l in graph_steps[step_1]:
        if s == tuple[0] and l == tuple[1]:
            return
    graph_steps[step_1].append(tuple)


async def printa_token(
    token_visited,
    workflow,
    graph_tokens,
    loading_context,
    pdf_name="graph_steps recovery",
):
    token_values = {}
    steps = {}
    for token_id, (token, _) in token_visited.items():
        token_values[token_id] = str_token_value(token)
        steps[token_id] = (
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
    token_values[INIT_DAG_FLAG] = INIT_DAG_FLAG
    step_name = search_step_name_into_graph(graph_tokens)
    token_values[step_name] = step_name

    graph_steps = {}
    for token_id, tokens_id in graph_tokens.items():
        step_1 = steps[token_id] if isinstance(token_id, int) else token_id
        if step_1 == INIT_DAG_FLAG:
            continue
        if step_1 not in graph_steps.keys():
            graph_steps[step_1] = []
        label = (
            str_token_value(token_visited[token_id][0]) + f"({token_id})"
            if isinstance(token_id, int)
            else token_values[token_id]
        )
        for token_id_2 in tokens_id:
            step_2 = steps[token_id_2] if isinstance(token_id_2, int) else token_id_2

            add_graph_tuple(graph_steps, step_1, (step_2, label))
    print_graph_figure_label(graph_steps, pdf_name)


#################################################
#################################################
#################################################
#################################################
#################################################
#################################################


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


async def data_location_exists(data_locations, context, token):
    for data_loc in data_locations:
        if data_loc.path == token.value["path"]:
            connector = context.deployment_manager.get_connector(data_loc.deployment)
            # location_allocation = job_version.step.workflow.context.scheduler.location_allocations[data_loc.deployment][data_loc.name]
            # available_locations = job_version.step.workflow.context.scheduler.get_locations(location_allocation.jobs[0])
            if exists := await remotepath.exists(
                connector, data_loc, token.value["path"]
            ):
                # print(
                #     f"token.id: {token.persistent_id} in loc {data_loc} -> exists {exists} "
                # )
                return True
            # print(
            #     f"token.id: {token.persistent_id} in loc {data_loc} -> exists {exists} "
            # )
    return False


def _get_data_location(path, context):
    data_locs = context.data_manager.get_data_locations(path)
    for data_loc in data_locs:
        if data_loc.path == path:
            return data_loc
    return None


async def is_token_available(token, context, loading_context):
    if isinstance(token, CWLFileToken):
        # TODO: è giusto cercare una loc dal suo path? se va bene aggiustare data_location_exists method
        data_loc = _get_data_location(token.value["path"], context)
        # print(f"token.id: {token.persistent_id} ({token.value['path']})")
        if not data_loc:  # if the data are in invalid locations, data_loc is None
            return False
        if not await data_location_exists([data_loc], context, token):
            # todo: invalidare tutti i path del data_loc
            logger.debug(f"Invalidated path {token.value['path']}")
            context.data_manager.invalidate_location(
                data_loc, token.value["path"], "test"
            )
            # context.data_manager.invalidate_location(data_loc, "/", token.value["path"])
            return False
        # todo: controllare checksum con token.value['checksum'] ?
        return True
    if isinstance(token, ListToken):
        # if at least one file does not exist, returns false
        a = await asyncio.gather(
            *(
                asyncio.create_task(
                    is_token_available(inner_token, context, loading_context)
                )
                for inner_token in token.value
            )
        )
        b = all(a)
        return b
    if isinstance(token, ObjectToken):
        # TODO: sistemare
        return True
    if isinstance(token, JobToken):
        return False
    return True


async def _put_tokens(
    new_workflow: Workflow,
    token_port: MutableMapping[int, str],
    port_tokens_counter: MutableMapping[str, int],
    dag_tokens: MutableMapping[int, MutableSet[int]],
    token_visited: MutableMapping[int, Tuple[Token, bool]],
):
    # add tokens into the ports
    for token_id in dag_tokens[INIT_DAG_FLAG]:
        port = new_workflow.ports[token_port[token_id]]
        port.put(token_visited[token_id][0])
        if len(port.token_list) == port_tokens_counter[port.name]:
            port.put(TerminationToken())


async def _populate_workflow(
    failed_step,
    token_visited,
    new_workflow,
    loading_context,
):
    steps = set()
    ports = {}

    token_port = {}  # { token.id : port.name }
    port_tokens_counter = {}  # {port.name : n.of tokens}
    for token_id, (_, is_available) in token_visited.items():
        row = await failed_step.workflow.context.database.get_port_from_token(token_id)
        if row["id"] in ports.keys():
            ports[row["id"]] = ports[row["id"]] and is_available
        else:
            ports[row["id"]] = is_available

        # save the token and its port name
        token_port[token_id] = row["name"]

        # save the port name and tokens number in the DAG it produces
        if row["name"] not in port_tokens_counter.keys():
            port_tokens_counter[row["name"]] = 0
        port_tokens_counter[row["name"]] += 1

    # add port into new_workflow
    for port in await asyncio.gather(
        *(
            Port.load(
                new_workflow.context,
                port_id,
                loading_context,
                new_workflow,
            )
            for port_id in ports.keys()
        )
    ):
        new_workflow.add_port(port)

    # add step into new_workflow
    for rows_dependency in await asyncio.gather(
        *(
            new_workflow.context.database.get_step_from_output_port(port_id)
            for port_id, is_available in ports.items()
            if not is_available
        )
    ):
        for row_dependency in rows_dependency:
            steps.add(row_dependency["step"])
    for step in await asyncio.gather(
        *(
            Step.load(
                new_workflow.context,
                step_id,
                loading_context,
                new_workflow,
            )
            for step_id in steps
        )
    ):
        new_workflow.add_step(step)

    # add output port of failed step into new_workflow
    for port in await asyncio.gather(
        *(
            Port.load(
                new_workflow.context, p.persistent_id, loading_context, new_workflow
            )
            for p in failed_step.get_output_ports().values()
        )
    ):
        new_workflow.add_port(port)
    return token_port, port_tokens_counter


INIT_DAG_FLAG = "init"


def add_elem_dictionary(key, elem, dictionary):
    if key not in dictionary.keys():
        dictionary[key] = set()
    dictionary[key].add(elem)


def get_token_by_tag(token_tag, token_list):
    for token in token_list:
        if token_tag == token.tag:
            return token
    return None


def get_job_token_from_visited(job_name, token_visited):
    for token, _ in token_visited.values():
        if isinstance(token, JobToken):
            if token.value.name == job_name:
                return token
    raise Exception(f"Job {job_name} not found in token_visited")


def contains_token_id(token_id, token_list):
    for token in token_list:
        if token_id == token.persistent_id:
            return True
    return False


class RequestJob:
    def __init__(self):
        self.job_token: JobToken = None
        self.token_output: Token = None
        self.is_running: bool = True


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
        self.retry_delay: int | None = retry_delay

        self.wait_queues: MutableMapping[str, asyncio.Condition] = {}

        # {workflow.name : { port.id: [ token.id ] } }
        self.retags: MutableMapping[str, MutableMapping[int, MutableSequence[int]]] = {}

        # job.name : RequestJob
        self.jobs_request: MutableMapping[str, RequestJob] = {}

    def _remove_token(self, dag_tokens, remove_tokens):
        d = {}
        other_remove = set()
        for k, vals in dag_tokens.items():
            if k not in remove_tokens:
                d[k] = set()
                for v in vals:
                    if v not in remove_tokens:
                        d[k].add(v)
                if not d[k]:
                    other_remove.add(k)
        if other_remove:
            d = self._remove_token(d, other_remove)
        return d

    async def _build_dag(
        self, tokens, failed_job, failed_step, workflow, loading_context
    ):
        dag_tokens = {}  # { token.id : set of next tokens' id | string }
        for t in failed_job.inputs.values():
            dag_tokens[t.persistent_id] = set((failed_step.name,))

        token_visited = {}  # { token_id: (token, is_available)}
        available_new_job_tokens = {}  # {old token id : new token id}

        while tokens:
            token = tokens.pop()
            if (
                isinstance(token, JobToken)
                and token.value.name in self.jobs_request.keys()
                and self.jobs_request[token.value.name].token_output
            ):
                print(
                    token.value.name,
                    "Condizioni vere",
                    self.jobs_request[token.value.name].token_output,
                    await is_token_available(
                        self.jobs_request[token.value.name].token_output,
                        workflow.context,
                        loading_context,
                    ),
                )
            if (
                False
                and isinstance(token, JobToken)
                and token.value.name in self.jobs_request.keys()
                and self.jobs_request[token.value.name].token_output
                and await is_token_available(
                    self.jobs_request[token.value.name].token_output,
                    workflow.context,
                    loading_context,
                )
            ):
                token_1 = self.jobs_request[token.value.name].job_token
                print("token_1", token_1)
                token_visited[token.persistent_id] = (token, False)
                token_visited[token_1.persistent_id] = (token_1, True)
                available_new_job_tokens[token.persistent_id] = token_1.persistent_id
                print("HEllO THERE " * 10)
            else:
                is_available = await is_token_available(
                    token, workflow.context, loading_context
                )

                # impossible case because when added in tokens, the elem is checked
                if token.persistent_id in token_visited.keys():
                    raise FailureHandlingException(
                        f"Token {token.persistent_id} already visited"
                    )
                token_visited[token.persistent_id] = (token, is_available)

                if not is_available:
                    prev_tokens = await _load_prev_tokens(
                        token.persistent_id,
                        loading_context,
                        workflow.context,
                    )
                    if prev_tokens:
                        for pt in prev_tokens:
                            add_elem_dictionary(
                                pt.persistent_id, token.persistent_id, dag_tokens
                            )
                            if (
                                pt.persistent_id not in token_visited.keys()
                                and not contains_token_id(pt.persistent_id, tokens)
                            ):
                                tokens.append(pt)
                    else:
                        add_elem_dictionary(
                            INIT_DAG_FLAG, token.persistent_id, dag_tokens
                        )
                else:
                    add_elem_dictionary(INIT_DAG_FLAG, token.persistent_id, dag_tokens)
                # alternativa ai due else ... però è più difficile la lettura del codice
                # if is_available or not prev_tokens:
                #     add_elem_dictionary(INIT_DAG_FLAG, token.persistent_id, dag_tokens)

        token_visited = dict(sorted(token_visited.items()))
        await printa_token(
            token_visited,
            workflow,
            dag_tokens,
            loading_context,
            "prima-e-dopo/"
            + str(datetime.datetime.now()).replace(" ", "_").replace(":", ".")
            + "_graph_steps_recovery",
        )
        # print_graph_figure(dag_tokens, "graph_token_recovery")
        pass

        # replace in the graph the old token output with the new
        # { token output id of new job : { tokens output is of old job } }
        replace_token_outputs = {}

        remove_tokens = set()
        for k, v in available_new_job_tokens.items():
            # recovery token generated by the new job
            t = self.jobs_request[token_visited[v][0].value.name].token_output
            token_visited[t.persistent_id] = (t, True)
            pass
            # save subsequent tokens, before deleting the token
            replace_token_outputs[t.persistent_id] = {
                t2 for t1 in dag_tokens[k] for t2 in dag_tokens[t1]
            }
            # replace_token_outputs[t.persistent_id] = set()
            # for t1 in dag_tokens[k]:
            #     for t2 in dag_tokens[t1]:
            #         replace_token_outputs[t.persistent_id].add(t2)
            pass
            # add to the list the tokens to be removed in the graph
            for t in dag_tokens[k]:
                remove_tokens.add(t)

        # remove token in the graph
        d = self._remove_token({k: v for k, v in dag_tokens.items()}, remove_tokens)

        # add old token dependencies but with the new token
        for k, vals in replace_token_outputs.items():
            d[k] = {v for v in vals if v not in remove_tokens}

        # add new tokens in init
        for v in available_new_job_tokens.values():
            d[INIT_DAG_FLAG].add(
                self.jobs_request[
                    token_visited[v][0].value.name
                ].token_output.persistent_id
            )
        pass
        await printa_token(
            token_visited,
            workflow,
            d,
            loading_context,
            "prima-e-dopo/"
            + str(datetime.datetime.now()).replace(" ", "_").replace(":", ".")
            + "_graph_steps_recovery_d",
        )
        # print_graph_figure(d, "graph_token_d")
        pass
        return dag_tokens, token_visited

    async def _do_handle_failure(self, job: Job, step: Step) -> CommandOutput:
        # Delay rescheduling to manage temporary failures (e.g. connection lost)
        if self.retry_delay is not None:
            await asyncio.sleep(self.retry_delay)
        try:
            command_output = await self._recover_jobs(job, step)
            # When receiving a FailureHandlingException, simply fail
        except FailureHandlingException as e:
            logger.exception(e)
            raise
        # When receiving a KeyboardInterrupt, propagate it (to allow debugging)
        except KeyboardInterrupt:
            raise
        except Exception as e:
            logger.exception(e)
            return await self.handle_exception(job, step, e)
        return command_output

    # todo: situazione problematica
    #  A -> B
    #  A -> C
    #  B -> C
    # A ha successo, B fallisce (cade ambiete) e viene rieseguito A, in C che input di A arriva? quello vecchio? quello vecchio e quello nuovo? In teoria solo quello vecchio, da gestire comunque? oppure lasciamo che fallisce e poi il failure manager prendere l'output nuovo di A?

    async def _recover_jobs(self, failed_job: Job, failed_step: Step) -> CommandOutput:
        # await print_graph(job_version, loading_context)
        loading_context = DefaultDatabaseLoadingContext()

        workflow = failed_step.workflow
        new_workflow = Workflow(
            context=workflow.context,
            type=workflow.type,
            name=random_name(),
            config=workflow.config,
        )

        # should be an impossible case
        if failed_step.persistent_id is None:
            raise FailureHandlingException(
                f"Workflow {workflow.name} has the step {failed_step.name} not saved in the database."
            )

        job_token = get_job_token(
            failed_job.name,
            failed_step.get_input_port("__job__").token_list,
        )

        tokens = list(failed_job.inputs.values())  # tokens to check
        tokens.append(job_token)

        dag_tokens, token_visited = await self._build_dag(
            tokens, failed_job, failed_step, workflow, loading_context
        )

        # create RequestJob
        for token, _ in token_visited.values():
            if isinstance(token, JobToken):
                if token.value.name not in self.wait_queues.keys():
                    self.wait_queues[token.value.name] = asyncio.Condition()
                async with self.wait_queues[token.value.name]:
                    if token.value.name not in self.jobs_request.keys():
                        self.jobs_request[token.value.name] = RequestJob()
        pass

        token_port, port_tokens_counter = await _populate_workflow(
            failed_step,
            token_visited,
            new_workflow,
            loading_context,
        )

        await _put_tokens(
            new_workflow,
            token_port,
            port_tokens_counter,
            dag_tokens,
            token_visited,
        )

        pass
        self._save_for_retag(new_workflow, dag_tokens, token_port, token_visited)
        pass

        for token, _ in token_visited.values():
            if isinstance(token, JobToken):
                # free resources scheduler
                await workflow.context.scheduler.notify_status(
                    token.value.name, Status.WAITING
                )

                # save jobs recovered
                if token.value.name not in self.jobs.keys():
                    self.jobs[token.value.name] = JobVersion(
                        job=None,
                        step=None,
                        outputs=None,
                        version=1,
                    )
                async with self.wait_queues[token.value.name]:
                    if self.jobs_request[token.value.name].job_token:
                        print(
                            f"job {token.value.name}. Il mio job_token ha un valore",
                            self.jobs_request[token.value.name].job_token,
                            "adesso però lo pongo a none",
                        )
                    self.jobs_request[token.value.name].job_token = None
                    if (
                        self.max_retries is None
                        or self.jobs[token.value.name].version < self.max_retries
                    ):
                        if not self.jobs_request[token.value.name].is_running:
                            self.jobs[token.value.name].version += 1
                            self.jobs_request[token.value.name].is_running = True
                            logger.debug(
                                f"Updated Job {token.value.name} at {self.jobs[token.value.name].version} times"
                            )
                    else:
                        logger.error(
                            f"FAILED Job {token.value.name} {self.jobs[token.value.name].version} times. Execution aborted"
                        )
                        raise FailureHandlingException()

        print("VIAAAAAAAAAAAAAA " + new_workflow.name)

        await new_workflow.save(workflow.context)
        executor = StreamFlowExecutor(new_workflow)
        try:
            await executor.run()
        except Exception as err:
            logger.exception(
                f"Sub workflow {new_workflow.name} to handle the failed job {failed_job.name} throws a exception."
            )
            raise err

        # get new job created by ScheduleStep
        return await self._execute_failed_job(
            failed_job, failed_step, new_workflow, loading_context
        )

    def is_valid_tag(self, workflow_name, tag, output_port):
        if workflow_name not in self.retags.keys():
            return True
        if output_port.name not in self.retags[workflow_name].keys():
            return True

        token_list = self.retags[workflow_name][output_port.name]
        for t in token_list:
            if t.tag == tag:
                return True
        return False

    def _save_for_retag(self, new_workflow, dag_token, token_port, token_visited):
        # todo: aggiungere la possibilità di eserguire comunque tutti i job delle scatter aggiungendo un parametro nel StreamFlow file
        if new_workflow.name not in self.retags.keys():
            self.retags[new_workflow.name] = {}

        port_token = {}
        for t_id, p_name in token_port.items():
            if p_name not in port_token:
                port_token[p_name] = set()
            port_token[p_name].add(t_id)

        for step in new_workflow.steps.values():
            if isinstance(step, ScatterStep):
                port = step.get_output_port()
                for t_id in port_token[port.name]:
                    add_elem_dictionary(
                        port.name,
                        token_visited[t_id][0],
                        self.retags[new_workflow.name],
                    )

    async def _execute_failed_job(
        self, failed_job, failed_step, new_workflow, loading_context
    ):
        new_job_token = get_job_token(
            failed_job.name,
            new_workflow.ports[failed_step.get_input_port("__job__").name].token_list,
        )

        # get new job inputs
        new_inputs = {}
        for step_port_name, token in failed_job.inputs.items():
            original_port = failed_step.get_input_port(step_port_name)
            if original_port.name in new_workflow.ports.keys():
                new_inputs[step_port_name] = get_token_by_tag(
                    token.tag, new_workflow.ports[original_port.name].token_list
                )
            else:
                new_inputs[step_port_name] = get_token_by_tag(
                    token.tag, original_port.token_list
                )
        new_job_token.value.inputs = new_inputs

        async with self.wait_queues[failed_job.name]:
            self.jobs_request[failed_job.name].job_token = new_job_token
        new_job = new_job_token.value
        new_step = await Step.load(
            new_workflow.context,
            failed_step.persistent_id,
            loading_context,
            new_workflow,
        )
        new_workflow.add_step(new_step)
        await new_step.save(new_workflow.context)

        cmd_out = await cast(ExecuteStep, new_step).command.execute(new_job)
        if cmd_out.status == Status.FAILED:
            logger.error(f"FAILED Job {new_job.name} with error:\n\t{cmd_out.value}")
            cmd_out = await self.handle_failure(new_job, new_step, cmd_out)
        print("Finito " + new_workflow.name)
        return cmd_out

    async def get_valid_job_token(self, job_token):
        if job_token.value.name in self.wait_queues.keys():
            async with self.wait_queues[job_token.value.name]:
                request = (
                    self.jobs_request[job_token.value.name]
                    if job_token.value.name in self.jobs_request.keys()
                    else None
                )
                if request:
                    if request.job_token:
                        return request.job_token
                    else:
                        request.job_token = job_token
        return job_token

    async def close(self):
        pass

    async def notify_jobs(self, job_name, token):
        if job_name in self.wait_queues.keys():
            async with self.wait_queues[job_name]:
                self.jobs_request[job_name].is_running = False
                self.jobs_request[job_name].token_output = token
                self.wait_queues[job_name].notify_all()

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

    async def handle_failure_transfer(self, job: Job, step: Step):
        print("CIAO, io devo gestire gli errori nei trasferimenti")
        failed_job = job
        failed_step = step

        loading_context = DefaultDatabaseLoadingContext()

        workflow = failed_step.workflow
        new_workflow = Workflow(
            context=workflow.context,
            type=workflow.type,
            name=random_name(),
            config=workflow.config,
        )

        # should be an impossible case
        if failed_step.persistent_id is None:
            raise FailureHandlingException(
                f"Workflow {workflow.name} has the step {failed_step.name} not saved in the database."
            )

        job_token = get_job_token(
            failed_job.name,
            failed_step.get_input_port("__job__").token_list,
        )

        tokens = list(failed_job.inputs.values())  # tokens to check
        tokens.append(job_token)

        print("tokens", tokens)
        dag_tokens, token_visited = await self._build_dag(
            tokens, failed_job, failed_step, workflow, loading_context
        )
        print("ci sono?0", failed_step.name in new_workflow.steps.keys())

        # create RequestJob
        for token, _ in token_visited.values():
            if isinstance(token, JobToken):
                if token.value.name not in self.wait_queues.keys():
                    self.wait_queues[token.value.name] = asyncio.Condition()
                async with self.wait_queues[token.value.name]:
                    if token.value.name not in self.jobs_request.keys():
                        self.jobs_request[token.value.name] = RequestJob()
        pass

        token_port, port_tokens_counter = await _populate_workflow(
            failed_step,
            token_visited,
            new_workflow,
            loading_context,
        )

        new_workflow.add_step(
            await Step.load(
                new_workflow.context,
                failed_step.persistent_id,
                loading_context,
                new_workflow,
            )
        )
        for port in failed_step.get_input_ports().values():
            new_workflow.add_port(
                await Port.load(
                    new_workflow.context,
                    port.persistent_id,
                    loading_context,
                    new_workflow,
                )
            )

        print("ci sono?1", failed_step.name in new_workflow.steps.keys())

        await _put_tokens(
            new_workflow,
            token_port,
            port_tokens_counter,
            dag_tokens,
            token_visited,
        )

        print("ci sono?3", failed_step.name in new_workflow.steps.keys())

        pass
        self._save_for_retag(new_workflow, dag_tokens, token_port, token_visited)
        pass

        print("ci sono?4", failed_step.name in new_workflow.steps.keys())

        for token, _ in token_visited.values():
            if isinstance(token, JobToken):
                # free resources scheduler
                await workflow.context.scheduler.notify_status(
                    token.value.name, Status.WAITING
                )

                # save jobs recovered
                if token.value.name not in self.jobs.keys():
                    self.jobs[token.value.name] = JobVersion(
                        job=None,
                        step=None,
                        outputs=None,
                        version=1,
                    )
                async with self.wait_queues[token.value.name]:
                    if self.jobs_request[token.value.name].job_token:
                        print(
                            f"job {token.value.name}. Il mio job_token ha un valore",
                            self.jobs_request[token.value.name].job_token,
                            "adesso però lo pongo a none",
                        )
                    self.jobs_request[token.value.name].job_token = None
                    if (
                        self.max_retries is None
                        or self.jobs[token.value.name].version < self.max_retries
                    ):
                        if not self.jobs_request[token.value.name].is_running:
                            self.jobs[token.value.name].version += 1
                            self.jobs_request[token.value.name].is_running = True
                            logger.debug(
                                f"Updated Job {token.value.name} at {self.jobs[token.value.name].version} times"
                            )
                    else:
                        logger.error(
                            f"FAILED Job {token.value.name} {self.jobs[token.value.name].version} times. Execution aborted"
                        )
                        raise FailureHandlingException()

        print("VIAAAAAAAAAAAAAA " + new_workflow.name)
        print("ci sono?5", failed_step.name in new_workflow.steps.keys())

        await new_workflow.save(workflow.context)
        executor = StreamFlowExecutor(new_workflow)
        try:
            await executor.run()
        except Exception as err:
            logger.exception(
                f"Sub workflow {new_workflow.name} to handle the failed job {failed_job.name} throws a exception."
            )
            raise err

        print("ci sono?6", failed_step.name in new_workflow.steps.keys())
        print("fsold", [t.token_list for t in failed_step.get_output_ports().values()])
        print(
            "fsnew",
            [
                t.token_list
                for t in new_workflow.steps[failed_step.name]
                .get_output_ports()
                .values()
            ],
        )

        a = [
            t.token_list
            for t in new_workflow.steps[failed_step.name].get_output_ports().values()
        ]
        print("aaaaa2", a)
        b = a[0]
        print("bbbbb2", b)
        out_token = b[0]
        print("ccccc2", out_token)
        data_loc = _get_data_location(out_token.value["path"], new_workflow.context)
        print(
            "output esiste? ",
            await remotepath.exists(
                connector=new_workflow.context.deployment_manager.get_connector(
                    data_loc.deployment
                ),
                location=data_loc,
                path=out_token.value["path"],
            ),
        )

        for _, port in new_workflow.steps[failed_step.name].get_output_ports().items():
            for t in port.token_list:
                if isinstance(t, TerminationToken):
                    print("Ha già un termination token........Questo approccio non va bene")

        for k, port in new_workflow.steps[failed_step.name].get_output_ports().items():
            for t in port.token_list:
                if len(port.token_list) < 3:
                    print(
                        "ALLARMEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE"
                    )
                print(f"inserisco token {t} in port {k}({port.name})")
                failed_step.get_output_port(k).put(t)
        print(
            "fsold pt2", [t.token_list for t in failed_step.get_output_ports().values()]
        )

        return Status.COMPLETED


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

    async def get_valid_job_token(self, job_token):
        return job_token

    def is_valid_tag(self, workflow_name, tag, output_port):
        return True

    async def notify_jobs(self, job_name, token):
        pass

    async def handle_failure_transfer(self, job: Job, step: Step):
        return Status.FAILED
