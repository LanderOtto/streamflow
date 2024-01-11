import asyncio
import json
from collections import deque
from enum import Enum
from typing import MutableMapping, Iterable

from streamflow.core.utils import (
    get_class_from_name,
    contains_id,
    get_tag,
    get_class_fullname,
)
from streamflow.core.exception import FailureHandlingException
from streamflow.cwl.processor import CWLTokenProcessor
from streamflow.cwl.token import CWLFileToken

from streamflow.persistence.loading_context import DefaultDatabaseLoadingContext
from streamflow.core.workflow import Token, Step, Port
from streamflow.cwl.transformer import (
    BackPropagationTransformer,
    CWLTokenTransformer,
    OutputForwardTransformer,
)
from streamflow.log_handler import logger
from streamflow.recovery.utils import (
    get_steps_from_output_port,
    _is_token_available,
    get_key_by_value,
    load_and_add_ports,
    load_missing_ports,
)
from streamflow.workflow.combinator import LoopTerminationCombinator
from streamflow.workflow.port import ConnectorPort
from streamflow.workflow.step import (
    InputInjectorStep,
    ExecuteStep,
    DeployStep,
    CombinatorStep,
    LoopOutputStep,
)
from streamflow.workflow.token import JobToken


class TokenAvailability(Enum):
    Unavailable = 0
    Available = 1
    FutureAvailable = 2


class ProvenanceToken:
    def __init__(self, token_instance, is_available, port_row, step_rows):
        self.instance = token_instance
        self.is_available = is_available
        self.port_row = port_row
        self.step_rows = step_rows


async def get_execute_step_out_token_ids(next_token_ids, context):
    execute_step_out_token_ids = set()
    for t_id in next_token_ids:
        if t_id > 0:
            port_row = await context.database.get_port_from_token(t_id)
            for step_id_row in await context.database.get_steps_from_output_port(
                port_row["id"]
            ):
                step_row = await context.database.get_step(step_id_row["step"])
                if step_row["type"] == get_class_fullname(ExecuteStep):
                    execute_step_out_token_ids.add(t_id)
        elif t_id == -1:
            return [t_id]
        else:
            raise Exception(f"Token {t_id} not valid")
    return execute_step_out_token_ids


async def evaluate_token_availability(token, step_rows, context, valid_data):
    if await _is_token_available(token, context, valid_data):
        is_available = TokenAvailability.Available
    else:
        is_available = TokenAvailability.Unavailable
    for step_row in step_rows:
        port_row = await context.database.get_port_from_token(token.persistent_id)
        if issubclass(get_class_from_name(port_row["type"]), ConnectorPort):
            return TokenAvailability.Unavailable

        if issubclass(
            get_class_from_name(step_row["type"]),
            (
                ExecuteStep,
                InputInjectorStep,
                BackPropagationTransformer,
                DeployStep,
            ),
        ):
            logger.debug(
                f"evaluate_token_availability: Token {token.persistent_id} is available? {is_available}. Checking {step_row['name']} step type: {step_row['type']}"
            )
            return is_available
    return TokenAvailability.Unavailable


def is_there_step_type(rows, types):
    for step_row in rows:
        if issubclass(get_class_from_name(step_row["type"]), types):
            return True
    return False


class DirectGraph:
    INIT_GRAPH_FLAG = "init"
    LAST_GRAPH_FLAG = "last"

    def __init__(self):
        self.graph = {}

    def add(self, src, dst):
        if src is None:
            src = DirectGraph.INIT_GRAPH_FLAG
        if dst is None:
            dst = DirectGraph.LAST_GRAPH_FLAG
        logger.debug(f"DG: Added {src} -> {dst}")
        self.graph.setdefault(src, set()).add(dst)
        # if DirectGraph.LAST_GRAPH_FLAG in self.graph[src] and len(self.graph[src]) > 1:
        #     self.graph[src].remove(DirectGraph.LAST_GRAPH_FLAG)
        #     logger.debug(
        #         f"DG: Remove {DirectGraph.LAST_GRAPH_FLAG} from {src} next elems"
        #     )

    def remove(self, vertex):
        self.graph.pop(vertex, None)
        removed = [vertex]
        vertices_without_next = set()
        for k, values in self.graph.items():
            if vertex in values:
                values.remove(vertex)
            if not values:
                vertices_without_next.add(k)
        for vert in vertices_without_next:
            removed.extend(self.remove(vert))

        to_mv = set()
        for k in self.keys():
            if k != DirectGraph.INIT_GRAPH_FLAG and not self.prev(k):
                to_mv.add(k)
        for k in to_mv:
            self.add(None, k)
        return removed

    def replace(self, old_vertex, new_vertex):
        for values in self.graph.values():
            if old_vertex in values:
                values.remove(old_vertex)
                values.add(new_vertex)
        if old_vertex in self.graph.keys():
            self.graph[new_vertex] = self.graph.pop(old_vertex)

    def succ(self, vertex):
        return set(t for t in self.graph.get(vertex, []))

    def prev(self, vertex):
        return set(v for v, next_vs in self.graph.items() if vertex in next_vs)

    def empty(self):
        return False if self.graph else True

    def __getitem__(self, name):
        return self.graph[name]

    def __iter__(self):
        return iter(self.graph)

    def keys(self):
        return self.graph.keys()

    def items(self):
        return self.graph.items()

    def values(self):
        return self.graph.values()

    def __str__(self):
        # return f"{json.dumps({k : list(v) for k, v in self.graph.items()}, indent=2)}"
        tmp = {f'"{k}"': [f"{v}" for v in values] for k, values in self.graph.items()}
        return (
            "{\n"
            + "\n".join([f"{k} : {values}" for k, values in tmp.items()])
            + "\n}\n"
        )


class RollbackDeterministicWorkflowPolicy:
    def __init__(self, context):
        # { port name : [ next port names ] }
        self.dcg_port = DirectGraph()

        # { token id : [ next token ids ] }
        self.dag_tokens = DirectGraph()

        # { port name : [ port ids ] }
        self.port_name_ids = {}

        # { port name : [ token ids ] }
        self.port_tokens = {}

        # { token id : is_available }
        self.token_available = {}

        # { token id : token instance }
        self.token_instances = {}

        self.context = context

    def is_present(self, t_id):
        for ts in self.port_tokens.values():
            if t_id in ts:
                return True
        return False

    def _remove_port_names(self, port_names):
        orphan_tokens = set()  # probably a orphan token
        for port_name in port_names:
            logger.debug(f"_remove_port_names: remove port {port_name}")
            for t_id in self.port_tokens.pop(port_name, []):
                orphan_tokens.add(t_id)
            self.port_name_ids.pop(port_name, None)
        for t_id in orphan_tokens:
            logger.debug(f"_remove_port_names: remove orphan token {t_id}")
            self.remove_token(t_id)
            # if not self.is_present(t_id):
            #     logger.debug(f"Remove orphan token {t_id}")
            #     self.token_available.pop(t_id, None)
            #     self.token_instances.pop(t_id, None)

    def remove_port(self, port_name):
        logger.debug(f"remove_port {port_name}")
        self._remove_port_names(self.dcg_port.remove(port_name))

    async def get_execute_output_port_names(self, job_token: JobToken):
        port_names = set()
        for t_id in self.dag_tokens.succ(job_token.persistent_id):
            logger.debug(
                f"Token {t_id} è successivo al job_token {job_token.persistent_id}"
            )
            if t_id in (DirectGraph.INIT_GRAPH_FLAG, DirectGraph.LAST_GRAPH_FLAG):
                continue
            port_name = get_key_by_value(t_id, self.port_tokens)
            port_id = max(self.port_name_ids[port_name])

            step_rows = await self.context.database.get_steps_from_output_port(port_id)
            for step_row in await asyncio.gather(
                *(
                    asyncio.create_task(self.context.database.get_step(sr["step"]))
                    for sr in step_rows
                )
            ):
                if issubclass(get_class_from_name(step_row["type"]), ExecuteStep):
                    port_names.add(port_name)
        return list(port_names)

    # rename it in get_equal_token_id ?
    def get_equal_token(self, port_name, token):
        for t_id in self.port_tokens.get(port_name, []):
            if isinstance(token, JobToken):
                if self.token_instances[t_id].value.name == token.value.name:
                    return t_id
            elif self.token_instances[t_id].tag == token.tag:
                return t_id
        return None

    def remove_token_prev_links(self, token_id):
        prev_ids = self.dag_tokens.prev(token_id)
        logger.info(f"Remove token link: from {token_id} to {prev_ids}")
        token_without_successors = set()
        for prev_t_id in prev_ids:
            self.dag_tokens[prev_t_id].remove(token_id)
            if len(self.dag_tokens[prev_t_id]) == 0 or (
                prev_t_id != DirectGraph.INIT_GRAPH_FLAG
                and isinstance(self.token_instances[prev_t_id], JobToken)
            ):
                if prev_t_id == DirectGraph.INIT_GRAPH_FLAG:
                    raise Exception(
                        "Impossible execute a workflow without a INIT token"
                    )
                token_without_successors.add(prev_t_id)
        logger.info(
            f"Remove token link: token {token_id} found token_without_successors: {token_without_successors}"
        )
        return token_without_successors

    async def replace_token_and_remove(self, port_name, provenance_token):
        # todo : vedere di unire con update_token perché fanno praticamente la stessa cosa
        token = provenance_token.instance
        old_token_id = self.get_equal_token(port_name, token)
        # if token.persistent_id == old_token_id:
        #     return
        logger.info(f"Replacing {old_token_id} with {token.persistent_id}")
        token_without_successors = self.remove_token_prev_links(old_token_id)
        for t_id in token_without_successors:
            self.remove_token(t_id)

        # remove old token
        self.port_tokens[port_name].remove(old_token_id)
        self.token_available.pop(old_token_id)
        self.token_instances.pop(old_token_id)

        # replace
        pass
        self.dag_tokens.replace(old_token_id, token.persistent_id)

        # add new token
        self.port_tokens.setdefault(port_name, set()).add(token.persistent_id)
        self.token_instances[token.persistent_id] = token
        self.token_available[token.persistent_id] = (
            provenance_token.is_available == TokenAvailability.Available
        )

        logger.debug(
            f"replace_token_and_remove: token id: {token.persistent_id} av: {provenance_token.is_available == TokenAvailability.Available}"
        )
        pass

    def remove_token(self, token_id):
        if token_id == DirectGraph.INIT_GRAPH_FLAG:
            return
        if succ_ids := self.dag_tokens.succ(token_id):
            # noop
            logger.info(f"WARN. Deleting {token_id} but it has successors: {succ_ids}")
            # raise Exception(
            #     f"Impossible remove token {token_id} because it has successors: {succ_ids}"
            # )
        logger.info(f"Remove token: token {token_id}")
        self.token_available.pop(token_id, None)
        self.token_instances.pop(token_id, None)
        token_without_successors = self.remove_token_prev_links(token_id)
        self.dag_tokens.remove(token_id)
        for t_id in token_without_successors:
            self.remove_token(t_id)
        empty_ports = set()
        for port_name, token_list in self.port_tokens.items():
            if token_id in token_list:
                self.port_tokens[port_name].remove(token_id)
            if len(self.port_tokens[port_name]) == 0:
                empty_ports.add(port_name)
        for port_name in empty_ports:
            logger.info(
                f"Remove token: token {token_id} found to remove port {port_name}"
            )
            self.remove_port(port_name)

    def _update_token(self, port_name, token, is_av):
        if equal_token_id := self.get_equal_token(port_name, token):
            if (
                equal_token_id > token.persistent_id
                or self.token_available[equal_token_id]
            ):
                logger.debug(
                    f"update_token: port {port_name} ricevuto in input t_id {token.persistent_id} (avai {is_av}). Pero' uso il token id {equal_token_id} (avai {self.token_available[equal_token_id]}) che avevo già. "
                )
                return equal_token_id
            if is_av:
                logger.debug(
                    f"update_token: Sostituisco old_token_id: {equal_token_id} con new_token_id: {token.persistent_id}. Inoltre rimuovo i suoi vecchi legami perché new token è disponibile"
                )
                token_without_successors = self.remove_token_prev_links(equal_token_id)
                for t_id in token_without_successors:
                    self.remove_token(t_id)
                self.dag_tokens.add(None, equal_token_id)
            self.port_tokens[port_name].remove(equal_token_id)
            self.token_instances.pop(equal_token_id)
            self.token_available.pop(equal_token_id)
            self.dag_tokens.replace(equal_token_id, token.persistent_id)
            logger.debug(
                f"update_token: Sostituisco t_id: {equal_token_id} con t_id: {token.persistent_id}"
            )
        if port_name:  # port name can be None (INIT or LAST)
            self.port_tokens.setdefault(port_name, set()).add(token.persistent_id)
        self.token_instances[token.persistent_id] = token
        self.token_available[token.persistent_id] = is_av
        logger.debug(
            f"update_token: port {port_name} ricevuto in input t_id {token.persistent_id}. E lo userò"
        )
        return token.persistent_id

    def new_add(self, token_info_a: ProvenanceToken, token_info_b: ProvenanceToken):
        port_name_a = token_info_a.port_row["name"] if token_info_a else None
        port_name_b = token_info_b.port_row["name"] if token_info_b else None

        self.dcg_port.add(port_name_a, port_name_b)

        token_a_id = (
            self._update_token(
                port_name_a,
                token_info_a.instance,
                token_info_a.is_available == TokenAvailability.Available,
            )
            if token_info_a
            else None
        )
        token_b_id = (
            self._update_token(
                port_name_b,
                token_info_b.instance,
                token_info_b.is_available == TokenAvailability.Available,
            )
            if token_info_b
            else None
        )
        token_a = self.token_instances.get(token_a_id, None)
        token_b = self.token_instances.get(token_b_id, None)

        logger.debug(
            f"new_add: {token_a.persistent_id if token_a else None} -> {token_b.persistent_id if token_b else None}"
        )
        self.dag_tokens.add(
            token_a.persistent_id if token_a else None,
            token_b.persistent_id if token_b else None,
        )

        if port_name_a:
            self.port_name_ids.setdefault(port_name_a, set()).add(
                token_info_a.port_row["id"]
            )
        if port_name_b:
            self.port_name_ids.setdefault(port_name_b, set()).add(
                token_info_b.port_row["id"]
            )

    async def get_port_and_step_ids(self, exclude_ports):
        steps = set()
        ports = {
            min(self.port_name_ids[port_name])
            for port_name in self.port_tokens.keys()
            if port_name not in exclude_ports
        }
        for row_dependencies in await asyncio.gather(
            *(
                asyncio.create_task(
                    self.context.database.get_steps_from_output_port(port_id)
                )
                for port_id in ports
            )
        ):
            for row_dependency in row_dependencies:
                logger.debug(
                    f"get_port_and_step_ids: Step {(await self.context.database.get_step(row_dependency['step']))['name']} (id {row_dependency['step']}) recuperato dalla port {get_key_by_value(row_dependency['port'], self.port_name_ids)} (id {row_dependency['port']})"
                )
                steps.add(row_dependency["step"])
        rows_dependencies = await asyncio.gather(
            *(
                asyncio.create_task(self.context.database.get_input_ports(step_id))
                for step_id in steps
            )
        )
        step_to_remove = set()
        for step_id, row_dependencies in zip(steps, rows_dependencies):
            for row_port in await asyncio.gather(
                *(
                    asyncio.create_task(
                        self.context.database.get_port(row_dependency["port"])
                    )
                    for row_dependency in row_dependencies
                )
            ):
                if row_port["name"] not in self.port_tokens.keys():
                    logger.debug(
                        f"get_port_and_step_ids: Step {(await self.context.database.get_step(row_dependency['step']))['name']} (id {step_id}) rimosso perché port {row_port['name']} non presente nei port_tokens. è presente nelle ports? {row_port['id'] in ports}"
                    )
                    step_to_remove.add(step_id)
        for s_id in step_to_remove:
            steps.remove(s_id)
        return ports, steps

    async def _populate_workflow(
        self,
        port_ids: Iterable[int],
        step_ids: Iterable[int],
        failed_step,
        new_workflow,
        loading_context,
    ):
        logger.debug(
            f"populate_workflow: wf {new_workflow.name} dag[INIT] {[ get_key_by_value(t_id, self.port_tokens) for t_id in self.dag_tokens[DirectGraph.INIT_GRAPH_FLAG]]}"
        )
        logger.debug(
            f"populate_workflow: wf {new_workflow.name} port {new_workflow.ports.keys()}"
        )
        await load_and_add_ports(port_ids, new_workflow, loading_context)

        step_name_id = await self.load_and_add_steps(
            step_ids, new_workflow, loading_context
        )

        await load_missing_ports(new_workflow, step_name_id, loading_context)

        # add failed step into new_workflow
        logger.debug(
            f"populate_workflow: wf {new_workflow.name} add_3.0 step {failed_step.name}"
        )
        new_workflow.add_step(
            await loading_context.load_step(
                new_workflow.context,
                failed_step.persistent_id,
            )
        )
        for port in await asyncio.gather(
            *(
                asyncio.create_task(
                    loading_context.load_port(new_workflow.context, p.persistent_id)
                )
                for p in failed_step.get_output_ports().values()
            )
        ):
            if port.name not in new_workflow.ports.keys():
                logger.debug(
                    f"populate_workflow: wf {new_workflow.name} add_3 port {port.name}"
                )
                new_workflow.add_port(port)

        # fixing skip ports in loop-terminator
        for step in new_workflow.steps.values():
            if isinstance(step, CombinatorStep) and isinstance(
                step.combinator, LoopTerminationCombinator
            ):
                dependency_names = set()
                for dep_name, port_name in step.input_ports.items():
                    # Some data are available so added directly in the LoopCombinatorStep inputs.
                    # In this case, LoopTerminationCombinator must not wait on ports where these data are created.
                    if port_name not in new_workflow.ports.keys():
                        dependency_names.add(dep_name)
                for name in dependency_names:
                    step.input_ports.pop(name)
                    step.combinator.items.remove(name)

        # remove steps which have not input ports loaded in new workflow
        steps_to_remove = set()
        for step in new_workflow.steps.values():
            if isinstance(step, InputInjectorStep):
                continue
            for p_name in step.input_ports.values():
                if p_name not in new_workflow.ports.keys():
                    # problema nato dai loop. Vengono caricati nel new_workflow tutti gli step che hanno come output le port
                    # nel grafo. Però nei loop, più step hanno stessa porta di output (forward, backprop, loop-term).
                    # per capire se lo step sia necessario controlliamo che anche le sue port di input siano state caricate
                    logger.debug(
                        f"populate_workflow: Rimuovo step {step.name} dal wf {new_workflow.name} perché manca la input port {p_name}"
                    )
                    steps_to_remove.add(step.name)
            if step.name not in steps_to_remove and isinstance(
                step, BackPropagationTransformer
            ):
                loop_terminator = False
                for port in step.get_output_ports().values():
                    for prev_step in port.get_input_steps():
                        if isinstance(prev_step, CombinatorStep) and isinstance(
                            prev_step.combinator, LoopTerminationCombinator
                        ):
                            loop_terminator = True
                            break
                    if loop_terminator:
                        break
                if not loop_terminator:
                    logger.debug(
                        f"populate_workflow: Rimuovo step {step.name} dal wf {new_workflow.name} perché manca come prev step un LoopTerminationCombinator"
                    )
                    steps_to_remove.add(step.name)
        for step_name in steps_to_remove:
            logger.debug(
                f"populate_workflow: Rimozione (2) definitiva step {step_name} dal new_workflow {new_workflow.name}"
            )
            new_workflow.steps.pop(step_name)

        graph = None
        # todo tmp soluzione per format_graph ripetuti. Risolvere a monte, nel momento in cui si fa la load
        for s in new_workflow.steps.values():
            if isinstance(s, CWLTokenTransformer) and isinstance(
                s.processor, CWLTokenProcessor
            ):
                if not graph:
                    graph = s.processor.format_graph
                else:
                    s.processor.format_graph = graph
        logger.debug("populate_workflow: Finish")

    async def load_and_add_steps(self, step_ids, new_workflow, loading_context):
        new_step_ids = set()
        step_name_id = {}
        for sid, step in zip(
            step_ids,
            await asyncio.gather(
                *(
                    asyncio.create_task(
                        loading_context.load_step(new_workflow.context, step_id)
                    )
                    for step_id in step_ids
                )
            ),
        ):
            logger.debug(f"Loaded step {step.name} (id {sid})")
            step_name_id[step.name] = sid

            # if there are not the input ports in the workflow, the step is not added
            if not (set(step.input_ports.values()) - set(new_workflow.ports.keys())):
                if isinstance(step, OutputForwardTransformer):
                    port_id = min(self.port_name_ids[step.get_output_port().name])
                    for (
                        step_dep_row
                    ) in await new_workflow.context.database.get_steps_from_input_port(
                        port_id
                    ):
                        step_row = await new_workflow.context.database.get_step(
                            step_dep_row["step"]
                        )
                        if step_row[
                            "name"
                        ] not in new_workflow.steps.keys() and issubclass(
                            get_class_from_name(step_row["type"]), LoopOutputStep
                        ):
                            logger.debug(
                                f"Step {step_row['name']} from id {step_row['id']} will be added soon (2)"
                            )
                            new_step_ids.add(step_row["id"])
                elif isinstance(step, BackPropagationTransformer):
                    # for port_name in step.output_ports.values(): # potrebbe sostituire questo for
                    for (
                        port_dep_row
                    ) in await new_workflow.context.database.get_output_ports(
                        step_name_id[step.name]
                    ):
                        # if there are more iterations
                        if (
                            len(
                                self.port_tokens[
                                    step.output_ports[port_dep_row["name"]]
                                ]
                            )
                            > 1
                        ):
                            for (
                                step_dep_row
                            ) in await new_workflow.context.database.get_steps_from_output_port(
                                port_dep_row["port"]
                            ):
                                step_row = await new_workflow.context.database.get_step(
                                    step_dep_row["step"]
                                )
                                if issubclass(
                                    get_class_from_name(step_row["type"]),
                                    CombinatorStep,
                                ) and issubclass(
                                    get_class_from_name(
                                        json.loads(step_row["params"])["combinator"][
                                            "type"
                                        ]
                                    ),
                                    LoopTerminationCombinator,
                                ):
                                    logger.debug(
                                        f"Step {step_row['name']} from id {step_row['id']} will be added soon (1)"
                                    )
                                    new_step_ids.add(step_row["id"])
                logger.debug(
                    f"populate_workflow: (1) Step {step.name} caricato nel wf {new_workflow.name}"
                )
                new_workflow.add_step(step)
            else:
                logger.debug(
                    f"populate_workflow: Step {step.name} non viene essere caricato perché nel wf {new_workflow.name} mancano le ports {set(step.input_ports.values()) - set(new_workflow.ports.keys())}. It is present in the workflow: {step.name in new_workflow.steps.keys()}"
                )
        for sid, other_step in zip(
            new_step_ids,
            await asyncio.gather(
                *(
                    asyncio.create_task(
                        loading_context.load_step(new_workflow.context, step_id)
                    )
                    for step_id in new_step_ids
                )
            ),
        ):
            logger.debug(
                f"populate_workflow: (2) Step {other_step.name} (from step id {sid}) caricato nel wf {new_workflow.name}"
            )
            step_name_id[other_step.name] = sid
            new_workflow.add_step(other_step)
        logger.debug("populate_workflow: Step caricati")
        return step_name_id


class NewProvenanceGraphNavigation:
    def __init__(self, context):
        self.dag_tokens = DirectGraph()
        self.info_tokens: MutableMapping[int, ProvenanceToken] = {}
        self.context = context

    def add(self, src_token: Token | None, dst_token: Token | None):
        self.dag_tokens.add(
            src_token.persistent_id if src_token else src_token,
            dst_token.persistent_id if dst_token else dst_token,
        )

    async def build_unfold_graph(self, init_tokens, valid_data):
        token_frontier = deque(init_tokens)
        loading_context = DefaultDatabaseLoadingContext()
        job_token = None
        for t in token_frontier:
            # if not isinstance(t, JobToken):
            #     self.add(t, None)
            self.add(t, None)
            if isinstance(t, JobToken):
                job_token = t
        for t in token_frontier:
            if not isinstance(t, JobToken):
                self.add(job_token, t)

        while token_frontier:
            token = token_frontier.popleft()
            port_row = await self.context.database.get_port_from_token(
                token.persistent_id
            )
            step_rows = await get_steps_from_output_port(port_row["id"], self.context)
            # questa condizione si potrebbe mettere come parametro e utilizzarla come taglio. Ovvero, dove l'utente vuole che si fermi la ricerca indietro
            if await self.context.failure_manager.is_running_token(token, valid_data):
                self.add(None, token)
                is_available = TokenAvailability.Unavailable
                logger.debug(
                    f"new_build_dag: Token id: {token.persistent_id} is running. Added in INIT"
                )
            else:
                if (
                    is_available := await evaluate_token_availability(
                        token, step_rows, self.context, valid_data
                    )
                ) == TokenAvailability.Available:
                    self.add(None, token)
                    logger.debug(
                        f"new_build_dag: Token id: {token.persistent_id} is available. Added in INIT"
                    )
                else:
                    if prev_tokens := await loading_context.load_prev_tokens(
                        self.context, token.persistent_id
                    ):
                        for prev_token in prev_tokens:
                            self.add(prev_token, token)
                            if (
                                prev_token.persistent_id not in self.info_tokens.keys()
                                and not contains_id(
                                    prev_token.persistent_id, token_frontier
                                )
                            ):
                                token_frontier.append(prev_token)
                        logger.debug(
                            f"new_build_dag: Token id: {token.persistent_id} is not available, it has {[t.persistent_id for t in prev_tokens]} prev tokens "
                        )
                    elif issubclass(
                        get_class_from_name(port_row["type"]), ConnectorPort
                    ):
                        logger.debug(
                            f"Token {token.persistent_id} is in a ConnectorPort"
                        )
                    else:
                        raise FailureHandlingException(
                            f"Token {token.persistent_id} is not available and it does not have prev tokens"
                        )
            self.info_tokens.setdefault(
                token.persistent_id,
                ProvenanceToken(token, is_available, port_row, step_rows),
            )
        for info in self.info_tokens.values():
            logger.debug(
                f"token-info\n\tid: {info.instance.persistent_id}\n\ttype: {type(info.instance)}\n\tvalue: {info.instance.value.name if isinstance(info.instance, JobToken) else info.instance.value['basename'] if isinstance(info.instance, CWLFileToken) else info.instance.value}"
                f"\n\ttag: {info.instance.tag}\n\tis_avai: {info.is_available}\n\tport: {dict(info.port_row)}\n\tsteps: {[ s['name'] for s in info.step_rows] }"
            )
        pass

    # todo: check if it is still necessary
    async def get_execute_token_ids(self, job_token):
        output_token_ids = set()
        for next_t_id in self.dag_tokens.succ(job_token.persistent_id):
            if not isinstance(next_t_id, int):
                continue
            step_rows = self.info_tokens[next_t_id].step_rows
            if is_there_step_type(step_rows, (ExecuteStep,)):
                output_token_ids.add(next_t_id)
        return output_token_ids

    async def _refold_graphs(self, output_ports):
        rdwp = RollbackDeterministicWorkflowPolicy(self.context)
        for t_id, next_t_ids in self.dag_tokens.items():
            logger.debug(f"dag[{t_id}] = {next_t_ids}")
        # for t_id, next_t_ids in self.dag_tokens.items():
        #     for next_t_id in next_t_ids:
        #         rdwp.new_add(
        #             self.info_tokens.get(t_id, None),
        #             self.info_tokens.get(next_t_id, None),
        #         )
        queue = deque((DirectGraph.LAST_GRAPH_FLAG,))
        while queue:
            t_id = queue.popleft()
            prev_t_ids = self.dag_tokens.prev(t_id)
            for prev_t_id in prev_t_ids:
                if prev_t_id not in rdwp.dag_tokens.keys():
                    queue.append(prev_t_id)
                rdwp.new_add(
                    self.info_tokens.get(prev_t_id, None),
                    self.info_tokens.get(t_id, None),
                )

        for out_port in output_ports:
            rdwp.dcg_port.replace(DirectGraph.LAST_GRAPH_FLAG, out_port.name)
            rdwp.dcg_port.add(out_port.name, DirectGraph.LAST_GRAPH_FLAG)
            placeholder = Token(
                None,
                get_tag(
                    self.info_tokens[t].instance
                    for t in self.dag_tokens.prev(DirectGraph.LAST_GRAPH_FLAG)
                    if not isinstance(t, JobToken)
                ),
            )
            placeholder.persistent_id = -1
            rdwp.dag_tokens.replace(
                DirectGraph.LAST_GRAPH_FLAG, placeholder.persistent_id
            )
            rdwp.dag_tokens.add(placeholder.persistent_id, DirectGraph.LAST_GRAPH_FLAG)
            rdwp.token_instances[placeholder.persistent_id] = placeholder
            rdwp.token_available[placeholder.persistent_id] = False
            rdwp.port_name_ids.setdefault(out_port.name, set()).add(
                out_port.persistent_id
            )
            rdwp.port_tokens.setdefault(out_port.name, set()).add(
                placeholder.persistent_id
            )
        return rdwp

    async def refold_graphs(
        self, output_ports, split_last_iteration=True
    ) -> RollbackDeterministicWorkflowPolicy:
        return await self._refold_graphs(output_ports)