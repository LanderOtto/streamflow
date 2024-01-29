import asyncio
import json
from collections import deque
from enum import Enum
from typing import MutableMapping, Iterable, MutableSequence

from streamflow.core.utils import (
    get_class_from_name,
    get_class_fullname,
)
from streamflow.core.exception import FailureHandlingException
from streamflow.cwl.processor import CWLTokenProcessor

from streamflow.persistence.loading_context import DefaultDatabaseLoadingContext
from streamflow.core.workflow import Token
from streamflow.cwl.transformer import (
    BackPropagationTransformer,
    CWLTokenTransformer,
    OutputForwardTransformer,
)
from streamflow.log_handler import logger
from streamflow.recovery.utils import (
    get_steps_row_from_output_port,
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


class RollbackDeterministicWorkflowPolicy:
    pass


class PortInfo:
    def __init__(self, id, name, port_type, token):
        self.id = id
        self.name = name
        self.type = port_type
        self.token = token


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
            for step_id_row in await context.database.get_input_steps(
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
                f"evaluate_token_availability: Token {token.persistent_id} is available? {is_available}. "
                f"Checking {step_row['name']} step type: {step_row['type']}"
            )
            return is_available
    return TokenAvailability.Unavailable


async def explore_provenance(
    init_tokens: MutableSequence[PortInfo],
    failed_step_output_ports,
    valid_data,
    context,
):
    loading_context = DefaultDatabaseLoadingContext()
    graph = ProvenanceGraph(context, loading_context)
    frontier = deque(init_tokens)
    token_visited = set()
    job_port_info = None
    for port_info in frontier:
        await graph.add(port_info, None)
        if isinstance(port_info.token, JobToken):
            job_port_info = port_info
        token_visited.add(port_info.token.persistent_id)
    for port_info in frontier:
        if not isinstance(port_info.token, JobToken):
            await graph.add(job_port_info, port_info)

    while frontier:
        port_info = frontier.popleft()
        step_rows = await get_steps_row_from_output_port(port_info.id, context)
        # questa condizione si potrebbe mettere come parametro e utilizzarla come taglio.
        # Ovvero, dove l'utente vuole che si fermi la ricerca indietro
        if await context.failure_manager.is_running_token(port_info.token, valid_data):
            await graph.add(None, port_info)
            logger.debug(
                f"new_build_dag: Token id: {port_info.token.persistent_id} is running. Added in INIT"
            )
        else:
            is_available = await evaluate_token_availability(
                port_info.token, step_rows, context, valid_data
            )
            if is_available == TokenAvailability.Available:
                await graph.add(None, port_info)
                logger.debug(
                    f"new_build_dag: Token id: {port_info.token.persistent_id} is available. Added in INIT"
                )
            else:
                if prev_tokens := await loading_context.load_prev_tokens(
                    context, port_info.token.persistent_id
                ):
                    for prev_token in prev_tokens:
                        port_row = await context.database.get_port_from_token(
                            prev_token.persistent_id
                        )
                        prev_port_info = PortInfo(
                            port_row["id"],
                            port_row["name"],
                            port_row["type"],
                            prev_token,
                        )
                        await graph.add(prev_port_info, port_info)
                        if prev_token.persistent_id not in token_visited:
                            frontier.append(prev_port_info)
                            token_visited.add(prev_port_info.token.persistent_id)
                    logger.debug(
                        f"new_build_dag: Token id: {port_info.token.persistent_id} is not available, "
                        f"it has {[t.persistent_id for t in prev_tokens]} prev tokens "
                    )
                elif issubclass(get_class_from_name(port_info.type), ConnectorPort):
                    logger.debug(
                        f"Token {port_info.token.persistent_id} is in a ConnectorPort"
                    )
                else:
                    raise FailureHandlingException(
                        f"Token {port_info.token.persistent_id} is not available and it does not have prev tokens"
                    )
    return graph


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
        self.graph.setdefault(src, set()).add(dst)

    def remove(self, vertex):
        self.graph.pop(vertex)

    # def remove(self, vertex):
    #     self.graph.pop(vertex, None)
    #     removed = [vertex]
    #     vertices_without_next = set()
    #     for k, values in self.graph.items():
    #         if vertex in values:
    #             values.remove(vertex)
    #         if not values:
    #             vertices_without_next.add(k)
    #     for vert in vertices_without_next:
    #         removed.extend(self.remove(vert))
    #
    #     to_mv = set()
    #     for k in self.keys():
    #         if k != DirectGraph.INIT_GRAPH_FLAG and not self.prev(k):
    #             to_mv.add(k)
    #     for k in to_mv:
    #         self.add(None, k)
    #     return removed

    def replace(self, old_vertex, new_vertex):
        for values in self.graph.values():
            if old_vertex in values:
                values.remove(old_vertex)
                values.add(new_vertex)
        if old_vertex in self.graph.keys():
            self.graph[new_vertex] = self.graph.pop(old_vertex)

    def succ(self, vertex):
        return {t for t in self.graph.get(vertex, [])}

    def prev(self, vertex):
        return {v for v, next_vs in self.graph.items() if vertex in next_vs}

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


class ProvenanceGraph:
    def __init__(self, context, loading_context):
        # { token id : [ next token ids ] }
        self.dag_tokens = DirectGraph()

        # { port name : [ port ids ] }
        self.port_name_ids = {}

        # { port name : [ token ids ] }
        self.port_tokens = {}

        self.context = context
        self.loading_context = loading_context

    async def get_token_instances(self) -> MutableMapping[int, Token]:
        token_ids_method_1 = set()
        tokens = []
        for t_list in self.port_tokens.values():
            for t_id in t_list:
                token_ids_method_1.add(t_id)
                tokens.append(
                    asyncio.create_task(
                        self.loading_context.load_token(self.context, t_id)
                    )
                )
        token_ids_method_2 = set()
        for t_id, token_id_list in self.dag_tokens.items():
            if isinstance(t_id, int):
                token_ids_method_2.add(t_id)
            for next_t_id in token_id_list:
                if isinstance(next_t_id, int):
                    token_ids_method_2.add(next_t_id)
        return {token.persistent_id: token for token in await asyncio.gather(*tokens)}

    def is_token_available(self, t_id: int) -> bool:
        return t_id in self.dag_tokens[DirectGraph.INIT_GRAPH_FLAG]

    async def get_equal_token(self, port_name, token):
        for t_id in self.port_tokens.get(port_name, []):
            curr_token = await self.loading_context.load_token(self.context, t_id)
            if isinstance(token, JobToken):
                if curr_token.value.name == token.value.name:
                    return t_id
            elif curr_token.tag == token.tag:
                return t_id
        return None

    def remove_prevs(self, token_id):
        tokens_without_successors = set()
        for prev_token_id in self.dag_tokens.prev(token_id):
            logger.debug(f"Remove link from {prev_token_id} to {token_id}")
            self.dag_tokens[prev_token_id].remove(token_id)
            if not self.dag_tokens[prev_token_id]:
                tokens_without_successors.add(prev_token_id)
        for t_id in tokens_without_successors:
            logger.debug(f"Remove token {t_id} because it has not successors")
            self.remove_prevs(t_id)
            self.dag_tokens.remove(t_id)
            port_name = get_key_by_value(t_id, self.port_tokens)
            self.port_tokens[port_name].remove(t_id)
            if not self.port_tokens[port_name]:
                logger.debug(f"Remove port_name {port_name} because it has not tokens")
                self.port_tokens.pop(port_name)
                self.port_name_ids.pop(port_name)

    def replace(self, token_id_a, token_id_b, port_name):
        logger.info(
            f"Token {token_id_a} replaces token {token_id_b} in port {port_name}"
        )
        self.dag_tokens.replace(token_id_a, token_id_b)
        self.port_tokens[port_name].remove(token_id_a)
        self.port_tokens[port_name].add(token_id_b)

    async def update_token(self, info_port: PortInfo, is_available):
        if info_port is None:
            return None

        # token is already present
        if token_id := await self.get_equal_token(info_port.name, info_port.token):
            # if it is available remove old token and its predecessors
            if is_available:
                logger.info(
                    f"updating token: {info_port.token.persistent_id} is available and replace {token_id}"
                )
                self.remove_prevs(token_id)
                self.replace(token_id, info_port.token.persistent_id, info_port.name)
                return info_port.token.persistent_id

            # if it is newer replace old token
            elif token_id < info_port.token.persistent_id:
                logger.info(
                    f"updating token: {info_port.token.persistent_id} is newer and replace {token_id}"
                )
                self.replace(token_id, info_port.token.persistent_id, info_port.name)
                return info_port.token.persistent_id
            else:
                logger.info(
                    f"updating token: {info_port.token.persistent_id} is not used and it returned old token {token_id}"
                )
                return token_id
        else:
            logger.info(
                f"updating token: equal token not present. Returned token {info_port.token.persistent_id}"
            )
            return info_port.token.persistent_id

    async def add(self, info_vertex_a: PortInfo, info_vertex_b: PortInfo):
        token_a_id = await self.update_token(info_vertex_a, False)
        token_b_id = await self.update_token(info_vertex_b, info_vertex_a is None)

        if token_b_id is None or token_b_id in self.dag_tokens.keys():
            self.dag_tokens.add(token_a_id, token_b_id)

            if token_a_id:
                self.port_tokens.setdefault(info_vertex_a.name, set()).add(token_a_id)
                self.port_name_ids.setdefault(info_vertex_a.name, set()).add(
                    info_vertex_a.id
                )

            if token_b_id:
                self.port_tokens.setdefault(info_vertex_b.name, set()).add(token_b_id)
                self.port_name_ids.setdefault(info_vertex_b.name, set()).add(
                    info_vertex_b.id
                )
        else:
            logger.info(
                f"Link {token_a_id} to {token_b_id} did not added because {token_b_id} is not a vertex"
            )

        for values in self.dag_tokens.values():
            for v in values:
                if v not in self.dag_tokens.keys() and v != DirectGraph.LAST_GRAPH_FLAG:
                    logger.info(
                        f"Adding {token_a_id} to {token_b_id} it was found that token {v} has not a successor"
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
                    self.context.database.get_input_steps(port_id)
                )
                for port_id in ports
            )
        ):
            for row_dependency in row_dependencies:
                step_name = (
                    await self.context.database.get_step(row_dependency["step"])
                )["name"]
                logger.debug(
                    f"get_port_and_step_ids: Step {step_name} (id {row_dependency['step']}) "
                    f"rescued from the port {get_key_by_value(row_dependency['port'], self.port_name_ids)} "
                    f"(id {row_dependency['port']})"
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
                    step_name = (await self.context.database.get_step(step_id))["name"]
                    logger.debug(
                        f"get_port_and_step_ids: Step {step_name} (id {step_id}) removed because "
                        f"port {row_port['name']} is not present in port_tokens. "
                        f"Is it present in ports to load? {row_port['id'] in ports}"
                    )
                    step_to_remove.add(step_id)
        for s_id in step_to_remove:
            steps.remove(s_id)
        return ports, steps

    async def populate_workflow(
        self,
        port_ids: Iterable[int],
        step_ids: Iterable[int],
        failed_step,
        new_workflow,
        loading_context,
    ):
        logger.debug(
            f"populate_workflow: wf {new_workflow.name} dag[INIT] "
            f"{[ get_key_by_value(t_id, self.port_tokens) for t_id in self.dag_tokens[DirectGraph.INIT_GRAPH_FLAG]]}"
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
        new_step = await loading_context.load_step(
            new_workflow.context,
            failed_step.persistent_id,
        )
        new_workflow.steps[new_step.name] = new_step
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
                new_workflow.ports[port.name] = port

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
                    # problema nato dai loop. Vengono caricati nel new_workflow tutti gli step che hanno come output
                    # le port nel grafo. Però nei loop, più step hanno stessa porta di output
                    # (forward, backprop, loop-term). per capire se lo step sia necessario controlliamo che anche
                    # le sue port di input siano state caricate
                    logger.debug(
                        f"populate_workflow: Remove step {step.name} from wf {new_workflow.name} "
                        f"because input port {p_name} is missing"
                    )
                    steps_to_remove.add(step.name)
            if step.name not in steps_to_remove and isinstance(
                step, BackPropagationTransformer
            ):
                is_there_loop_terminator = any(
                    prev_step
                    for port in step.get_output_ports().values()
                    for prev_step in port.get_input_steps()
                    if isinstance(prev_step, CombinatorStep)
                    and isinstance(prev_step.combinator, LoopTerminationCombinator)
                )
                if not is_there_loop_terminator:
                    logger.debug(
                        f"populate_workflow: Remove step {step.name} from wf {new_workflow.name} because "
                        f"it is missing a prev step like LoopTerminationCombinator"
                    )
                    steps_to_remove.add(step.name)
        for step_name in steps_to_remove:
            logger.debug(
                f"populate_workflow: Rimozione (2) definitiva step {step_name} dal new_workflow {new_workflow.name}"
            )
            new_workflow.steps.pop(step_name)

        graph = None
        # todo tmp solution
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

            # if step input port are not in the workflow, the step is not added
            if not (set(step.input_ports.values()) - set(new_workflow.ports.keys())):
                if isinstance(step, OutputForwardTransformer):
                    # add step only if there is its LoopOutputStep
                    port_id = min(self.port_name_ids[step.get_output_port().name])
                    dependency_rows = (
                        await new_workflow.context.database.get_output_steps(
                            port_id
                        )
                    )
                    for step_dep_row in dependency_rows:
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
                    dependency_rows = (
                        await new_workflow.context.database.get_output_ports(
                            step_name_id[step.name]
                        )
                    )
                    for port_dep_row in dependency_rows:
                        # if there are more iterations
                        if (
                            len(
                                self.port_tokens[
                                    step.output_ports[port_dep_row["name"]]
                                ]
                            )
                            > 1
                        ):
                            step_dependency_rows = await new_workflow.context.database.get_input_steps(
                                port_dep_row["port"]
                            )
                            for step_dep_row in step_dependency_rows:
                                step_row = await new_workflow.context.database.get_step(
                                    step_dep_row["step"]
                                )
                                # if there is a CombinatorStep with inside a LoopTerminationCombinator
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
                    f"populate_workflow: (1) Step {step.name} loaded in the wf {new_workflow.name}"
                )
                new_workflow.steps[step.name] = step
            else:
                logger.debug(
                    f"populate_workflow: Step {step.name} is not loaded because in the wf {new_workflow.name}"
                    f"the ports {set(step.input_ports.values()) - set(new_workflow.ports.keys())} are missing."
                    f"It is present in the workflow: {step.name in new_workflow.steps.keys()}"
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
                f"populate_workflow: (2) Step {other_step.name} (from step id {sid}) loaded "
                f"in the wf {new_workflow.name}"
            )
            step_name_id[other_step.name] = sid
            new_workflow.steps[other_step.name] = other_step
        logger.debug("populate_workflow: Step caricati")
        return step_name_id

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

            step_rows = await self.context.database.get_input_steps(port_id)
            for step_row in await asyncio.gather(
                *(
                    asyncio.create_task(self.context.database.get_step(sr["step"]))
                    for sr in step_rows
                )
            ):
                if issubclass(get_class_from_name(step_row["type"]), ExecuteStep):
                    port_names.add(port_name)
        return list(port_names)

    def get_port_from_token(self, token_id: int) -> str:
        return get_key_by_value(token_id, self.port_tokens)

    def remove_token_prev_links(self, token_id: int) -> MutableSequence[int]:
        self.remove_prevs(token_id)
        return []

    async def replace_token_and_remove(
        self, port_name: str, provenance_token: ProvenanceToken
    ) -> MutableSequence[int]:
        token = provenance_token.instance
        old_token_id = await self.get_equal_token(port_name, token)
        self.replace(old_token_id, token.persistent_id, port_name)
        self.remove_prevs(token.persistent_id)
        self.dag_tokens.add(None, token.persistent_id)
        logger.info(
            f"replace_token_and_remove: add link from init to {token.persistent_id}"
        )
        return []
