from __future__ import annotations


import asyncio
import json
from typing import MutableMapping, MutableSequence, Tuple, MutableSet

from streamflow.core.context import StreamFlowContext
from streamflow.core.utils import (
    get_class_fullname,
    get_class_from_name,
    contains_id,
    get_tag_level,
)
from streamflow.core.exception import FailureHandlingException

from streamflow.core.workflow import Job, Step, Port, Token
from streamflow.cwl.processor import CWLTokenProcessor
from streamflow.cwl.step import CWLLoopConditionalStep, CWLRecoveryLoopConditionalStep
from streamflow.cwl.transformer import (
    BackPropagationTransformer,
    OutputForwardTransformer,
    CWLTokenTransformer,
)
from streamflow.log_handler import logger
from streamflow.recovery.utils import (
    is_output_port_forward,
    is_next_of_someone,
    get_steps_from_output_port,
    _is_token_available,
    get_necessary_tokens,
    get_prev_vertices,
    INIT_DAG_FLAG,
    get_key_by_value,
    convert_to_json,
    get_recovery_loop_expression,
    get_output_ports,
)
from streamflow.workflow.combinator import LoopTerminationCombinator
from streamflow.workflow.port import ConnectorPort, JobPort
from streamflow.workflow.step import (
    CombinatorStep,
    LoopOutputStep,
    InputInjectorStep,
    LoopCombinatorStep,
    ExecuteStep,
)
from streamflow.workflow.token import (
    TerminationToken,
    JobToken,
    IterationTerminationToken,
)
from streamflow.core.workflow import Workflow


class RecoveryContext:
    def __init__(
        self,
        output_ports: MutableSequence[str],
        port_name_ids: MutableMapping[str, MutableSet[int]],
        context,
    ):
        self.context = context
        self.input_ports = set()

        # { port_names }
        self.output_ports = output_ports

        # { name : [ ids ] }
        self.port_name_ids = port_name_ids


class WorkflowRecovery(RecoveryContext):
    def __init__(
        self,
        dag_ports: MutableMapping[str, MutableSet[str]],
        output_ports: MutableSequence[str],
        port_name_ids: MutableMapping[str, MutableSet[int]],
        context: StreamFlowContext,
    ):
        super().__init__(output_ports, port_name_ids, context)

        # { port_name : [ next_port_names ] }
        self.dag_ports = dag_ports

        # { port_name : [ token_ids ] }
        self.port_tokens = {}

        # { id : (Token, is_available) }
        self.token_visited = {}

        # LoopCombinatorStep name if the failed step is inside a loop
        self.external_loop_step_name = None
        self.external_loop_step = None

    def add_into_vertex(
        self,
        port_name_to_add: str,
        token_to_add: Token,
        output_port_forward: bool,
    ):
        if isinstance(token_to_add, JobToken):
            logger.debug(
                f"add_into_vertex: Token to add is JobToken {token_to_add.value.name} (id {token_to_add.persistent_id}) on port {port_name_to_add}"
            )
        if port_name_to_add in self.port_tokens.keys():
            for sibling_token_id in self.port_tokens[port_name_to_add]:
                sibling_token = self.token_visited[sibling_token_id][0]
                # If there are two of the same token in the same port, keep the newer token or that available
                # In the case of JobTokens, they are the same when they have the same job.name
                # In other token types, they are equal when they have the same tag
                if isinstance(token_to_add, JobToken):
                    if not isinstance(sibling_token, JobToken):
                        raise FailureHandlingException(
                            f"Non è un jobtoken {sibling_token}"
                        )
                    if token_to_add.value.name == sibling_token.value.name:
                        if (
                            self.token_visited[token_to_add.persistent_id][1]
                            or token_to_add.persistent_id > sibling_token_id
                        ):
                            self.port_tokens[port_name_to_add].remove(sibling_token_id)
                            break
                        else:
                            return
                else:
                    if token_to_add.tag == sibling_token.tag:
                        if (
                            self.token_visited[token_to_add.persistent_id][1]
                            or token_to_add.persistent_id > sibling_token_id
                        ):
                            self.port_tokens[port_name_to_add].remove(sibling_token_id)
                            break
                        else:
                            return
        logger.debug(
            f"add_into_vertex: Aggiungo token {token_to_add} id {token_to_add.persistent_id} tag {token_to_add.tag} nella port_tokens[{port_name_to_add}]"
        )
        self.port_tokens.setdefault(port_name_to_add, set()).add(
            token_to_add.persistent_id
        )

    async def add_into_graph(
        self,
        port_name_key: str,
        port_name_to_add: str,
        token_to_add: Token = None,
    ):
        output_port_forward = await is_output_port_forward(
            min(self.port_name_ids[port_name_to_add]), self.context
        )
        if token_to_add:
            self.add_into_vertex(
                port_name_to_add,
                token_to_add,
                output_port_forward,
            )

        # todo: fare i controlli prima di aggiungere.
        #   in particolare se la port è presente in init e si sta aggiungendo in un'altra port
        #       - o non si aggiunge
        #       - o si toglie da init e si aggiunge nella nuova port
        #   stessa cosa viceversa, se è in un'altra port e si sta aggiungendo in init
        # edit. attuale implementazione: si toglie da init e si aggiunge alla port nuova
        if (
            INIT_DAG_FLAG in self.dag_ports.keys()
            and port_name_to_add in self.dag_ports[INIT_DAG_FLAG]
            and port_name_key != INIT_DAG_FLAG
        ):
            logger.debug(
                f"add_into_graph: Inserisco la port {port_name_to_add} dopo la port {port_name_key}. però è già in INIT"
            )
        if port_name_key == INIT_DAG_FLAG and port_name_to_add in [
            pp
            for p, plist in self.dag_ports.items()
            for pp in plist
            if p != INIT_DAG_FLAG
        ]:
            logger.debug(
                f"add_into_graph: Inserisco la port {port_name_to_add} in INIT. però è già in {port_name_key}"
            )
        self.dag_ports.setdefault(port_name_key, set()).add(port_name_to_add)

        # It is possible find some token available and other unavailable in a scatter
        # In this case the port is added in init and in another port.
        # If the port is next in another port, it is necessary to detach the port from the init next list
        if (
            INIT_DAG_FLAG in self.dag_ports.keys()
            and port_name_to_add in self.dag_ports[INIT_DAG_FLAG]
            and is_next_of_someone(port_name_to_add, self.dag_ports)
        ):
            if not output_port_forward:
                logger.debug(
                    f"add_into_graph: port {port_name_to_add} is removed from init "
                )
                self.dag_ports[INIT_DAG_FLAG].remove(port_name_to_add)
            else:
                logger.debug(f"add_into_graph: port {port_name_to_add} is kept in init")

    async def build_dag(
        self, token_frontier, workflow, loading_context, graph_cut=None
    ):
        if not graph_cut:
            graph_cut = []
        # todo: cambiarlo in {token_id: is_available} e quando è necessaria l'istanza del token recuperarla dal loading_context.load_token(id)
        # { token_id: (token, is_available)}
        all_token_visited = self.token_visited

        # {old_job_token_id : job_request_running}
        running_new_job_tokens = {}

        # {old_job_token_id : (new_job_token_id, new_output_token_id)}
        available_new_job_tokens = {}

        # counter of LoopCombinatorStep and LoopTerminator visited
        # if it is odd, it means that the failed step is inside a loop
        counter_loop = 0

        while token_frontier:
            token = token_frontier.pop(0)
            if len(
                graph_cut
            ) > 0 or not await self.context.failure_manager.has_token_already_been_recovered(
                token,
                all_token_visited,
                available_new_job_tokens,
                running_new_job_tokens,
                workflow.context,
            ):
                # impossible case because when added in tokens, the elem is checked
                if token.persistent_id in all_token_visited.keys():
                    raise FailureHandlingException(
                        f"Token {token.persistent_id} already visited"
                    )

                port_row = await workflow.context.database.get_port_from_token(
                    token.persistent_id
                )
                self.port_name_ids.setdefault(port_row["name"], set()).add(
                    port_row["id"]
                )
                step_rows = await get_steps_from_output_port(
                    port_row["id"], workflow.context
                )
                is_available = await _is_token_available(token, workflow.context)

                invalidate = True
                for step_row in step_rows:
                    logger.debug(
                        f"build_dag: Trovato {step_row['name']} di tipo {get_class_from_name(step_row['type'])} dalla port {port_row['name']}"
                    )
                    if issubclass(
                        get_class_from_name(step_row["type"]),
                        (ExecuteStep, InputInjectorStep, BackPropagationTransformer),
                    ):
                        invalidate = False
                    elif issubclass(
                        get_class_from_name(step_row["type"]), CombinatorStep
                    ) and issubclass(
                        get_class_from_name(
                            json.loads(step_row["params"])["combinator"]["type"]
                        ),
                        LoopTerminationCombinator,
                    ):
                        counter_loop += 1
                        if (
                            not self.external_loop_step_name
                            and counter_loop % 2 == 1
                            and issubclass(
                                get_class_from_name(
                                    json.loads(step_row["params"])["combinator"]["type"]
                                ),
                                LoopTerminationCombinator,
                            )
                        ):
                            raise FailureHandlingException(
                                "Visited a LoopTerminationCombinator but not a CWLLoopConditionalStep before"
                            )
                    elif issubclass(
                        get_class_from_name(step_row["type"]), CWLLoopConditionalStep
                    ):
                        counter_loop += 1
                        if not self.external_loop_step_name and counter_loop % 2 == 1:
                            self.external_loop_step_name = step_row["name"]
                if invalidate:
                    is_available = False
                if port_row["name"] in graph_cut:
                    logger.debug(
                        f"build_dag: port {port_row['name']} in graph_cut and token {token.persistent_id} turned available True (original val {is_available}). Graph_cut len {len(graph_cut)} -> {graph_cut}"
                    )
                    is_available = True
                all_token_visited[token.persistent_id] = (token, is_available)
                if not is_available:
                    if prev_tokens := await loading_context.load_prev_tokens(
                        workflow.context, token.persistent_id
                    ):
                        prev_port_rows = await asyncio.gather(
                            *(
                                asyncio.create_task(
                                    workflow.context.database.get_port_from_token(
                                        pt.persistent_id
                                    )
                                )
                                for pt in prev_tokens
                            )
                        )
                        logger.debug(
                            f"build_dag: Dal token id {token.persistent_id} ho recuperato prev-tokens {[pt.persistent_id for pt in prev_tokens]}"
                        )
                        for pt, prev_port_row in zip(prev_tokens, prev_port_rows):
                            self.port_name_ids.setdefault(
                                prev_port_row["name"], set()
                            ).add(prev_port_row["id"])
                            await self.add_into_graph(
                                prev_port_row["name"],
                                port_row["name"],
                                token,
                            )
                            if (
                                pt.persistent_id not in all_token_visited.keys()
                                and not contains_id(pt.persistent_id, token_frontier)
                            ):
                                token_frontier.append(pt)
                    else:
                        logger.debug(
                            f"build_dag: token id {token.persistent_id} non ha token precedenti"
                        )
                        await self.add_into_graph(
                            INIT_DAG_FLAG,
                            port_row["name"],
                            token,
                        )
                else:
                    logger.debug(
                        f"build_dag: Il token {token.persistent_id} è disponibile, quindi aggiungo la sua port {port_row['name']} tra gli init"
                    )
                    await self.add_into_graph(
                        INIT_DAG_FLAG,
                        port_row["name"],
                        token,
                    )
            else:
                logger.debug(
                    f"build_dag: Il token {token.persistent_id} è disponibile su has_token_already_been_recovered (new tokens {available_new_job_tokens[token.persistent_id]})"
                )
                port_row = await workflow.context.database.get_port_from_token(
                    token.persistent_id
                )
                await self.add_into_graph(
                    INIT_DAG_FLAG,
                    port_row["name"],
                    token,
                )
        logger.debug(
            f"build_dag: len_token_visited_original: {len(self.token_visited)} - len_after_checking: {len(get_necessary_tokens(self.port_tokens, all_token_visited))}"
        )
        logger.debug(
            f"build_dag: JobTokens: {set([t.value.name for t, _ in get_necessary_tokens(self.port_tokens, all_token_visited).values() if isinstance(t, JobToken)])}",
        )

        all_token_visited = dict(sorted(all_token_visited.items()))
        for t, a in all_token_visited.values():
            logger.debug(
                f"build_dag: Token id: {t.persistent_id} tag: {t.tag} val: {t} is available? {a}"
            )
        logger.debug(f"PORT_TOKENS: {convert_to_json(self.port_tokens)}")
        logger.debug(f"DAG_PORTS: {convert_to_json(self.dag_ports)}")

        logger.debug(
            f"build_dag: available_new_job_tokens n.elems: {len(available_new_job_tokens)} {json.dumps(available_new_job_tokens, indent=2)}"
        )
        # json_running = json.dumps(
        #     {
        #         k: {
        #             "job_token": v.job_token.value.name if v.job_token else None,
        #             "is_running": v.is_running,
        #             "queue": [(x.port.name, x.waiting_token) for x in v.queue],
        #         }
        #         for k, v in running_new_job_tokens.items()
        #     },
        #     indent=2,
        # )
        logger.debug(
            f"build_dag: running_new_job_tokens n.elems: {len(available_new_job_tokens)}"  # f"{json_running}"
        )

        logger.debug("build_dag: pre riduzione")
        await self.reduce_graph(
            available_new_job_tokens,
            loading_context,
        )
        logger.debug(
            f"build_dag: grafo ridotto - JobTokens: {set([t.value.name for t, _ in get_necessary_tokens(self.port_tokens, all_token_visited).values() if isinstance(t, JobToken)])}"
        )

    async def reduce_graph(
        self,
        available_new_job_tokens,
        loading_context,
    ):
        for v in available_new_job_tokens.values():
            if v["out-token-port-name"] not in self.port_tokens.keys():
                raise FailureHandlingException("Non c'è la porta. NON VA BENE")

        # todo: aggiustare nel caso in cui uno step abbia più port di output.
        #   Nella replace_token, se i token della port sono tutti disponibili
        #   allora rimuove tutte le port precedenti.
        #   ma se i token della seconda port di output dello step non sono disponibili
        #   è necessario eseguire tutte le port precedenti.
        for v in available_new_job_tokens.values():
            if v["out-token-port-name"] in self.port_tokens.keys():
                await self.replace_token(
                    v["out-token-port-name"],
                    v["old-out-token"],
                    self.token_visited[v["new-out-token"]][0],
                )
                await self.remove_prev(
                    v["old-out-token"],
                    loading_context,
                )
                logger.debug(
                    f"reduce_graph: Port_tokens {v['out-token-port-name']} - token {v['old-out-token']} sostituito con token {v['new-out-token']}. Quindi la port ha {self.port_tokens[v['out-token-port-name']]} tokens"
                )
            else:
                logger.debug(
                    f"reduce_graph: Port_tokens {v['out-token-port-name']} - port non più presente. Non serve più eseguire lo step annesso"
                )

    async def replace_token(
        self,
        port_name,
        token_id_to_replace: int,
        token_to_add: Token,
    ):
        if token_id_to_replace in self.port_tokens[port_name]:
            self.port_tokens[port_name].remove(token_id_to_replace)
        # token_visited.pop(token_id_to_replace, None)
        self.port_tokens[port_name].add(token_to_add.persistent_id)
        self.token_visited[token_to_add.persistent_id] = (token_to_add, True)
        logger.debug(
            f"replace_token: replace token {token_id_to_replace} con {token_to_add.persistent_id} - I token della port {port_name} sono tutti disp? {all((self.token_visited[t_id][1] for t_id in self.port_tokens[port_name]))}"
        )
        if all((self.token_visited[t_id][1] for t_id in self.port_tokens[port_name])):
            for prev_port_name in get_prev_vertices(port_name, self.dag_ports):
                await self.remove_from_graph(prev_port_name)
            await self.add_into_graph(INIT_DAG_FLAG, port_name, None)

    async def remove_from_graph(self, port_name_to_remove: str):
        other_ports_to_remove = set()
        # Removed all occurrences as next port
        for port_name, next_port_names in self.dag_ports.items():
            if port_name_to_remove in next_port_names:
                next_port_names.remove(port_name_to_remove)
            if len(next_port_names) == 0:
                other_ports_to_remove.add(port_name)

        # removed all tokens generated by port
        for t_id in self.port_tokens.pop(port_name_to_remove, ()):
            # token_visited.pop(t_id)
            logger.debug(
                f"remove_from_graph: pop token id {t_id} from port_tokens[{port_name_to_remove}]"
            )

        # removed in the graph and moved its next port in INIT
        for port_name in self.dag_ports.pop(port_name_to_remove, ()):
            if not is_next_of_someone(port_name, self.dag_ports):
                await self.add_into_graph(INIT_DAG_FLAG, port_name, None)
        logger.debug(
            f"remove_from_graph: pop {port_name_to_remove} from dag_ports and port_tokens"
        )
        # remove vertex detached from the graph
        for port_name in other_ports_to_remove:
            await self.remove_from_graph(port_name)

    async def remove_token_by_id(self, token_id_to_remove: int):
        # token_visited.pop(token_id_to_remove, None)
        logger.debug(
            f"remove_token_by_id: remove token {token_id_to_remove} from visited"
        )
        ports_to_remove = set()
        for port_name, token_ids in self.port_tokens.items():
            if token_id_to_remove in token_ids:
                token_ids.remove(token_id_to_remove)
                logger.debug(
                    f"remove_token_by_id: remove token {token_id_to_remove} from {port_name}"
                )
            if len(token_ids) == 0:
                ports_to_remove.add(port_name)
        for port_name in ports_to_remove:
            await self.remove_from_graph(port_name)

    async def remove_token(self, token, port_name):
        msg = (
            "job {token.value.name}"
            if isinstance(token, JobToken)
            else f"tag {token.tag}"
        )
        if port_name in self.port_tokens.keys():
            for t_id in self.port_tokens[port_name]:
                if (
                    not isinstance(token, JobToken)
                    and self.token_visited[t_id][0].tag == token.tag
                ) or (
                    isinstance(token, JobToken)
                    and self.token_visited[t_id][0].value.name == token.value.name
                ):
                    logger.debug(
                        f"remove_token: Rimuovo token {t_id} con {msg} dal port_tokens[{port_name}]"
                    )
                    await self.remove_token_by_id(t_id)
                    return
            logger.debug(
                f"remove_token: Volevo rimuovere token con {msg} dal port_tokens[{port_name}] ma non ce n'è"
            )
        else:
            logger.debug(
                f"remove_token: Volevo rimuovere token con {msg} ma non c'è port {port_name} in port_tokens"
            )

    # richiamare ricorsivamente finche i token.tag hanno sempre lo stesso livello o se si trova un JobToken
    async def remove_prev(
        self,
        token_id_to_remove: int,
        loading_context,
    ):
        curr_tag_level = get_tag_level(self.token_visited[token_id_to_remove][0].tag)
        for token_row in await self.context.database.get_dependees(token_id_to_remove):
            port_row = await self.context.database.get_port_from_token(
                token_row["dependee"]
            )
            if token_row["dependee"] not in self.token_visited.keys():
                logger.debug(
                    f"remove_prev: token id {token_row['dependee']} non presente nei token_visited"
                )
                continue
            elif port_row["type"] != get_class_fullname(ConnectorPort):
                if token_row["dependee"] not in self.token_visited.keys():
                    self.token_visited[token_row["dependee"]] = (
                        await loading_context.load_token(
                            self.context, token_row["dependee"]
                        ),
                        False,
                    )
                    logger.debug(
                        f"remove_prev: prev token id {token_row['dependee']} tag {self.token_visited[token_row['dependee']][0].tag} non è presente tra i token visitati, lo aggiungo"
                    )
                logger.debug(
                    f"remove_prev: token {token_id_to_remove} rm prev token id: {token_row['dependee']} val: {self.token_visited[token_row['dependee']][0]} tag: {self.token_visited[token_row['dependee']][0].tag}"
                )

                port_dependee_row = await self.context.database.get_port_from_token(
                    token_row["dependee"]
                )
                dependee_token = self.token_visited[token_row["dependee"]][0]
                a = port_dependee_row["type"] == get_class_fullname(JobPort)
                b = isinstance(dependee_token, JobToken)
                if a != b:
                    raise FailureHandlingException("Diverse port")
                if isinstance(dependee_token, JobToken) or (
                    not isinstance(self.token_visited[token_id_to_remove][0], JobToken)
                    and curr_tag_level == get_tag_level(dependee_token.tag)
                ):
                    await self.remove_token(dependee_token, port_dependee_row["name"])
                    msg = (
                        f"job {dependee_token.value.name} "
                        if isinstance(dependee_token, JobToken)
                        else ""
                    )
                    logger.debug(
                        f"remove_prev: token {token_id_to_remove} tag {self.token_visited[token_id_to_remove][0].tag} (lvl {curr_tag_level}) ha prev {msg}id {token_row['dependee']} tag {dependee_token.tag} (lvl {get_tag_level(dependee_token.tag)}) -> {curr_tag_level == get_tag_level(dependee_token.tag)}"
                    )
                    await self.remove_prev(token_row["dependee"], loading_context)
                else:
                    logger.debug(
                        f"remove_prev: pulizia fermata al token {token_id_to_remove} con tag {self.token_visited[token_id_to_remove][0].tag}"
                    )
            else:
                logger.debug(
                    f"remove_prev: token {token_id_to_remove} volevo rm prev token id {token_row['dependee']} tag {self.token_visited[token_row['dependee']][0].tag} ma è un token del ConnectorPort"
                )

    async def explore_top_down(self):
        ports_frontier = {port_name for port_name in self.dag_ports[INIT_DAG_FLAG]}

        ports_visited = set()

        while ports_frontier:
            port_name = ports_frontier.pop()
            ports_visited.add(port_name)
            port_id = min(self.port_name_ids[port_name])
            dep_steps_port_rows = await self.context.database.get_steps_from_input_port(
                port_id
            )
            step_rows = await asyncio.gather(
                *(
                    asyncio.create_task(
                        self.context.database.get_step(dependency_row["step"])
                    )
                    for dependency_row in dep_steps_port_rows
                )
            )
            for step_row in step_rows:
                for port_row in await get_output_ports(step_row["id"], self.context):
                    self.port_name_ids.setdefault(port_row["name"], set()).add(
                        port_row["id"]
                    )
                    if port_row["name"] not in ports_visited:
                        ports_frontier.add(port_row["name"])
                    await self.add_into_graph(port_name, port_row["name"])
        pass

    async def get_port_and_step_ids(self):
        steps = set()
        ports = {
            min(self.port_name_ids[port_name]) for port_name in self.port_tokens.keys()
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
                    f"get_port_and_step_ids: Step {(await self.context.database.get_step(row_dependency['step']))['name']} recuperato dalla port {get_key_by_value(row_dependency['port'], self.port_name_ids)}"
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
                    step_to_remove.add(step_id)
        for s_id in step_to_remove:
            steps.remove(s_id)
        pass
        return ports, steps


async def _put_tokens(
    new_workflow: Workflow,
    init_ports: MutableSet[str],
    port_tokens: MutableMapping[str, MutableSet[int]],
    token_visited: MutableMapping[int, Tuple[Token, bool]],
    wr,
):
    last_iteration = {}
    for port_name in init_ports:
        token_list = [
            token_visited[t_id][0]
            for t_id in port_tokens[port_name]
            if isinstance(t_id, int) and token_visited[t_id][1]
        ]
        token_list.sort(key=lambda x: x.tag, reverse=False)
        for i, t in enumerate(token_list):
            for t1 in token_list[i:]:
                if t.persistent_id != t1.persistent_id and t.tag == t1.tag:
                    raise FailureHandlingException(
                        f"Tag ripetuto id1: {t.persistent_id} id2: {t1.persistent_id}"
                    )
        port = new_workflow.ports[port_name]
        is_back_prop_output_port = any(
            s
            for s in port.get_input_steps()
            if isinstance(s, BackPropagationTransformer)
        )
        loop_combinator_input = any(
            s for s in port.get_output_steps() if isinstance(s, LoopCombinatorStep)
        )
        if loop_combinator_input and len(port_tokens[port_name]) > 1:
            # token_list.pop()
            # wr.port_tokens[port.name].pop()
            last_iteration.setdefault(port.name, set())
            for p_id in wr.port_name_ids[port.name]:
                last_iteration[port.name].add(p_id)

        for t in token_list:
            if isinstance(t, (TerminationToken, IterationTerminationToken)):
                raise FailureHandlingException(
                    f"Added {type(t)} into port {port.name} but it is wrong"
                )
            port.put(t)
            if is_back_prop_output_port:
                port.put(TerminationToken())
                break

        len_port_token_list = len(port.token_list)
        len_port_tokens = len(port_tokens[port_name])

        if len_port_token_list > 0 and len_port_token_list == len_port_tokens:
            if loop_combinator_input and not is_back_prop_output_port:
                if last_iteration:
                    if isinstance(port.token_list[-1], TerminationToken):
                        token = port.token_list[-1]
                    else:
                        token = port.token_list[-1]
                        # tag = port.token_list[-1].tag.split(".")
                        # tag[-1] = str(int(tag[-1]) + 1)
                        # token = Token(tag=".".join(tag), value=None)
                        logger.debug(
                            f"put_tokens: Port {port.name}, with {len(port.token_list)} tokens, inserts EMPTY token with tag {token.tag}"
                        )
                        # port.put(token)
                    logger.debug(
                        f"put_tokens: Port {port.name}, with {len(port.token_list)} tokens, inserts IterationTerminationToken with tag {token.tag}"
                    )
                    port.put(IterationTerminationToken(token.tag))
                logger.debug(
                    f"put_tokens: Port {port.name}, with {len(port.token_list)} tokens, inserts IterationTerminationToken with tag {port.token_list[0].tag}"
                )
                port.put(IterationTerminationToken(port.token_list[0].tag))
            if not any(t for t in port.token_list if isinstance(t, TerminationToken)):
                logger.debug(
                    f"put_tokens: Port {port.name}, with {len(port.token_list)} tokens, insert termination token"
                )
                port.put(TerminationToken())
        else:
            logger.debug(
                f"put_tokens: Port {port.name}, with {len(port.token_list)} tokens, does NOT insert TerminationToken"
            )

    for step in new_workflow.steps.values():
        if isinstance(step, LoopCombinatorStep):
            t = list(step.get_input_ports().values()).pop().token_list[0]
            if t.tag.split(".")[-1] != "0":
                prefix = ".".join(t.tag.split(".")[:-1])
                step.combinator.iteration_map[prefix] = int(t.tag.split(".")[-1])
                logger.debug(
                    f"recover_jobs-last_iteration: Step {step.name} combinator updated map[{prefix}] = {step.combinator.iteration_map[prefix]}"
                )
    return last_iteration


async def load_and_add_ports(port_ids, new_workflow, loading_context):
    for port in await asyncio.gather(
        *(
            asyncio.create_task(
                Port.load(
                    new_workflow.context,
                    port_id,
                    loading_context,
                    new_workflow,
                )
            )
            for port_id in port_ids
        )
    ):
        if port.name not in new_workflow.ports.keys():
            new_workflow.add_port(port)
            logger.debug(
                f"populate_workflow: wf {new_workflow.name} add_1 port {port.name}"
            )
        else:
            logger.debug(
                f"populate_workflow: La port {port.name} è già presente nel workflow {new_workflow.name}"
            )
    logger.debug("populate_workflow: Port caricate")


async def load_and_add_steps(step_ids, new_workflow, wr, loading_context):
    new_step_ids = set()
    step_name_id = {}
    for sid, step in zip(
        step_ids,
        await asyncio.gather(
            *(
                asyncio.create_task(
                    Step.load(
                        new_workflow.context,
                        step_id,
                        loading_context,
                        new_workflow,
                    )
                )
                for step_id in step_ids
            )
        ),
    ):
        step_name_id[step.name] = sid

        # if there are not the input ports in the workflow, the step is not added
        if not (set(step.input_ports.values()) - set(new_workflow.ports.keys())):
            # removesuffix python 3.9
            if isinstance(step, CWLLoopConditionalStep) and (
                wr.external_loop_step_name.removesuffix("-recovery")
                == step.name.removesuffix("-recovery")
            ):
                if not wr.external_loop_step:
                    wr.external_loop_step = step
                else:
                    continue
            elif isinstance(step, OutputForwardTransformer):
                port_id = min(wr.port_name_ids[step.get_output_port().name])
                for (
                    step_dep_row
                ) in await new_workflow.context.database.get_steps_from_input_port(
                    port_id
                ):
                    step_row = await new_workflow.context.database.get_step(
                        step_dep_row["step"]
                    )
                    if step_row["name"] not in new_workflow.steps.keys() and issubclass(
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
                    if len(wr.port_tokens[step.output_ports[port_dep_row["name"]]]) > 1:
                        for (
                            step_dep_row
                        ) in await new_workflow.context.database.get_steps_from_output_port(
                            port_dep_row["port"]
                        ):
                            step_row = await new_workflow.context.database.get_step(
                                step_dep_row["step"]
                            )
                            if issubclass(
                                get_class_from_name(step_row["type"]), CombinatorStep
                            ) and issubclass(
                                get_class_from_name(
                                    json.loads(step_row["params"])["combinator"]["type"]
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
                    Step.load(
                        new_workflow.context,
                        step_id,
                        loading_context,
                        new_workflow,
                    )
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


async def load_missing_ports(new_workflow, step_name_id, loading_context):
    missing_ports = set()
    for step in new_workflow.steps.values():
        if isinstance(step, InputInjectorStep):
            continue
        for dep_name, p_name in step.output_ports.items():
            if p_name not in new_workflow.ports.keys():
                # problema nato dai loop. when-loop ha output tutti i param. Però il grafo è costruito sulla presenza o
                # meno dei file, invece i param str, int, ..., no. Quindi le port di questi param non vengono esplorate
                # le aggiungo ma durante l'esecuzione verranno utilizzati come "port pozzo" dei token prodotti
                logger.debug(
                    f"populate_workflow: Aggiungo port {p_name} al wf {new_workflow.name} perché è un output port dello step {step.name}"
                )
                depe_row = await new_workflow.context.database.get_output_port(
                    step_name_id[step.name], dep_name
                )
                missing_ports.add(depe_row["port"])
    for port in await asyncio.gather(
        *(
            asyncio.create_task(
                Port.load(new_workflow.context, p_id, loading_context, new_workflow)
            )
            for p_id in missing_ports
        )
    ):
        logger.debug(
            f"populate_workflow: wf {new_workflow.name} add_2 port {port.name}"
        )
        new_workflow.add_port(port)


def get_failed_loop_conditional_step(new_workflow, wr):
    if not wr.external_loop_step_name:
        return None

    return wr.external_loop_step


async def _populate_workflow(
    wr,
    port_ids: MutableSequence[int],
    step_ids: MutableSequence[int],
    failed_step,
    new_workflow,
    loading_context,
):
    logger.debug(
        f"populate_workflow: wf {new_workflow.name} dag[INIT] {wr.dag_ports[INIT_DAG_FLAG]}"
    )
    logger.debug(
        f"populate_workflow: wf {new_workflow.name} port {new_workflow.ports.keys()}"
    )
    await load_and_add_ports(port_ids, new_workflow, loading_context)

    step_name_id = await load_and_add_steps(step_ids, new_workflow, wr, loading_context)

    await load_missing_ports(new_workflow, step_name_id, loading_context)

    # add failed step into new_workflow
    logger.debug(
        f"populate_workflow: wf {new_workflow.name} add_3.0 step {failed_step.name}"
    )
    new_workflow.add_step(
        await Step.load(
            new_workflow.context,
            failed_step.persistent_id,
            loading_context,
            new_workflow,
        )
    )
    for port in await asyncio.gather(
        *(
            asyncio.create_task(
                Port.load(
                    new_workflow.context, p.persistent_id, loading_context, new_workflow
                )
            )
            for p in failed_step.get_output_ports().values()
        )
    ):
        if port.name not in new_workflow.ports.keys():
            logger.debug(
                f"populate_workflow: wf {new_workflow.name} add_3 port {port.name}"
            )
            new_workflow.add_port(port)

    if replace_step := get_failed_loop_conditional_step(new_workflow, wr):
        port_name = list(replace_step.input_ports.values()).pop()
        ll_cond_step = CWLRecoveryLoopConditionalStep(
            replace_step.name
            if isinstance(replace_step, CWLRecoveryLoopConditionalStep)
            else replace_step.name + "-recovery",
            new_workflow,
            get_recovery_loop_expression(
                # len(wr.port_tokens[port_name])
                1
                if len(wr.port_tokens[port_name]) == 1
                else len(wr.port_tokens[port_name]) - 1
            ),
            full_js=True,
        )
        logger.debug(
            f"Step {ll_cond_step.name} (wf {new_workflow.name}) setted with expression: {ll_cond_step.expression}"
        )
        for dep_name, port in replace_step.get_input_ports().items():
            logger.debug(
                f"Step {ll_cond_step.name} (wf {new_workflow.name}) add input port {dep_name} {port.name}"
            )
            ll_cond_step.add_input_port(dep_name, port)
        for dep_name, port in replace_step.get_output_ports().items():
            logger.debug(
                f"Step {ll_cond_step.name} (wf {new_workflow.name}) add output port {dep_name} {port.name}"
            )
            ll_cond_step.add_output_port(dep_name, port)
        for dep_name, port in replace_step.get_skip_ports().items():
            logger.debug(
                f"Step {ll_cond_step.name} (wf {new_workflow.name}) add skip port {dep_name} {port.name}"
            )
            ll_cond_step.add_skip_port(dep_name, port)
        new_workflow.steps.pop(replace_step.name)
        new_workflow.add_step(ll_cond_step)
        logger.debug(
            f"populate_workflow: (3) Step {ll_cond_step.name} caricato nel wf {new_workflow.name}"
        )
        logger.debug(
            f"populate_workflow: Rimuovo lo step {replace_step.name} dal wf {new_workflow.name} perché lo rimpiazzo con il nuovo step {ll_cond_step.name}"
        )

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
                # nel grafo. Però nei loop, più step hanno stessa porta di output (forwad, backprop, loop-term).
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
    # tmp soluzione. risolvere a monte, nel momento in cui si fa la load
    for s in new_workflow.steps.values():
        if isinstance(s, CWLTokenTransformer) and isinstance(
            s.processor, CWLTokenProcessor
        ):
            if not graph:
                graph = s.processor.format_graph
            else:
                s.processor.format_graph = graph
    logger.debug("populate_workflow: Finish")


class JobVersion:
    __slots__ = ("job", "outputs", "step", "version")

    def __init__(
        self,
        job: Job = None,
        outputs: MutableMapping[str, Token] | None = None,
        step: Step = None,
        version: int = 1,
    ):
        self.job: Job = job
        self.outputs: MutableMapping[str, Token] | None = outputs
        self.step: Step = step
        self.version: int = version
