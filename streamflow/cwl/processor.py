from __future__ import annotations

import asyncio
from typing import Any, Callable, MutableMapping, MutableSequence, Optional, Type, cast

import cwltool.builder
from rdflib import Graph
from schema_salad.exceptions import ValidationException

from streamflow.core.context import StreamFlowContext
from streamflow.core.data import LOCAL_LOCATION
from streamflow.core.exception import WorkflowDefinitionException, WorkflowExecutionException
from streamflow.core.persistence import DatabaseLoadingContext
from streamflow.core.utils import flatten_list, get_class_fullname, get_path_processor, get_tag
from streamflow.core.workflow import CommandOutput, CommandOutputProcessor, Job, Status, Token, TokenProcessor, Workflow
from streamflow.cwl import utils
from streamflow.cwl.command import CWLCommandOutput
from streamflow.cwl.token import CWLFileToken
from streamflow.cwl.utils import LoadListing, SecondaryFile
from streamflow.workflow.token import ListToken, ObjectToken


async def _save_processors_dict(context: StreamFlowContext,
                                processors: MutableMapping[str, Any]):
    return {k: {'type': get_class_fullname(type(p)), 'params': v} for k, p, v in zip(
        processors.keys(),
        processors.values(),
        await asyncio.gather(
            *(asyncio.create_task(v.save(context)) for v in processors.values())))}


class CWLTokenProcessor(TokenProcessor):

    def __init__(self,
                 name: str,
                 workflow: Workflow,
                 token_type: Optional[str] = None,
                 check_type: bool = True,
                 enum_symbols: Optional[MutableSequence[str]] = None,
                 expression_lib: Optional[MutableSequence[str]] = None,
                 file_format: Optional[str] = None,
                 format_graph: Optional[Graph] = None,
                 full_js: bool = False,
                 load_contents: Optional[bool] = None,
                 load_listing: Optional[LoadListing] = None,
                 only_propagate_secondary_files: bool = True,
                 optional: bool = False,
                 secondary_files: Optional[MutableSequence[SecondaryFile]] = None,
                 streamable: bool = False):
        super().__init__(name, workflow)
        self.token_type: str = token_type
        self.check_type: bool = check_type
        self.enum_symbols: Optional[MutableSequence[str]] = enum_symbols
        self.expression_lib: Optional[MutableSequence[str]] = expression_lib
        self.file_format: Optional[str] = file_format
        self.format_graph: Optional[Graph] = format_graph
        self.full_js: bool = full_js
        self.load_contents: Optional[bool] = load_contents
        self.load_listing: Optional[LoadListing] = load_listing
        self.only_propagate_secondary_files: bool = only_propagate_secondary_files
        self.optional: bool = optional
        self.secondary_files: MutableSequence[SecondaryFile] = secondary_files or []
        self.streamable: bool = streamable

    @classmethod
    async def _load(cls,
                    context: StreamFlowContext,
                    row: MutableMapping[str, Any],
                    loading_context: DatabaseLoadingContext) -> CWLTokenProcessor:
        format_graph = Graph()
        return cls(
            name=row['name'],
            workflow=await loading_context.load_workflow(context, row['workflow']),
            token_type=row['token_type'],
            check_type=row['check_type'],
            enum_symbols=row['enum_symbols'],
            expression_lib=row['expression_lib'],
            file_format=row['file_format'],
            format_graph=(format_graph.parse(data=row['format_graph']) if row['format_graph'] is not None
                          else format_graph),
            full_js=row['full_js'],
            load_contents=row['load_contents'],
            load_listing=LoadListing(row['load_listing']) if row['load_listing'] is not None else None,
            only_propagate_secondary_files=row['only_propagate_secondary_files'],
            optional=row['optional'],
            secondary_files=[SecondaryFile(sf['pattern'], sf['required']) for sf in row['secondary_files']],
            streamable=row['streamable'])

    async def _process_file_token(self, inputs: MutableMapping[str, Token], token_value: Any):
        # Build context
        context = utils.build_context(inputs)
        # Get path
        filepath = utils.get_path_from_token(token_value)
        # Check file format if present
        if self.file_format:
            context = {**context, **{'self': token_value}}
            input_formats = utils.eval_expression(
                expression=self.file_format,
                context=context,
                full_js=self.full_js,
                expression_lib=self.expression_lib)
            try:
                cwltool.builder.check_format(token_value, input_formats, self.format_graph)
            except ValidationException as e:
                raise WorkflowExecutionException(e.message) from e
        # If file exists, get coordinates
        if filepath and (
                data_location := self.workflow.context.data_manager.get_source_location(filepath, LOCAL_LOCATION)):
            connector = self.workflow.context.deployment_manager.get_connector(data_location.deployment)
            path_processor = utils.get_path_processor(connector)
            location = data_location.location
            base_path = path_processor.normpath(data_location.path[:-len(data_location.relpath)])
            # Process file contents
            token_value = await utils.update_file_token(
                context=self.workflow.context,
                connector=connector,
                location=location,
                token_value=token_value,
                load_contents=self.load_contents,
                load_listing=self.load_listing)
            # Process secondary files
            if token_value.get('secondaryFiles'):
                initial_paths = [utils.get_path_from_token(sf) for sf in token_value['secondaryFiles']]
                sf_map = dict(zip(initial_paths, await asyncio.gather(*(asyncio.create_task(
                    utils.update_file_token(
                        context=self.workflow.context,
                        connector=connector,
                        location=location,
                        token_value=sf,
                        load_contents=self.load_contents,
                        load_listing=self.load_listing
                    )) for sf in token_value['secondaryFiles']))))
            else:
                sf_map = {}
            if self.secondary_files:
                sf_context = {**context, 'self': token_value}
                await utils.process_secondary_files(
                    context=self.workflow.context,
                    secondary_files=self.secondary_files,
                    sf_map=sf_map,
                    js_context=sf_context,
                    full_js=self.full_js,
                    expression_lib=self.expression_lib,
                    connector=connector,
                    locations=[location],
                    token_value=token_value,
                    load_contents=self.load_contents,
                    load_listing=self.load_listing,
                    only_retrieve_from_token=self.only_propagate_secondary_files)
            # Add all secondary files to the token
            if sf_map:
                token_value['secondaryFiles'] = list(sf_map.values())
            # Register path
            await utils.register_data(
                context=self.workflow.context,
                connector=connector,
                locations=[location],
                token_value=token_value,
                base_path=base_path)
        # Return token value
        return token_value

    async def process(self, inputs: MutableMapping[str, Token], token: Token) -> Token:
        # If value is token, propagate the process call
        if isinstance(token.value, Token):
            return token.update(await self.process(inputs, token.value))
        # Check type
        if self.check_type and self.token_type:
            utils.check_token_type(
                name=self.name,
                token_value=token.value,
                token_type=self.token_type,
                enum_symbols=self.enum_symbols,
                optional=self.optional)
        # Process token
        if utils.get_token_class(token.value) in ['File', 'Directory']:
            return token.update(await self._process_file_token(inputs, token.value))
        else:
            return token

    async def save(self, context: StreamFlowContext):
        return {**await super().save(context),
                **{'token_type': self.token_type,
                   'check_type': self.check_type,
                   'enum_symbols': self.enum_symbols,
                   'expression_lib': self.expression_lib,
                   'file_format': self.file_format,
                   'format_graph': self.format_graph.serialize() if self.format_graph else None,
                   'full_js': self.full_js,
                   'load_contents': self.load_contents,
                   'load_listing': self.load_listing.value if self.load_listing else None,
                   'only_propagate_secondary_files': self.only_propagate_secondary_files,
                   'optional': self.optional,
                   'secondary_files': await asyncio.gather(*(asyncio.create_task(s.save(context))
                                                             for s in self.secondary_files)),
                   'streamable': self.streamable}}


class CWLCommandOutputProcessor(CommandOutputProcessor):

    def __init__(self,
                 name: str,
                 workflow: Workflow,
                 token_type: Optional[str] = None,
                 enum_symbols: Optional[MutableSequence[str]] = None,
                 expression_lib: Optional[MutableSequence[str]] = None,
                 file_format: Optional[str] = None,
                 full_js: bool = False,
                 glob: Optional[str] = None,
                 load_contents: bool = False,
                 load_listing: LoadListing = LoadListing.no_listing,
                 optional: bool = False,
                 output_eval: Optional[str] = None,
                 secondary_files: Optional[MutableSequence[SecondaryFile]] = None,
                 streamable: bool = False):
        super().__init__(name, workflow)
        self.token_type: str = token_type
        self.enum_symbols: Optional[MutableSequence[str]] = enum_symbols
        self.expression_lib: Optional[MutableSequence[str]] = expression_lib
        self.file_format: Optional[str] = file_format
        self.full_js: bool = full_js
        self.glob: Optional[str] = glob
        self.load_contents: bool = load_contents
        self.load_listing: LoadListing = load_listing
        self.optional: bool = optional
        self.output_eval: Optional[str] = output_eval
        self.secondary_files: MutableSequence[SecondaryFile] = secondary_files or []
        self.streamable: bool = streamable

    async def _build_token(self,
                           job: Job,
                           context: MutableMapping[str, Any],
                           token_value: Any) -> Token:
        if isinstance(token_value, MutableMapping):
            if utils.get_token_class(token_value) in ['File', 'Directory']:
                connector = self.workflow.context.scheduler.get_connector(job.name)
                locations = self.workflow.context.scheduler.get_locations(job.name)
                # Register path
                await utils.register_data(
                    context=self.workflow.context,
                    connector=connector,
                    locations=locations,
                    token_value=token_value,
                    base_path=job.output_directory)
                # Process file format
                if self.file_format:
                    context = {**context, **{'self': token_value}}
                    token_value['format'] = utils.eval_expression(
                        expression=self.file_format,
                        context=context,
                        full_js=self.full_js,
                        expression_lib=self.expression_lib)
                return CWLFileToken(
                    value=token_value,
                    tag=get_tag(job.inputs.values()))
            else:
                token_tasks = {k: asyncio.create_task(
                    self._build_token(job, context, v)) for k, v in token_value.items()}
                return ObjectToken(
                    value=dict(zip(token_tasks.keys(), await asyncio.gather(*token_tasks.values()))),
                    tag=get_tag(job.inputs.values()))
        elif isinstance(token_value, MutableSequence):
            return ListToken(
                value=await asyncio.gather(*(asyncio.create_task(
                    self._build_token(job, context, t)) for t in token_value)),
                tag=get_tag(job.inputs.values()))
        else:
            return Token(
                value=token_value,
                tag=get_tag(job.inputs.values()))

    async def _process_command_output(self,
                                      job: Job,
                                      command_output: CWLCommandOutput,
                                      context: MutableMapping[str, Any]):
        scheduler = self.workflow.context.scheduler
        connector = scheduler.get_connector(job.name)
        locations = scheduler.get_locations(job.name)
        path_processor = get_path_processor(connector)
        token_value = command_output.value
        # If `token_value` is a dictionary, directly extract the token value from it
        if isinstance(token_value, MutableMapping) and self.name in token_value:
            token = token_value[self.name]
            return await utils.build_token_value(
                context=self.workflow.context,
                js_context=context,
                full_js=self.full_js,
                expression_lib=self.expression_lib,
                secondary_files=self.secondary_files,
                connector=connector,
                locations=locations,
                token_value=token,
                load_contents=self.load_contents,
                load_listing=self.load_listing)
        # Otherwise, generate the output object as described in `outputs` field
        if self.glob is not None:
            # Adjust glob path
            if '$(' in self.glob or '${' in self.glob:
                globpath = utils.eval_expression(
                    expression=self.glob,
                    context=context,
                    full_js=self.full_js,
                    expression_lib=self.expression_lib)
            else:
                globpath = self.glob
            # Resolve glob
            resolve_tasks = []
            for location in locations:
                if isinstance(globpath, MutableSequence):
                    for path in globpath:
                        if not path_processor.isabs(path):
                            path = path_processor.join(job.output_directory, path)
                        resolve_tasks.append(utils.expand_glob(job, self.workflow, connector, location, path))
                else:
                    if not path_processor.isabs(globpath):
                        globpath = path_processor.join(job.output_directory, globpath)
                    resolve_tasks.append(utils.expand_glob(job, self.workflow, connector, location, globpath))
            paths, effective_paths = ([list(x) for x in zip(*flatten_list(await asyncio.gather(*resolve_tasks)))]
                                      or [[], []])
            # Get token class from paths
            class_tasks = [asyncio.create_task(utils.get_class_from_path(p, job, self.workflow.context))
                           for p in effective_paths]
            paths = [{'path': p, 'class': c} for p, c in zip(paths, await asyncio.gather(*class_tasks))]
            # If evaluation is not needed, simply return paths as token value
            if self.output_eval is None:
                token_list = await utils.build_token_value(
                    context=self.workflow.context,
                    js_context=context,
                    full_js=self.full_js,
                    expression_lib=self.expression_lib,
                    secondary_files=self.secondary_files,
                    connector=connector,
                    locations=locations,
                    token_value=paths,
                    load_contents=self.load_contents,
                    load_listing=self.load_listing)
                return token_list if len(token_list) > 1 else token_list[0] if len(token_list) == 1 else None
            # Otherwise, fill context['self'] with glob data and proceed
            else:
                context['self'] = await utils.build_token_value(
                    context=self.workflow.context,
                    js_context=context,
                    full_js=self.full_js,
                    expression_lib=self.expression_lib,
                    secondary_files=self.secondary_files,
                    connector=connector,
                    locations=locations,
                    token_value=paths,
                    load_contents=self.load_contents,
                    load_listing=self.load_listing)
        if self.output_eval is not None:
            # Fill context with exit code
            context['runtime']['exitCode'] = command_output.exit_code
            # Evaluate output
            token = utils.eval_expression(
                expression=self.output_eval,
                context=context,
                full_js=self.full_js,
                expression_lib=self.expression_lib)
            # Build token
            return await utils.build_token_value(
                context=self.workflow.context,
                js_context=context,
                full_js=self.full_js,
                expression_lib=self.expression_lib,
                secondary_files=self.secondary_files,
                connector=connector,
                locations=locations,
                token_value=token,
                load_contents=self.load_contents,
                load_listing=self.load_listing)
        # As the default value (no return path is met in previous code), simply process the command output
        return await utils.build_token_value(
            context=self.workflow.context,
            js_context=context,
            full_js=self.full_js,
            expression_lib=self.expression_lib,
            secondary_files=self.secondary_files,
            connector=connector,
            locations=locations,
            token_value=token_value,
            load_contents=self.load_contents,
            load_listing=self.load_listing)

    async def process(self, job: Job, command_output: CWLCommandOutput) -> Optional[Token]:
        if command_output.status == Status.SKIPPED:
            return None
        else:
            # Retrieve token value
            context = utils.build_context(
                inputs=job.inputs,
                output_directory=job.output_directory,
                tmp_directory=job.tmp_directory,
                hardware=self.workflow.context.scheduler.get_hardware(job.name))
            return await self._build_token(
                job, context, await self._process_command_output(job, command_output, context))

    async def save(self, context: StreamFlowContext):
        return {**await super().save(context),
                **{'token_type': self.token_type,
                   'enum_symbols': self.enum_symbols,
                   'expression_lib': self.expression_lib,
                   'file_format': self.file_format,
                   'full_js': self.full_js,
                   'glob': self.glob,
                   'load_contents': self.load_contents,
                   'load_listing': self.load_listing.value if self.load_listing else None,
                   'optional': self.optional,
                   'output_eval': self.output_eval,
                   'secondary_files': await asyncio.gather(*(asyncio.create_task(s.save(context))
                                                             for s in self.secondary_files)),
                   'streamable': self.streamable}}


class CWLMapTokenProcessor(TokenProcessor):

    def __init__(self,
                 name: str,
                 workflow: Workflow,
                 processor: TokenProcessor):
        super().__init__(name, workflow)
        self.processor: TokenProcessor = processor

    @classmethod
    async def _load(cls,
                    context: StreamFlowContext,
                    row: MutableMapping[str, Any],
                    loading_context: DatabaseLoadingContext) -> CWLMapTokenProcessor:
        return cls(
            name=row['name'],
            workflow=await loading_context.load_workflow(context, row['workflow']),
            processor=await TokenProcessor.load(context, row['processor'], loading_context))

    async def process(self, inputs: MutableMapping[str, Token], token: Token) -> Token:
        # If value is token, propagate the process call
        if isinstance(token.value, Token):
            return token.update(await self.process(inputs, token.value))
        # Check if token value is a list
        if not isinstance(token, ListToken):
            raise WorkflowDefinitionException(
                "Invalid value {} for token {}: it should be an array".format(token.value, self.name))
        # Propagate evaluation to the inner processor
        return token.update(await asyncio.gather(*(asyncio.create_task(
            self.processor.process(inputs, v)) for v in token.value)))

    async def save(self, context: StreamFlowContext):
        return {**await super().save(context),
                **{'processor': {'type': get_class_fullname(type(self.processor)),
                                 'params': await self.processor.save(context)}}}


class CWLMapCommandOutputProcessor(CommandOutputProcessor):

    def __init__(self,
                 name: str,
                 workflow: Workflow,
                 processor: CommandOutputProcessor):
        super().__init__(name, workflow)
        self.processor: CommandOutputProcessor = processor

    async def process(self, job: Job, command_output: CommandOutput) -> Optional[Token]:
        if isinstance(command_output.value, MutableMapping) and self.name in command_output.value:
            value = command_output.value[self.name]
            command_output = (command_output.update([{self.name: v} for v in value])
                              if isinstance(value, MutableMapping) else command_output.update(value))
        if isinstance(command_output.value, MutableSequence):
            token = ListToken(
                value=await asyncio.gather(*(asyncio.create_task(
                    self.processor.process(job, command_output.update(value))
                ) for value in command_output.value)),
                tag=get_tag(job.inputs.values()))
        else:
            token = await self.processor.process(job, command_output)
        if not isinstance(token, ListToken):
            token = ListToken(
                value=[token],
                tag=token.tag)
        return token.update(token.value)

    async def save(self, context: StreamFlowContext):
        return {**await super().save(context),
                **{'processor': {'type': get_class_fullname(type(self.processor)),
                                 'params': await self.processor.save(context)}}}


class CWLObjectTokenProcessor(TokenProcessor):

    def __init__(self,
                 name: str,
                 workflow: Workflow,
                 processors: MutableMapping[str, TokenProcessor]):
        super().__init__(name, workflow)
        self.processors: MutableMapping[str, TokenProcessor] = processors

    @classmethod
    async def _load(cls,
                    context: StreamFlowContext,
                    row: MutableMapping[str, Any],
                    loading_context: DatabaseLoadingContext) -> CWLObjectTokenProcessor:
        return cls(
            name=row['name'],
            workflow=await loading_context.load_workflow(context, row['workflow']),
            processors={k: v for k, v in zip(
                row['processors'].keys(),
                await asyncio.gather(*(asyncio.create_task(TokenProcessor.load(context, v, loading_context))
                                       for v in row['processors'].values())))})

    async def process(self, inputs: MutableMapping[str, Token], token: Token) -> Token:
        # If value is token, propagate the process call
        if isinstance(token.value, Token):
            return token.update(await self.process(inputs, token.value))
        # Check if token value is a dictionary
        if not isinstance(token.value, MutableMapping):
            raise WorkflowDefinitionException(
                "Invalid value {} for token {}: it should be a record".format(token.value, self.name))
        # Propagate evaluation to the inner processors
        return token.update(dict(zip(token.value, [t.value for t in await asyncio.gather(*(asyncio.create_task(
            self.processors[k].process(inputs, token.update(v))) for k, v in token.value.items()))])))

    async def save(self, context: StreamFlowContext):
        return {**await super().save(context),
                **{'processors': await _save_processors_dict(context, self.processors)}}


class CWLObjectCommandOutputProcessor(CommandOutputProcessor):

    def __init__(self,
                 name: str,
                 workflow: Workflow,
                 processors: MutableMapping[str, CommandOutputProcessor],
                 expression_lib: Optional[MutableSequence[str]] = None,
                 full_js: bool = False,
                 output_eval: Optional[str] = None):
        super().__init__(name, workflow)
        self.processors: MutableMapping[str, CommandOutputProcessor] = processors
        self.expression_lib: Optional[MutableSequence[str]] = expression_lib
        self.full_js: bool = full_js
        self.output_eval: Optional[str] = output_eval

    async def process(self, job: Job, command_output: CommandOutput) -> Optional[Token]:
        if self.output_eval:
            # Build context and fill it with exit code
            context = utils.build_context(
                inputs=job.inputs,
                output_directory=job.output_directory,
                tmp_directory=job.tmp_directory,
                hardware=self.workflow.context.scheduler.get_hardware(job.name))
            context['runtime']['exitCode'] = cast(CWLCommandOutput, command_output).exit_code
            # Evaluate output
            command_output = command_output.update(utils.eval_expression(
                expression=self.output_eval,
                context=context,
                full_js=self.full_js,
                expression_lib=self.expression_lib))
        if isinstance(command_output.value, MutableMapping):
            if self.name in command_output.value:
                return await self.process(job, command_output.update(command_output.value[self.name]))
            else:
                token_tasks = {k: asyncio.create_task(p.process(job, command_output.update(command_output.value[k])))
                               for k, p in self.processors.items() if k in command_output.value}
                return ObjectToken(
                    value=dict(
                        zip(token_tasks.keys(), await asyncio.gather(*token_tasks.values()))),
                    tag=get_tag(job.inputs.values()))
        else:
            token_tasks = {k: asyncio.create_task(p.process(job, command_output)) for k, p in self.processors.items()}
            return ObjectToken(
                value=dict(
                    zip(token_tasks.keys(), await asyncio.gather(*token_tasks.values()))),
                tag=get_tag(job.inputs.values()))

    async def save(self, context: StreamFlowContext):
        return {**await super().save(context),
                **{'processors': await _save_processors_dict(context, self.processors),
                   'expression_lib': self.expression_lib,
                   'full_js': self.full_js,
                   'output_eval': self.output_eval}}


class CWLUnionTokenProcessor(TokenProcessor):

    def __init__(self,
                 name: str,
                 workflow: Workflow,
                 processors: MutableSequence[TokenProcessor]):
        super().__init__(name, workflow)
        self.processors: MutableSequence[TokenProcessor] = processors
        self.check_processor: MutableMapping[Type[TokenProcessor], Callable] = {
            CWLTokenProcessor: self._check_default_processor,
            CWLObjectTokenProcessor: self._check_object_processor,
            CWLMapTokenProcessor: self._check_map_processor,
            CWLUnionTokenProcessor: self._check_union_processor}

    # noinspection PyMethodMayBeStatic
    def _check_default_processor(self, processor: CWLCommandOutputProcessor, token_value: Any):
        try:
            utils.check_token_type(
                name=processor.name,
                token_value=token_value,
                token_type=processor.token_type,
                enum_symbols=processor.enum_symbols,
                optional=processor.optional)
            return True
        except WorkflowExecutionException:
            return False

    def _check_map_processor(self, processor: CWLMapTokenProcessor, token_value: Any):
        return isinstance(token_value, MutableSequence) and self.check_processor[type(processor.processor)](
            processor.processor, token_value[0])

    def _check_object_processor(self, processor: CWLObjectTokenProcessor, token_value: Any):
        return (isinstance(token_value, MutableMapping) and
                all(k in processor.processors and
                    self.check_processor[type(processor.processors[k])](processor.processors[k], v)
                    for k, v in token_value.items()))

    def _check_union_processor(self, processor: CWLUnionTokenProcessor, token_value: Any):
        return any(self.check_processor[type(p)](p, token_value) for p in processor.processors)

    @classmethod
    async def _load(cls,
                    context: StreamFlowContext,
                    row: MutableMapping[str, Any],
                    loading_context: DatabaseLoadingContext) -> CWLUnionTokenProcessor:
        return cls(
            name=row['name'],
            workflow=await loading_context.load_workflow(context, row['workflow']),
            processors=cast(MutableSequence[TokenProcessor], await asyncio.gather(*(asyncio.create_task(
                TokenProcessor.load(context, p, loading_context)) for p in row['processors']))))

    def get_processor(self, token_value: Any) -> TokenProcessor:
        for processor in self.processors:
            if self.check_processor[type(processor)](processor, token_value):
                return processor
        raise WorkflowDefinitionException("No suitable command output processors for value " + str(token_value))

    async def process(self, inputs: MutableMapping[str, Token], token: Token) -> Token:
        # If value is token, propagate the process call
        if isinstance(token.value, Token):
            return token.update(await self.process(inputs, token.value))
        # Select the correct processor for the evaluation
        processor = self.get_processor(token.value)
        # Propagate evaluation to the selected processor
        return await processor.process(inputs, token)

    async def save(self, context: StreamFlowContext):
        return {**await super().save(context),
                **{'processors': await asyncio.gather(*(asyncio.create_task(v.save(context))
                                                        for v in self.processors))}}


class CWLUnionCommandOutputProcessor(CommandOutputProcessor):

    def __init__(self,
                 name: str,
                 workflow: Workflow,
                 processors: MutableSequence[CommandOutputProcessor]):
        super().__init__(name, workflow)
        self.processors: MutableSequence[CommandOutputProcessor] = processors
        self.check_processor: MutableMapping[Type[CommandOutputProcessor], Callable] = {
            CWLCommandOutputProcessor: self._check_default_processor,
            CWLObjectCommandOutputProcessor: self._check_object_processor,
            CWLMapCommandOutputProcessor: self._check_map_processor,
            CWLUnionCommandOutputProcessor: self._check_union_processor}

    def _check_default_processor(self, processor: CWLCommandOutputProcessor, token_value: Any):
        try:
            utils.check_token_type(
                name=processor.name,
                token_value=token_value,
                token_type=processor.token_type,
                enum_symbols=processor.enum_symbols,
                optional=processor.optional)
            return True
        except WorkflowExecutionException:
            return False

    # noinspection PyMethodMayBeStatic
    def _check_map_processor(self, processor: CWLMapCommandOutputProcessor, token_value: Any):
        return isinstance(token_value, MutableSequence) and self.check_processor[type(processor.processor)](
            processor.processor, token_value[0])

    def _check_object_processor(self, processor: CWLObjectCommandOutputProcessor, token_value: Any):
        return (isinstance(token_value, MutableMapping) and
                all(k in processor.processors and
                    self.check_processor[type(processor.processors[k])](processor.processors[k], v)
                    for k, v in token_value.items()))

    def _check_union_processor(self, processor: CWLUnionCommandOutputProcessor, token_value: Any):
        return any(self.check_processor[type(p)](p, token_value) for p in processor.processors)

    def get_processor(self, token_value: Any) -> CommandOutputProcessor:
        for processor in self.processors:
            if self.check_processor[type(processor)](processor, token_value):
                return processor
        raise WorkflowDefinitionException("No suitable token processors for value " + str(token_value))

    async def process(self, job: Job, command_output: CommandOutput) -> Optional[Token]:
        token_value = command_output.value
        # If `token_value` is a dictionary, directly extract the token value from it
        if isinstance(token_value, MutableMapping) and self.name in token_value:
            token_value = token_value[self.name]
        return await self.get_processor(token_value).process(job, command_output)

    async def save(self, context: StreamFlowContext):
        return {**await super().save(context),
                **{'processors': await asyncio.gather(*(asyncio.create_task(v.save(context))
                                                        for v in self.processors))}}
