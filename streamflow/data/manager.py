from __future__ import annotations

import asyncio
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING

from importlib_resources import files

from streamflow.core.data import DataLocation, DataManager, DataType
from streamflow.data import remotepath
from streamflow.deployment.connector.local import LocalConnector
from streamflow.deployment.utils import get_path_processor

if TYPE_CHECKING:
    from streamflow.core.context import StreamFlowContext
    from streamflow.core.deployment import Connector, ExecutionLocation
    from typing import MutableMapping, MutableSequence


async def _copy(
    src_connector: Connector | None,
    src_location: ExecutionLocation | None,
    src: str,
    dst_connector: Connector | None,
    dst_locations: MutableSequence[ExecutionLocation] | None,
    dst: str,
    writable: False,
) -> None:
    if isinstance(src_connector, LocalConnector):
        await dst_connector.copy_local_to_remote(
            src=src,
            dst=dst,
            locations=dst_locations,
            read_only=not writable,
        )
    elif isinstance(dst_connector, LocalConnector):
        await src_connector.copy_remote_to_local(
            src=src,
            dst=dst,
            locations=[src_location],
            read_only=not writable,
        )
    else:
        await dst_connector.copy_remote_to_remote(
            src=src,
            dst=dst,
            locations=dst_locations,
            source_location=src_location,
            source_connector=src_connector,
            read_only=not writable,
        )


class DefaultDataManager(DataManager):
    def __init__(self, context: StreamFlowContext):
        super().__init__(context)
        self.path_mapper = RemotePathMapper(context)

    async def close(self):
        pass

    def get_data_locations(
        self,
        path: str,
        deployment: str | None = None,
        location_name: str | None = None,
        data_type: DataType | None = None,
    ) -> MutableSequence[DataLocation]:
        data_locations = self.path_mapper.get(
            path, data_type, deployment, location_name
        )
        data_locations = [
            loc for loc in data_locations if loc.data_type != DataType.INVALID
        ]
        return data_locations

    @classmethod
    def get_schema(cls) -> str:
        return (
            files(__package__)
            .joinpath("schemas")
            .joinpath("data_manager.json")
            .read_text("utf-8")
        )

    def get_source_location(
        self, path: str, dst_deployment: str
    ) -> DataLocation | None:
        if data_locations := self.get_data_locations(path=path):
            dst_connector = self.context.deployment_manager.get_connector(
                dst_deployment
            )
            same_connector_locations = {
                loc
                for loc in data_locations
                if loc.deployment == dst_connector.deployment_name
            }
            if same_connector_locations:
                for loc in same_connector_locations:
                    if loc.data_type == DataType.PRIMARY:
                        return loc
                return list(same_connector_locations)[0]
            else:
                local_locations = {
                    loc
                    for loc in data_locations
                    if isinstance(
                        self.context.deployment_manager.get_connector(loc.deployment),
                        LocalConnector,
                    )
                }
                if local_locations:
                    for loc in local_locations:
                        if loc.data_type == DataType.PRIMARY:
                            return loc
                    return list(local_locations)[0]
                else:
                    for loc in data_locations:
                        if loc.data_type == DataType.PRIMARY:
                            return loc
                    return list(data_locations)[0]
        else:
            return None

    def invalidate_location(self, location: ExecutionLocation, path: str) -> None:
        self.path_mapper.invalidate_location(location, path)

    def register_path(
        self,
        location: ExecutionLocation,
        path: str,
        relpath: str | None = None,
        data_type: DataType = DataType.PRIMARY,
    ) -> DataLocation:
        data_location = DataLocation(
            location=location,
            path=path,
            relpath=relpath or path,
            data_type=data_type,
            available=True,
        )
        self.path_mapper.put(path=path, data_location=data_location, recursive=True)
        self.context.checkpoint_manager.register(data_location)
        return data_location

    def register_relation(
        self, src_location: DataLocation, dst_location: DataLocation
    ) -> None:
        data_locations = self.path_mapper.get(src_location.path)
        for data_location in list(data_locations):
            self.path_mapper.put(data_location.path, dst_location)
            self.path_mapper.put(dst_location.path, data_location)

    async def transfer_data(
        self,
        src_location: ExecutionLocation,
        src_path: str,
        dst_locations: MutableSequence[ExecutionLocation],
        dst_path: str,
        writable: bool = False,
    ) -> None:
        src_connector = self.context.deployment_manager.get_connector(
            src_location.deployment
        )
        dst_connector = self.context.deployment_manager.get_connector(
            dst_locations[0].deployment
        )
        # Create destination folder
        await remotepath.mkdir(dst_connector, dst_locations, str(Path(dst_path).parent))
        # Follow symlink for source path
        await asyncio.gather(
            *(
                asyncio.create_task(src_data_loc.available.wait())
                for src_data_loc in self.get_data_locations(
                    src_path, src_location.deployment, src_location.name
                )
            )
        )
        src_path = await remotepath.follow_symlink(
            self.context, src_connector, src_location, src_path
        )
        primary_locations = self.path_mapper.get(src_path, DataType.PRIMARY)
        copy_tasks = []
        remote_locations = []
        data_locations = []
        for dst_location in dst_locations:
            # Check if a primary copy of the source path is already present on the destination location
            found_existing_loc = False
            for primary_loc in primary_locations:
                if primary_loc.name == dst_location.name:
                    # Wait for the source location to be available on the destination path
                    await primary_loc.available.wait()

                    # save data location of destination folder created
                    self.register_path(dst_location, str(Path(dst_path).parent))

                    # If yes, perform a symbolic link if possible
                    if not writable:
                        await remotepath.symlink(
                            dst_connector, dst_location, primary_loc.path, dst_path
                        )
                        self.path_mapper.create_and_map(
                            location_type=DataType.SYMBOLIC_LINK,
                            src_path=src_path,
                            dst_path=dst_path,
                            dst_location=dst_location,
                            available=True,
                        )
                    # Otherwise, perform a copy operation
                    else:
                        copy_tasks.append(
                            asyncio.create_task(
                                _copy(
                                    src_connector=dst_connector,
                                    src_location=dst_location,
                                    src=primary_loc.path,
                                    dst_connector=dst_connector,
                                    dst_locations=[dst_location],
                                    dst=dst_path,
                                    writable=True,
                                )
                            )
                        )
                        data_locations.append(
                            self.path_mapper.put(
                                path=dst_path,
                                data_location=DataLocation(
                                    location=dst_location,
                                    path=dst_path,
                                    relpath=list(self.path_mapper.get(src_path))[
                                        0
                                    ].relpath,
                                    data_type=DataType.PRIMARY,
                                    available=False,
                                ),
                            )
                        )
                    found_existing_loc = True
                    break
            # Otherwise, perform a remote copy and mark the destination as primary
            if not found_existing_loc:
                remote_locations.append(dst_location)

                # save data location of destination folder created
                self.register_path(dst_location, str(Path(dst_path).parent))

                if writable:
                    data_locations.append(
                        self.path_mapper.put(
                            path=dst_path,
                            data_location=DataLocation(
                                location=dst_location,
                                path=dst_path,
                                relpath=list(self.path_mapper.get(src_path))[0].relpath,
                                data_type=DataType.PRIMARY,
                                available=False,
                            ),
                        )
                    )
                else:
                    data_locations.append(
                        self.path_mapper.create_and_map(
                            location_type=DataType.PRIMARY,
                            src_path=src_path,
                            dst_path=dst_path,
                            dst_location=dst_location,
                        )
                    )
        # Perform all the copy operations
        if remote_locations:
            copy_tasks.append(
                asyncio.create_task(
                    _copy(
                        src_connector=src_connector,
                        src_location=src_location,
                        src=src_path,
                        dst_connector=dst_connector,
                        dst_locations=remote_locations,
                        dst=dst_path,
                        writable=writable,
                    )
                )
            )
        await asyncio.gather(*copy_tasks)
        # Mark all destination data locations as available
        for data_location in data_locations:
            data_location.available.set()


class RemotePathNode:
    __slots__ = ("children", "locations")

    def __init__(self):
        self.children: MutableMapping[str, RemotePathNode] = {}
        self.locations: MutableMapping[
            str, MutableMapping[str, MutableSequence[DataLocation]]
        ] = {}


class RemotePathMapper:
    def __init__(self, context: StreamFlowContext):
        self._filesystem: RemotePathNode = RemotePathNode()
        self.context: StreamFlowContext = context

    def _remove_node(self, location: DataLocation, node: RemotePathNode):
        if location.deployment in node.locations:
            del node.locations[location.deployment][location.name]
        for n in node.children.values():
            self._remove_node(location, n)

    def create_and_map(
        self,
        location_type: DataType,
        src_path: str,
        dst_path: str,
        dst_location: ExecutionLocation,
        available: bool = False,
    ) -> DataLocation:
        data_locations = self.get(src_path)
        # if not data_locations:
        #     # it is possible that src='' following a symbolic link
        #     # edit. tenere solo per debug. Ho aggiunto un controllo a monte dentro remotepath.py
        #     raise WorkflowTransferException(
        #         f"No data locations available {src_path if src_path else 'None'}"
        #     )
        dst_data_location = DataLocation(
            location=dst_location,
            path=dst_path,
            relpath=list(data_locations)[0].relpath,
            data_type=location_type,
            available=available,
        )
        for data_location in list(data_locations):
            self.put(data_location.path, dst_data_location)
            self.put(dst_path, data_location)
        self.put(dst_path, dst_data_location)
        return dst_data_location

    def get(
        self,
        path: str,
        location_type: DataType | None = None,
        deployment: str | None = None,
        name: str | None = None,
    ) -> MutableSequence[DataLocation]:
        path = PurePosixPath(Path(path).as_posix())
        node = self._filesystem
        for token in path.parts:
            if token in node.children:
                node = node.children[token]
            else:
                return []
        result = []
        for dep in [deployment] if deployment is not None else node.locations:
            for n in [name] if name is not None else node.locations.setdefault(dep, {}):
                locations = node.locations.setdefault(dep, {}).setdefault(n, [])
                result.extend(
                    [
                        loc
                        for loc in locations
                        if not (location_type and loc.data_type != location_type)
                    ]
                )
        return result

    def invalidate_location(self, location: ExecutionLocation, path: str) -> None:
        path = PurePosixPath(Path(path).as_posix())
        node = self._filesystem
        for token in path.parts:
            node = node.children[token]
        for node_child in node.children.values():
            for data_loc in node_child.locations.setdefault(
                location.deployment, {}
            ).get(location.name, []):
                if data_loc.data_type != DataType.INVALID:
                    self.invalidate_location(data_loc, data_loc.path)
                    data_loc.data_type = DataType.INVALID

    def put(
        self, path: str, data_location: DataLocation, recursive: bool = False
    ) -> DataLocation:
        path = PurePosixPath(Path(path).as_posix())
        path_processor = get_path_processor(
            self.context.deployment_manager.get_connector(data_location.deployment)
        )
        node = self._filesystem
        nodes = {}
        # Create or navigate hierarchy
        for i, token in enumerate(path.parts):
            node = node.children.setdefault(token, RemotePathNode())
            if recursive:
                nodes[path_processor.join(*path.parts[: i + 1])] = node
        if not recursive:
            nodes[str(path)] = node
        # Process hierarchy bottom-up to add parent locations
        relpath = data_location.relpath
        for node_path in reversed(nodes):
            node = nodes[node_path]
            if node_path == str(path):
                location = data_location
            else:
                location = DataLocation(
                    location=data_location.location,
                    path=node_path,
                    relpath=(
                        relpath
                        if relpath and node_path.endswith(relpath)
                        else path_processor.basename(node_path)
                    ),
                    data_type=DataType.PRIMARY,
                    available=True,
                )
            node_location = node.locations.setdefault(
                location.deployment, {}
            ).setdefault(location.name, [])
            paths = [
                loc.path for loc in node_location if loc.data_type != DataType.INVALID
            ]
            if location.path in paths:
                break
            else:
                node.locations[location.deployment][location.name].append(location)
                relpath = path_processor.dirname(relpath)
        # Return location
        return data_location

    def remove_location(self, location: DataLocation):
        data_locations = self._filesystem.locations.setdefault(
            location.deployment, {}
        ).get(location.name)
        for data_location in data_locations:
            self._remove_node(data_location, self._filesystem)
