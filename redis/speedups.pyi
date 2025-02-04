from typing import Any, Dict, Iterable, List, Tuple, Union

from redis.typing import EncodableT, EncodedT

class KeySlotter:
    def __init__(self, encoding: str, errors: str) -> None: ...
    def key_slot(self, key: EncodableT) -> int: ...

class CommandPacker:
    def __init__(self, encoding: str, errors: str) -> None: ...
    def pack_command(self, args: Tuple[EncodableT, ...]) -> List[EncodedT]: ...
    def pack_commands(
        self, commands: Iterable[Tuple[EncodableT, ...]]
    ) -> List[EncodedT]: ...

class CommandSlotter:
    def __init__(self, encoding: str, errors: str) -> None: ...
    def initialize(self, commands: Dict[str, Union[int, Dict[str, Any]]]) -> None: ...
    def command_slot(self, command: str, args: Tuple[EncodableT, ...]) -> int: ...
