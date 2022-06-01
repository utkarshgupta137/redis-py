import random
from binascii import crc_hqx
from typing import TYPE_CHECKING, Any, Dict, Tuple, Union

from redis.encoder import Encoder
from redis.exceptions import (
    MovableKeysError,
    RedisClusterException,
    RedisError,
    ResponseError,
)
from redis.typing import EncodableT

if TYPE_CHECKING:
    from redis.asyncio.cluster import ClusterNode

try:
    from redis.speedups import CommandSlotter as _CommandSlotter
    from redis.speedups import KeySlotter as _KeySlotter

    SPEEDUPS = True
except Exception:
    SPEEDUPS = False


__all__ = ["CommandSlotter", "KeySlotter", "REDIS_CLUSTER_HASH_SLOTS", "SPEEDUPS"]


# Redis Cluster's key space is divided into 16384 slots.
# see: https://redis.io/docs/reference/cluster-spec/#key-distribution-model
REDIS_CLUSTER_HASH_SLOTS = 16384


class KeySlotter:
    __slots__ = "_key_slotter", "encoder", "encoding", "errors", "speedups"

    def __init__(self, encoder: Encoder, speedups: bool = True) -> None:
        self.encoder = encoder
        self.encoding = encoder.encoding
        self.errors = encoder.encoding_errors
        self.speedups = SPEEDUPS and speedups
        if self.speedups:
            self._key_slotter = _KeySlotter(self.encoding, self.errors)

    def key_slot(self, key: EncodableT) -> int:
        if self.speedups:
            try:
                return self._key_slotter.key_slot(key)
            except Exception:
                ...

        k = self.encoder.encode(key)
        start = k.find(b"{")
        if start > -1:
            end = k.find(b"}", start + 1)
            if end > -1 and end != start + 1:
                k = k[start + 1 : end]
        return crc_hqx(k, 0) % REDIS_CLUSTER_HASH_SLOTS


class CommandSlotter(KeySlotter):
    __slots__ = KeySlotter.__slots__ + ("_command_slotter", "commands", "node")

    def __init__(self, encoder: Encoder, speedups: bool = True) -> None:
        super().__init__(encoder, speedups)
        if self.speedups:
            self._command_slotter = _CommandSlotter(self.encoding, self.errors)

        self.commands: Dict[str, Union[int, Dict[str, Any]]] = {}

    async def initialize(self, node: "ClusterNode") -> None:
        self.node = node

        commands = await self.node.execute_command("COMMAND")
        for cmd, command in commands.items():
            if "movablekeys" in command["flags"]:
                commands[cmd] = -1
            elif command["first_key_pos"] == 0 and command["last_key_pos"] == 0:
                commands[cmd] = 0
            elif command["first_key_pos"] == 1 and command["last_key_pos"] == 1:
                commands[cmd] = 1
        self.commands = {cmd.upper(): command for cmd, command in commands.items()}

        if self.speedups:
            self._command_slotter.initialize(self.commands)

    def command_slot(self, command: str, *args: EncodableT) -> int:
        if self.speedups:
            try:
                return self._command_slotter.command_slot(command, args)
            except Exception:
                ...

        # get the keys in the command
        keys = self.get_keys(command, *args) if args else ()

        # single key command
        if len(keys) == 1:
            return self.key_slot(keys[0])

        # make sure that all keys map to the same slot
        slots = {self.key_slot(key) for key in keys}
        if len(slots) != 1:
            if slots:
                raise RedisClusterException(
                    f"{command, *args} - all keys must map to the same key slot"
                )

            # no keys in command
            if command in ("EVAL", "EVALSHA", "FCALL", "FCALL_RO"):
                # command can be run on any shard
                return random.randrange(0, REDIS_CLUSTER_HASH_SLOTS)

            raise RedisClusterException(
                f"Missing key. No way to dispatch {command, *args} to Redis Cluster. "
                "You can execute the command by specifying target nodes."
            )

        return slots.pop()

    def get_keys(self, command: str, *args: EncodableT) -> Tuple[EncodableT, ...]:
        if command in ("MOVABLE_KEYS", "EVAL", "EVALSHA", "FCALL", "FCALL_RO"):
            # COMMAND GETKEYS is buggy with EVAL/EVALSHA for redis < 7.0
            # see: https://github.com/redis/redis/pull/9733
            if command in ("EVAL", "EVALSHA"):
                # syntax: EVAL "script body" num_keys ...
                if len(args) < 2:
                    raise RedisClusterException(
                        f"Invalid args in command: {command, *args}"
                    )
                return args[2 : 2 + args[1]]

            if command == "MOVABLE_KEYS":
                return args

            if len(args) < 2:
                # command has no keys in it
                return ()

        try:
            cmd = self.commands[command]
        except KeyError:
            # try after upper casing & splitting the command
            # eg: 'MEMORY' for 'memory usage'
            args = tuple(command.upper().split()) + args
            command = args[0]
            args = args[1:]
            if command not in self.commands:
                raise RedisError(
                    f"{command.upper()} command doesn't exist in Redis commands"
                )

            cmd = self.commands[command]

        if cmd == 1:
            return (args[0],)
        if cmd == 0:
            return ()
        if cmd == -1:
            raise MovableKeysError()

        args = (command,) + args
        last_key_pos = cmd["last_key_pos"]
        if last_key_pos < 0:
            last_key_pos = len(args) + last_key_pos
        return args[cmd["first_key_pos"] : last_key_pos + 1 : cmd["step_count"]]

    async def get_moveable_keys(self, *args: EncodableT) -> int:
        # TODO: Implement COMMAND INFO
        # see: https://github.com/redis/redis/pull/8324

        try:
            args = tuple(args[0].upper().split()) + args[1:]
            keys = await self.node.execute_command("COMMAND GETKEYS", *args)
        except ResponseError as e:
            message = e.__str__()
            if (
                "Invalid arguments" in message
                or "The command has no key arguments" in message
            ):
                keys = ()
            else:
                raise e

        return self.command_slot("MOVABLE_KEYS", *keys)
