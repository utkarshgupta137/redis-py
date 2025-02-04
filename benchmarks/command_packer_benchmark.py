import sys

from base import Benchmark

from redis.commands.packer import SPEEDUPS, CommandPacker
from redis.encoder import Encoder


class CommandPackerBenchmark(Benchmark):
    ARGUMENTS = (
        {"name": "value_type", "values": ["str", "bytes", "int", "float", "memory"]},
        {"name": "speedups", "values": [False, True]},
        {"name": "value_size", "values": [10, 100, 1000, 10000, 100000]},
    )

    def setup(self, speedups, value_type, value_size):
        encoder = Encoder(
            encoding="utf-8", encoding_errors="strict", decode_responses=True
        )
        self.packer = CommandPacker(encoder=encoder, speedups=speedups)

        if value_type == "str":
            self.value = "value" * value_size
        elif value_type == "bytes":
            self.value = b"value" * value_size
        elif value_type == "int":
            self.value = min(int("1" * value_size), sys.maxsize)
        elif value_type == "float":
            self.value = min(float("1" * value_size) + 0.1 + 0.2, sys.float_info.max)
        elif value_type == "memory":
            self.value = memoryview(b"value" * value_size)

    def run(self, speedups, value_type, value_size):
        self.packer.pack_command("SET", "benchmark", self.value)


if __name__ == "__main__":
    if SPEEDUPS:
        CommandPackerBenchmark().run_benchmark()
    else:
        print("Speedups not installed. Aborting.")
