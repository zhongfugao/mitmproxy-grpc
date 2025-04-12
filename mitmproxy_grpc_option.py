from typing import Optional
import protobuf_modification

from mitmproxy import ctx

class GrpcOption:

    def __init__(self, protobuf_modifier: protobuf_modification.ProtobufModifier) -> None:
        self.protobuf_modifier = protobuf_modifier

    def load(self, loader):
        loader.add_option(
            name = "descriptor_file",
            typespec = Optional[str],
            default = None,
            help = "Set the descriptor file used for serialiation and deserialization of protobuf content",
        )

    def configure(self, updates):
        if ("descriptor_file" in updates 
            and ctx.options.__contains__("descriptor_file")
            and ctx.options.descriptor_file is not None
        ):
            self.protobuf_modifier.set_descriptor(ctx.options.descriptor_file)