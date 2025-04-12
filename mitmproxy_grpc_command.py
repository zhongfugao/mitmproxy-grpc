import protobuf_modification

from mitmproxy import command
from mitmproxy import ctx
from mitmproxy import types

from collections.abc import Sequence


class GrpcCommand:
    def __init__(
        self, protobuf_modifier: protobuf_modification.ProtobufModifier
    ) -> None:
        self.protobuf_modifier = protobuf_modifier

    @command.command("grpc.options")
    def edit_focus_options(self) -> Sequence[str]:
        focus_options = [
            "request-body",
            "response-body",
        ]

        return focus_options

    @command.command("grpc")
    @command.argument("flow_part", type=types.Choice("grpc.options"))
    def edit_focus(self, flow_part: str) -> None:
        request = ctx.master.view.focus.flow.request
        response = ctx.master.view.focus.flow.response
        path = request.path

        if flow_part == "request-body":
            content = request.get_content(strict=False) or b""
            http_message = request
        elif flow_part == "response-body":
            content = response.get_content(strict=False) or b""
            http_message = response
        else:
            ctx.log(f"Unknown option {flow_part}")
            return

        deserialized_content = self.protobuf_modifier.deserialize(
            http_message, path, content
        )
        modifiedContent = ctx.master.spawn_editor(deserialized_content)

        # Many editors make it hard to save a file without a terminating
        # newline on the last line. When editing message bodies, this can
        # cause problems.
        if ctx.master.options.console_strip_trailing_newlines:
            modifiedContent = modifiedContent.rstrip(b"\n")

        http_message.content = self.protobuf_modifier.serialize(
            http_message, path, modifiedContent
        )
