# The MIT License (MIT)
#
# Copyright © 2025 Guillermo Rodriguez & Tyler Goodlet
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the “Software”), to
# deal in the Software without restriction, including without limitation the
# rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
# sell copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
'''
Helpers for sharing of file descriptors through unix sockets

'''

from pathlib import Path
from typing import AsyncGenerator, Sequence
from contextlib import asynccontextmanager as acm

import anyio


'''
Trio implementation of a fd passing server

'''


async def fdshare_server_task(share_path: Path, fds: Sequence[int]) -> None:
    async def handle(client):
        async with client:
            await client.send_fds(b'this message is ignored', fds)

    listener = await anyio.create_unix_listener(share_path)
    await listener.serve(handle)


@acm
async def open_fd_share_socket(
    share_path: Path, fds: Sequence[int]
) -> AsyncGenerator[None, None]:
    async with anyio.create_task_group() as tg:
        tg.start_soon(fdshare_server_task, share_path, fds)
        yield
        tg.cancel_scope.cancel()


@acm
async def maybe_open_fd_share_socket(
    share_path: str | Path | None, fds: Sequence[int]
) -> AsyncGenerator[None, None]:
    if share_path is None:
        yield
        return

    async with open_fd_share_socket(Path(share_path), fds):
        yield
