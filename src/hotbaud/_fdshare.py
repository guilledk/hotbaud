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
Reimplementation of multiprocessing.reduction.sendfds & recvfds, using acms and trio.

cpython impl:
https://github.com/python/cpython/blob/275056a7fdcbe36aaac494b4183ae59943a338eb/Lib/multiprocessing/reduction.py#L138

'''
import array
from typing import AsyncGenerator, Sequence
from contextlib import asynccontextmanager as acm

import trio
from trio import socket


class FDSharingError(Exception):
    ...


async def send_fds(
    fds: Sequence[int],
    conn: socket.SocketType
) -> None:
    '''
    Async trio reimplementation of `multiprocessing.reduction.sendfds`

    https://github.com/python/cpython/blob/275056a7fdcbe36aaac494b4183ae59943a338eb/Lib/multiprocessing/reduction.py#L142

    '''
    # await ready msg from client
    if await conn.recv(1) != b'R':
        raise FDSharingError('did not receive ready msg')

    # setup int array for fds
    afds = array.array('i', fds)

    # first byte of msg will be len of fds to send % 256, acting as a fd amount
    # verification on `recv_fds` we refer to it as `check_byte`
    msg = bytes([len(afds) % 256])

    # send msg with custom SCM_RIGHTS type
    await conn.sendmsg(
        [msg],
        [(socket.SOL_SOCKET, socket.SCM_RIGHTS, afds)]
    )

    # finally wait receiver ack
    if await conn.recv(1) != b'A':
        raise FDSharingError('did not receive acknowledgement of fd')


async def recv_fds(sock_path: str, amount: int) -> tuple:
    '''
    Async trio reimplementation of `multiprocessing.reduction.recvfds`

    https://github.com/python/cpython/blob/275056a7fdcbe36aaac494b4183ae59943a338eb/Lib/multiprocessing/reduction.py#L150

    It's equivalent to std just using `trio.open_unix_socket` for connecting and
    changes on error handling.

    '''
    stream = await trio.open_unix_socket(sock_path)
    sock = stream.socket

    # send ready msg
    await sock.send(b'R')

    # prepare int array for fds
    a = array.array('i')
    bytes_size = a.itemsize * amount

    # receive 1 byte + space necesary for SCM_RIGHTS msg for {amount} fds
    msg, ancdata, _flags, _addr = await sock.recvmsg(
        1, socket.CMSG_SPACE(bytes_size)
    )

    # maybe failed to receive msg?
    if not msg and not ancdata:
        raise FDSharingError(f'Expected to receive {amount} fds from {sock_path}, but got EOF')

    # send ack, std comment mentions this ack pattern was to get around an
    # old macosx bug, but they are not sure if its necesary any more, in
    # any case its not a bad pattern to keep
    await sock.send(b'A')  # Ack

    # expect to receive only one `ancdata` item
    if len(ancdata) != 1:
        raise FDSharingError(
            f'Expected to receive exactly one \"ancdata\" but got {len(ancdata)}: {ancdata}'
        )

    # unpack SCM_RIGHTS msg
    cmsg_level, cmsg_type, cmsg_data = ancdata[0]

    # check proper msg type
    if cmsg_level != socket.SOL_SOCKET:
        raise FDSharingError(
            f'Expected CMSG level to be SOL_SOCKET({socket.SOL_SOCKET}) but got {cmsg_level}'
        )

    if cmsg_type != socket.SCM_RIGHTS:
        raise FDSharingError(
            f'Expected CMSG type to be SCM_RIGHTS({socket.SCM_RIGHTS}) but got {cmsg_type}'
        )

    # check proper data alignment
    length = len(cmsg_data)
    if length % a.itemsize != 0:
        raise FDSharingError(
            f'CMSG data alignment error: len of {length} is not divisible by int size {a.itemsize}'
        )

    # attempt to cast as int array
    a.frombytes(cmsg_data)

    # validate length check byte
    valid_check_byte = amount % 256  # check byte acording to `recv_fds` caller
    recvd_check_byte = msg[0]  # actual received check byte
    payload_check_byte = len(a) % 256  # check byte acording to received fd int array

    if recvd_check_byte != payload_check_byte:
        raise FDSharingError(
            'Validation failed: received check byte '
            f'({recvd_check_byte}) does not match fd int array len % 256 ({payload_check_byte})'
        )

    if valid_check_byte != recvd_check_byte:
        raise FDSharingError(
            'Validation failed: received check byte '
            f'({recvd_check_byte}) does not match expected fd amount % 256 ({valid_check_byte})'
        )

    return tuple(a)


'''
Trio implementation of a fd passing server

'''


async def _fdshare_server_task(
    sock: socket.SocketType,
    fds: Sequence[int]
) -> None:
    async with trio.open_nursery() as nursery:
        while True:
            conn, _ = await sock.accept()
            nursery.start_soon(send_fds, fds, conn)


@acm
async def open_fd_share_socket(
    sock: socket.SocketType,
    fds: Sequence[int]
) -> AsyncGenerator[None, None]:
    async with trio.open_nursery() as nursery:
        nursery.start_soon(_fdshare_server_task, sock, fds)
        yield
        nursery.cancel_scope.cancel()


@acm
async def maybe_open_fd_share_socket(
    sock: socket.SocketType | None,
    fds: Sequence[int]
) -> AsyncGenerator[None, None]:
    if sock is None:
        yield
        return

    async with open_fd_share_socket(
        sock, fds
    ):
        yield
