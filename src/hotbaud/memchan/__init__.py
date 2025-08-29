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
from ._impl import (
    MCToken as MCToken,
    alloc_memory_channel as alloc_memory_channel,
    open_memory_channel as open_memory_channel,
    Buffer as Buffer,
    MemorySendChannel as MemorySendChannel,
    MemoryReceiveChannel as MemoryReceiveChannel,
    attach_to_memory_receiver as attach_to_memory_receiver,
    attach_to_memory_sender as attach_to_memory_sender,
    MemoryChannel as MemoryChannel,
    attach_to_memory_channel as attach_to_memory_channel
)

from ._fans import (
    attach_fan_out_sender as attach_fan_out_sender,
    attach_fan_in_receiver as attach_fan_in_receiver
)
