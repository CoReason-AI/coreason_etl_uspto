# Copyright (c) 2026 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_uspto

"""
AGENT INSTRUCTION: This module provides the Fake Root Streaming Generator logic.
It streams ZIP files from HTTP, decompresses chunks on the fly, and injects a fake <root> tag
so lxml sees a valid document. It then yields individual patent dicts.
"""

import datetime
from collections.abc import Iterator
from typing import Any

import requests
import xmltodict
from lxml import etree

from coreason_etl_uspto.config import FederatedEnvironmentPolicy
from coreason_etl_uspto.utils.logger import logger


def stream_uspto_zip(
    url: str, session: requests.Session, policy: FederatedEnvironmentPolicy | None = None
) -> Iterator[bytes]:
    """
    AGENT INSTRUCTION: This generator streams the ZIP file from the given URL and decompresses it on the fly.
    Now supports both 'https://' and 'file://' protocols for autonomous fallbacks.
    """
    logger.info(f"Streaming ZIP file from URL: {url}")

    if policy is None:
        policy = FederatedEnvironmentPolicy()

    # Route local files bypassing HTTP
    if url.startswith("file://"):
        file_path = url.replace("file://", "")
        def local_chunk_generator():
            with open(file_path, "rb") as f:
                while True:
                    chunk = f.read(policy.uspto_stream_chunk_size)
                    if not chunk:
                        break
                    yield chunk
        stream = local_chunk_generator()
    else:
        response = session.get(url, stream=True)
        response.raise_for_status()
        stream = response.iter_content(chunk_size=policy.uspto_stream_chunk_size)

    import struct
    import zlib

    # We need to read the 30-byte Local File Header to skip to the compressed data
    header_format = "<4s5H3I2H"
    header_size = struct.calcsize(header_format)

    buffer = b""
    while len(buffer) < header_size:
        try:
            chunk = next(stream)
            if not chunk:
                continue
            buffer += chunk
        except StopIteration:
            logger.warning(f"Unexpected end of stream while reading ZIP header from {url}")
            return

    header_bytes = buffer[:header_size]
    buffer = buffer[header_size:]

    unpacked = struct.unpack(header_format, header_bytes)
    signature = unpacked[0]
    compression = unpacked[3]
    name_len = unpacked[9]
    extra_len = unpacked[10]

    if signature != b"PK\x03\x04":
        logger.warning(f"Invalid ZIP local file header signature in {url}")
        return

    if compression != 8:
        # 8 is DEFLATED, which is standard. If not, we can't blindly zlib decompress.
        logger.warning(f"Unsupported compression method {compression} in {url}")
        return

    # Read filename and extra field
    skip_len = name_len + extra_len
    while len(buffer) < skip_len:
        try:
            chunk = next(stream)
            if not chunk:
                continue
            buffer += chunk
        except StopIteration:
            logger.warning(f"Unexpected end of stream while skipping filename/extra in {url}")
            return

    filename = buffer[:name_len].decode("utf-8", errors="ignore")
    if not filename.endswith(".xml"):
        logger.warning(f"No XML files found in {url} (found {filename})")
        return

    buffer = buffer[skip_len:]

    # We are now at the start of the compressed data.
    # Create a streaming decompressor. We use -15 for max_wbits to indicate raw DEFLATE stream without zlib header
    decompressor = zlib.decompressobj(-15)

    # Decompress the remaining buffer
    if buffer:
        uncompressed = decompressor.decompress(buffer)
        if uncompressed:
            yield uncompressed

    # Decompress the rest of the stream
    for chunk in stream:
        if not chunk:
            continue

        uncompressed = decompressor.decompress(chunk)
        if uncompressed:
            yield uncompressed

        if decompressor.unused_data:
            # The decompressor hit the end of the DEFLATE stream.
            # The rest of the zip file (Central Directory, etc.) is in unused_data.
            # We assume there's only one XML file we care about per ZIP.
            break

    # Flush the decompressor
    try:
        remaining = decompressor.flush()
        if remaining:  # pragma: no cover
            yield remaining
    except zlib.error:  # pragma: no cover
        logger.debug(f"zlib flush error for {url}, likely harmless given stream boundaries.")


class FakeRootStream:
    """
    AGENT INSTRUCTION: This class wraps an iterator of bytes, injecting <root> at the start
    and </root> at the end to create a valid single XML document.
    """

    def __init__(self, stream: Iterator[bytes]):
        self.stream = stream
        self.started = False
        self.finished = False
        self.buffer = b""

    def read(self, size: int = -1) -> bytes:
        if self.finished and not self.buffer:
            return b""

        if not self.started:
            self.started = True
            self.buffer = b"<root>\n"

        while (size < 0 or len(self.buffer) < size) and not self.finished:
            try:
                chunk = next(self.stream)
                self.buffer += chunk
            except StopIteration:
                self.finished = True
                self.buffer += b"\n</root>"

        if size < 0:
            result = self.buffer
            self.buffer = b""
            return result

        result = self.buffer[:size]
        self.buffer = self.buffer[size:]
        return result


def parse_uspto_stream(stream: Iterator[bytes], tag: str, source_url: str) -> Iterator[dict[str, Any]]:
    """
    AGENT INSTRUCTION: This function parses the stream using lxml.etree.iterparse,
    yielding dict representations of the patents and managing memory efficiently.
    """
    fake_stream = FakeRootStream(stream)

    context = etree.iterparse(fake_stream, events=("end",), tag=tag, recover=True, huge_tree=True)

    for _event, elem in context:
        try:
            # Convert single XML tree to dict
            xml_str = etree.tostring(elem, encoding="unicode")
            doc = xmltodict.parse(xml_str)

            # Enrich with Metadata
            doc["ingestion_meta"] = {
                "source_file": source_url,
                "parsed_at": datetime.datetime.now(datetime.UTC).isoformat(),
            }

            # Inject the root element name as a key to help the DTD router
            doc["_root_element"] = tag

            yield doc
        except Exception as e:
            logger.error(f"Failed to parse element in {source_url}: {e}")
            # Yield a failure record
            yield {
                "_error": True,
                "error_message": str(e),
                "source_url": source_url,
            }
        finally:
            # CRITICAL: Clear memory
            elem.clear()
            while elem.getprevious() is not None:
                del elem.getparent()[0]
