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
import zipfile
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
    """
    logger.info(f"Streaming ZIP file from URL: {url}")
    response = session.get(url, stream=True)
    response.raise_for_status()

    # We need to process the zip file from the stream
    # Python's zipfile requires a file-like object with seek()
    # For large files, downloading to memory is not ideal, but requests' stream doesn't support seek.
    # We will simulate a stream by reading the whole file into memory if it's not too big
    # Or implement a custom streaming unzipper.
    # As the constraint says "must not load the full unzipped file (5GB+) into RAM",
    # downloading the zipped file (typically ~100MB) might be acceptable, but ideally we stream.
    # Let's download the zip to memory (BytesIO) as the zipped version is smaller,
    # or write to a temp file and read. Since we must not load the *unzipped* file into RAM,
    # loading the *zipped* file into a BytesIO might be okay if it fits, or we can stream to a temp file.
    # To keep it memory efficient without hitting disk, we can use an approach if possible,
    # but ZipFile needs seek. Let's use a temporary file or BytesIO for the zip archive.

    # To stream the zip securely without keeping the entire zip in memory,
    # we can use a SpooledTemporaryFile which falls back to disk if it exceeds a small buffer.
    # The max_size is set to 10MB; above that, it writes to a temporary file on disk.
    from tempfile import SpooledTemporaryFile

    if policy is None:
        policy = FederatedEnvironmentPolicy()

    with SpooledTemporaryFile(max_size=policy.uspto_max_memory_mb * 1024 * 1024) as temp_file:
        for chunk in response.iter_content(chunk_size=policy.uspto_stream_chunk_size):
            if chunk:
                temp_file.write(chunk)

        temp_file.seek(0)

        with zipfile.ZipFile(temp_file) as z:
            # Assuming one XML file per ZIP
            xml_filename = [name for name in z.namelist() if name.endswith(".xml")]
            if not xml_filename:
                logger.warning(f"No XML files found in {url}")
                return

            with z.open(xml_filename[0]) as xml_file:
                while True:
                    chunk = xml_file.read(policy.uspto_stream_chunk_size)
                    if not chunk:
                        break
                    yield chunk


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
            # XML declaration might be in the stream, we should ideally put <root> after it,
            # but lxml iterparse usually handles <root> even if xml declaration is inside,
            # or we strip xml declarations. USPTO XMLs often have multiple XML declarations
            # (one for each patent). lxml with recover=True might handle it.
            # But the requirement says: "Injects a fake <root> tag at the start and </root> at the end"
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

    # We use recover=True because USPTO files often have multiple <?xml ...?> declarations
    # concatenated together, which strictly is invalid XML even with a fake root.
    # iterparse in lxml does not accept `parser` keyword natively in all versions like regular parse does,
    # but it accepts recover=True directly as an argument to iterparse.

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
